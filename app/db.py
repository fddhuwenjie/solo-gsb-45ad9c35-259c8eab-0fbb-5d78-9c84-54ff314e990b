"""SQLite 持久化与幂等存储。

``batches.batch_id`` 唯一约束保证同一批次重复提交不会产生第二条记录；
写入冲突时调用方重新读取已存结果回放，实现幂等。

处置复核与运输联单走 :func:`transaction`：在**同一 SQLite 事务**内重算
比对快照、校验并写入案件处置行 / 联单批次行，任一步失败整体回滚
（过期快照、缺失处置绝不落库）。联单落库后不可变更，相同联单号仅支持
同内容幂等回放。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional

_DB_PATH = os.environ.get("COLDCHAIN_DB", "/data/coldchain.db")
_write_lock = threading.RLock()


def get_db_path() -> str:
    return _DB_PATH


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """打开一个显式事务（写锁串行化），供复核路径原子读写。

    正常退出时提交；异常时回滚。调用方可直接用返回的连接执行查询。
    """
    with _write_lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(_DB_PATH)), exist_ok=True)
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS batches (
                batch_id      TEXT PRIMARY KEY,
                device_id     TEXT NOT NULL,
                profile_id    TEXT,
                request_hash  TEXT NOT NULL,
                request_json  TEXT NOT NULL,
                anomaly_count INTEGER NOT NULL,
                created_at    REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS anomalies (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id                TEXT NOT NULL
                                    REFERENCES batches(batch_id) ON DELETE CASCADE,
                seq                     INTEGER NOT NULL,
                start_ts                REAL NOT NULL,
                end_ts                  REAL NOT NULL,
                peak_temperature        REAL NOT NULL,
                peak_ts                 REAL NOT NULL,
                direction               TEXT NOT NULL,
                peak_segment_id         TEXT NOT NULL,
                attributed_segment_id   TEXT NOT NULL,
                attributed_segment_type TEXT NOT NULL,
                duration_seconds        REAL NOT NULL,
                coverage_json           TEXT NOT NULL,
                sample_counts_json      TEXT NOT NULL DEFAULT '{}',
                UNIQUE(batch_id, seq)
            );

            CREATE TABLE IF NOT EXISTS cases (
                case_id         TEXT PRIMARY KEY,
                batch_id        TEXT NOT NULL
                                REFERENCES batches(batch_id),
                analysis_digest TEXT NOT NULL,
                content_hash    TEXT NOT NULL,
                status          TEXT NOT NULL CHECK (status IN ('open', 'closed')),
                created_at      REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS case_decisions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id    TEXT NOT NULL
                           REFERENCES cases(case_id) ON DELETE CASCADE,
                seq        INTEGER NOT NULL,
                decision   TEXT NOT NULL
                           CHECK (decision IN ('resolved', 'accepted', 'dismissed')),
                handled_at REAL NOT NULL,
                note       TEXT,
                UNIQUE(case_id, seq)
            );

            CREATE TABLE IF NOT EXISTS shipments (
                shipment_id  TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                result       TEXT NOT NULL
                             CHECK (result IN ('released', 'held')),
                created_at   REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS shipment_batches (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                shipment_id     TEXT NOT NULL
                                REFERENCES shipments(shipment_id) ON DELETE CASCADE,
                seq             INTEGER NOT NULL,
                batch_id        TEXT NOT NULL
                                REFERENCES batches(batch_id),
                case_id         TEXT NOT NULL
                                REFERENCES cases(case_id),
                analysis_digest TEXT NOT NULL,
                handoff_at      REAL NOT NULL,
                anomaly_count   INTEGER NOT NULL,
                case_status     TEXT NOT NULL
                                CHECK (case_status IN ('open', 'closed')),
                UNIQUE(shipment_id, seq)
            );

            CREATE TABLE IF NOT EXISTS calibration_profiles (
                profile_id   TEXT PRIMARY KEY,
                device_id    TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at   REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS calibration_intervals (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id TEXT NOT NULL
                           REFERENCES calibration_profiles(profile_id) ON DELETE CASCADE,
                seq        INTEGER NOT NULL,
                start_ts   REAL NOT NULL,
                end_ts     REAL NOT NULL,
                scale      REAL NOT NULL,
                offset_value REAL NOT NULL,
                UNIQUE(profile_id, seq)
            );
            """
        )
        # 旧库迁移：按旧口径写入的异常行没有 sample_counts_json 列。
        # 仅补列；行内错误归因由读取时依据原始请求重算后覆盖修正。
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(anomalies)").fetchall()
        }
        if "sample_counts_json" not in existing:
            conn.execute(
                "ALTER TABLE anomalies ADD COLUMN sample_counts_json TEXT"
            )
        # 旧库迁移：校准档案特性上线前的批次没有 profile_id 列（旧批次恒为 NULL）。
        batch_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(batches)").fetchall()
        }
        if "profile_id" not in batch_cols:
            conn.execute("ALTER TABLE batches ADD COLUMN profile_id TEXT")


def _select_batch(
    conn: sqlite3.Connection, batch_id: str
) -> Optional[dict[str, Any]]:
    return conn.execute(
        "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()


def _select_anomalies(
    conn: sqlite3.Connection, batch_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM anomalies WHERE batch_id = ? ORDER BY seq", (batch_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_batch(batch_id: str) -> Optional[dict[str, Any]]:
    """读取已存批次（含异常列表）；不存在返回 None。"""
    conn = _connect()
    try:
        batch = _select_batch(conn, batch_id)
        if batch is None:
            return None
        rows = _select_anomalies(conn, batch_id)
    finally:
        conn.close()
    return {"batch": dict(batch), "anomalies": rows}


def _anomaly_row(batch_id: str, i: int, a: dict[str, Any]) -> tuple[Any, ...]:
    return (
        batch_id,
        i,
        a["start"],
        a["end"],
        a["peak_temperature"],
        a["peak_timestamp"],
        a["direction"],
        a["peak_segment_id"],
        a["attributed_segment_id"],
        a["attributed_segment_type"],
        a["duration_seconds"],
        json.dumps(a["segment_coverage_seconds"], ensure_ascii=False),
        json.dumps(a.get("segment_sample_counts", {}), ensure_ascii=False),
    )


_INSERT_ANOMALY_SQL = """
    INSERT INTO anomalies
        (batch_id, seq, start_ts, end_ts, peak_temperature, peak_ts,
         direction, peak_segment_id, attributed_segment_id,
         attributed_segment_type, duration_seconds, coverage_json,
         sample_counts_json)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _save_batch_conn(
    conn: sqlite3.Connection,
    batch_id: str,
    device_id: str,
    request_hash: str,
    request_json: str,
    anomalies: list[dict[str, Any]],
    profile_id: Optional[str] = None,
) -> None:
    """在给定事务/连接内原子写入批次与异常（batch_id 冲突抛 IntegrityError）。"""
    conn.execute(
        """INSERT INTO batches
           (batch_id, device_id, profile_id, request_hash, request_json,
            anomaly_count, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            batch_id,
            device_id,
            profile_id,
            request_hash,
            request_json,
            len(anomalies),
            time.time(),
        ),
    )
    conn.executemany(
        _INSERT_ANOMALY_SQL,
        [_anomaly_row(batch_id, i, a) for i, a in enumerate(anomalies)],
    )


def save_batch(
    batch_id: str,
    device_id: str,
    request_hash: str,
    request_json: str,
    anomalies: list[dict[str, Any]],
    profile_id: Optional[str] = None,
) -> bool:
    """原子写入批次与异常。batch_id 已存在（并发重复提交）返回 False。"""
    with _write_lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _save_batch_conn(
                    conn, batch_id, device_id, request_hash,
                    request_json, anomalies, profile_id=profile_id,
                )
            except sqlite3.IntegrityError:
                # batch_id 唯一约束冲突：幂等回放，不视为错误
                conn.rollback()
                return False
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
    return True


def _replace_anomalies_conn(
    conn: sqlite3.Connection, batch_id: str, anomalies: list[dict[str, Any]]
) -> None:
    """在给定事务/连接内用重算结果整体替换已存异常行（调用方保证批次存在）。"""
    conn.execute("DELETE FROM anomalies WHERE batch_id = ?", (batch_id,))
    conn.executemany(
        _INSERT_ANOMALY_SQL,
        [_anomaly_row(batch_id, i, a) for i, a in enumerate(anomalies)],
    )
    conn.execute(
        "UPDATE batches SET anomaly_count = ? WHERE batch_id = ?",
        (len(anomalies), batch_id),
    )


def replace_anomalies(batch_id: str, anomalies: list[dict[str, Any]]) -> bool:
    """用依据原始请求重算的结果整体替换已存异常行。

    批次不存在返回 False；否则在单事务内删除旧行（含按旧口径写入的错误
    归因行）并写入新行，同步 anomaly_count。
    """
    with _write_lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            exists = _select_batch(conn, batch_id)
            if exists is None:
                conn.rollback()
                return False
            _replace_anomalies_conn(conn, batch_id, anomalies)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
    return True


# ---------- 案件处置 ----------


def get_case_for_update(
    conn: sqlite3.Connection, case_id: str
) -> Optional[dict[str, Any]]:
    """事务内读取案件（含处置行）；调用方已持写事务，读到的就是当前快照。"""
    case = conn.execute(
        "SELECT * FROM cases WHERE case_id = ?", (case_id,)
    ).fetchone()
    if case is None:
        return None
    rows = conn.execute(
        "SELECT * FROM case_decisions WHERE case_id = ? ORDER BY seq", (case_id,)
    ).fetchall()
    return {"case": dict(case), "decisions": [dict(r) for r in rows]}


def save_case(
    conn: sqlite3.Connection,
    case_id: str,
    batch_id: str,
    analysis_digest: str,
    content_hash: str,
    status: str,
    decisions: list[tuple[int, str, float, Optional[str]]],
) -> None:
    """在给定事务内写入案件与逐条处置行。

    decisions 元素为 (seq, decision, handled_at_epoch, note)。case_id 唯一
    约束冲突会抛 IntegrityError，由调用方按幂等/换内容规则处理。
    """
    conn.execute(
        """INSERT INTO cases
           (case_id, batch_id, analysis_digest, content_hash,
            status, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            case_id,
            batch_id,
            analysis_digest,
            content_hash,
            status,
            time.time(),
        ),
    )
    conn.executemany(
        """INSERT INTO case_decisions
           (case_id, seq, decision, handled_at, note)
           VALUES (?, ?, ?, ?, ?)""",
        [(case_id, seq, decision, handled_at, note)
         for seq, decision, handled_at, note in decisions],
    )


# ---------- 运输联单 ----------


def get_shipment_for_update(
    conn: sqlite3.Connection, shipment_id: str
) -> Optional[dict[str, Any]]:
    """事务内读取联单（含按序批次行）；调用方已持写事务。"""
    ship = conn.execute(
        "SELECT * FROM shipments WHERE shipment_id = ?", (shipment_id,)
    ).fetchone()
    if ship is None:
        return None
    rows = conn.execute(
        "SELECT * FROM shipment_batches WHERE shipment_id = ? ORDER BY seq",
        (shipment_id,),
    ).fetchall()
    return {"shipment": dict(ship), "batches": [dict(r) for r in rows]}


def save_shipment(
    conn: sqlite3.Connection,
    shipment_id: str,
    content_hash: str,
    result: str,
    items: list[tuple[int, str, str, str, float, int, str]],
) -> None:
    """在给定事务内写入不可变联单及其按序批次行。

    items 元素为 (seq, batch_id, case_id, analysis_digest, handoff_at_epoch,
    anomaly_count, case_status)。shipment_id 唯一约束冲突会抛
    IntegrityError，由调用方按幂等/换内容规则处理。
    """
    conn.execute(
        """INSERT INTO shipments (shipment_id, content_hash, result, created_at)
           VALUES (?, ?, ?, ?)""",
        (shipment_id, content_hash, result, time.time()),
    )
    conn.executemany(
        """INSERT INTO shipment_batches
           (shipment_id, seq, batch_id, case_id, analysis_digest,
            handoff_at, anomaly_count, case_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (shipment_id, seq, batch_id, case_id, digest,
             handoff_at, anomaly_count, case_status)
            for seq, batch_id, case_id, digest, handoff_at,
            anomaly_count, case_status in items
        ],
    )


# ---------- 设备温度校准档案 ----------


def get_profile_for_update(
    conn: sqlite3.Connection, profile_id: str
) -> Optional[dict[str, Any]]:
    """事务内读取不可变校准档案（含按序生效区间）；不存在返回 None。"""
    profile = conn.execute(
        "SELECT * FROM calibration_profiles WHERE profile_id = ?",
        (profile_id,),
    ).fetchone()
    if profile is None:
        return None
    rows = conn.execute(
        """SELECT start_ts, end_ts, scale, offset_value
           FROM calibration_intervals WHERE profile_id = ? ORDER BY seq""",
        (profile_id,),
    ).fetchall()
    return {
        "profile": dict(profile),
        "intervals": [dict(r) for r in rows],
    }


def get_profile(profile_id: str) -> Optional[dict[str, Any]]:
    """读取校准档案（含生效区间）；不存在返回 None。"""
    conn = _connect()
    try:
        result = get_profile_for_update(conn, profile_id)
    finally:
        conn.close()
    return result


def save_profile(
    conn: sqlite3.Connection,
    profile_id: str,
    device_id: str,
    content_hash: str,
    intervals: list[tuple[float, float, float, float]],
) -> None:
    """在给定事务内写入不可变校准档案及其按序生效区间。

    intervals 元素为 (start_epoch, end_epoch, scale, offset)。profile_id
    唯一约束冲突会抛 IntegrityError，由调用方按幂等/换内容规则处理。
    """
    conn.execute(
        """INSERT INTO calibration_profiles
           (profile_id, device_id, content_hash, created_at)
           VALUES (?, ?, ?, ?)""",
        (profile_id, device_id, content_hash, time.time()),
    )
    conn.executemany(
        """INSERT INTO calibration_intervals
           (profile_id, seq, start_ts, end_ts, scale, offset_value)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            (profile_id, seq, start, end, scale, offset)
            for seq, (start, end, scale, offset) in enumerate(intervals)
        ],
    )
