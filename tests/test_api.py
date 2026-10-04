"""API 层测试：检测结果、幂等回放、内容冲突、校验、持久化。"""

import json
import os
import tempfile
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-test-"), "test.db")
os.environ["COLDCHAIN_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.main import app  # noqa: E402


def iso(epoch_seconds: int) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def iso_out(epoch_seconds: int) -> str:
    """输出时间戳固定毫秒精度且带 Z 后缀。"""
    return iso(epoch_seconds).replace("Z", ".000Z")


def make_payload(batch_id="B-001", device_id="reefer-01"):
    return {
        "batch_id": batch_id,
        "device_id": device_id,
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": [
            {"timestamp": iso(0), "temperature": 9.0},
            {"timestamp": iso(60), "temperature": 9.5},
            {"timestamp": iso(120), "temperature": 5.0},   # 短暂回落
            {"timestamp": iso(180), "temperature": 10.0},
            {"timestamp": iso(240), "temperature": 11.0},
            {"timestamp": iso(1200), "temperature": 9.0},  # 跨缺口，拆段
            {"timestamp": iso(1260), "temperature": 9.0},
        ],
        "segments": [
            {
                "segment_id": "seg-1",
                "segment_type": "loading",
                "start": iso(0),
                "end": iso(600),
                "lower_limit": 0.0,
                "upper_limit": 8.0,
            },
            {
                "segment_id": "seg-2",
                "segment_type": "transport",
                "start": iso(600),
                "end": iso(1800),
                "lower_limit": 0.0,
                "upper_limit": 8.0,
            },
        ],
    }


def make_cross_segment_payload(batch_id="B-XSEG"):
    """新旧口径分歧数据：seg-1=[0,1000)、seg-2=[1000,1200)。

    异常区间 [990,1100] 内 seg-1 有 3 个越界采样、seg-2 仅 1 个；
    按重叠秒数 seg-2(100) > seg-1(10)，旧口径会误归因 seg-2。
    """
    return {
        "batch_id": batch_id,
        "device_id": "reefer-01",
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": [
            {"timestamp": iso(990), "temperature": 9.0},
            {"timestamp": iso(995), "temperature": 9.0},
            {"timestamp": iso(999), "temperature": 9.0},
            {"timestamp": iso(1100), "temperature": 9.0},
        ],
        "segments": [
            {
                "segment_id": "seg-1",
                "segment_type": "loading",
                "start": iso(0),
                "end": iso(1000),
                "lower_limit": 0.0,
                "upper_limit": 8.0,
            },
            {
                "segment_id": "seg-2",
                "segment_type": "transport",
                "start": iso(1000),
                "end": iso(1200),
                "lower_limit": 0.0,
                "upper_limit": 8.0,
            },
        ],
    }


def _seed_legacy_cross_segment_batch(batch_id="B-LEGACY"):
    """直接向 SQLite 写入一批旧口径数据：原始采样正确、异常行按秒数误归因。

    模拟线上已被旧版本写入的批次（anomalies 行的 sample_counts_json 为
    NULL，attributed_segment_id 错误地写成 seg-2）。
    """
    import sqlite3
    import time

    payload = make_cross_segment_payload(batch_id=batch_id)
    # 旧版本的哈希算法与当前一致（只有归因口径变化），按真实哈希入库，
    # 这样相同请求 POST 才能命中幂等、不同请求仍应 409。
    from app.models import AnalysisRequest
    from app.main import _canonical_hash

    request_hash = _canonical_hash(AnalysisRequest.model_validate(payload))
    request_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    with sqlite3.connect(_TMP_DB) as conn:
        conn.execute(
            """INSERT INTO batches
               (batch_id, device_id, request_hash, request_json,
                anomaly_count, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (batch_id, payload["device_id"], request_hash,
             request_json, 1, time.time()),
        )
        # 旧行：归因 seg-2（错误）；除 start/end 外的字段按 seg-2 口径填
        conn.execute(
            """INSERT INTO anomalies
               (batch_id, seq, start_ts, end_ts, peak_temperature, peak_ts,
                direction, peak_segment_id, attributed_segment_id,
                attributed_segment_type, duration_seconds, coverage_json,
                sample_counts_json)
               VALUES (?, 0, 990, 1100, 9.0, 990, 'high', 'seg-1', 'seg-2',
                       'transport', 110.0,
                       '{"seg-2": 100.0, "seg-1": 10.0}', NULL)""",
            (batch_id,),
        )
    return payload


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 先用旧 schema 建库并留一列未迁移的旧行，模拟线上历史库；
        # init_db() 随后补列，旧行 sample_counts_json 仍为 NULL。
        import sqlite3

        with sqlite3.connect(_TMP_DB) as conn:
            conn.executescript(
                """
                CREATE TABLE batches (
                    batch_id      TEXT PRIMARY KEY,
                    device_id     TEXT NOT NULL,
                    request_hash  TEXT NOT NULL,
                    request_json  TEXT NOT NULL,
                    anomaly_count INTEGER NOT NULL,
                    created_at    REAL NOT NULL
                );
                CREATE TABLE anomalies (
                    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id                TEXT NOT NULL,
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
                    UNIQUE(batch_id, seq)
                );
                """
            )
        db.init_db()
        cls.client = TestClient(app)

    def test_01_health(self):
        r = self.client.get("/health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "ok")

    def test_02_analyze_merges_dips_and_splits_gaps(self):
        r = self.client.post("/api/v1/batches/analyze", json=make_payload())
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertFalse(body["cached"])
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        self.assertEqual(body["anomaly_count"], 2)

        first, second = body["anomalies"]
        # 第一段：0–240，回落不切断，峰值 11
        self.assertEqual(first["start"], iso_out(0))
        self.assertEqual(first["end"], iso_out(240))
        self.assertEqual(first["peak_timestamp"], iso_out(240))
        self.assertEqual(first["peak_temperature"], 11.0)
        self.assertEqual(first["duration_seconds"], 240.0)
        self.assertEqual(first["attributed_segment_id"], "seg-1")
        # 第二段：缺口后 1200–1260
        self.assertEqual(second["start"], iso_out(1200))
        self.assertEqual(second["end"], iso_out(1260))
        self.assertEqual(second["attributed_segment_id"], "seg-2")

    def test_03_repeat_request_is_idempotent(self):
        payload = make_payload()
        r1 = self.client.post("/api/v1/batches/analyze", json=payload)
        r2 = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        self.assertTrue(r2.json()["cached"])
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertEqual(
            r1.json()["anomalies"], r2.json()["anomalies"]
        )

    def test_04_get_batch_replays(self):
        r = self.client.get("/api/v1/batches/B-001")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertTrue(body["cached"])
        self.assertEqual(body["anomaly_count"], 2)

    def test_05_get_unknown_batch_404(self):
        r = self.client.get("/api/v1/batches/nope")
        self.assertEqual(r.status_code, 404)

    def test_06_same_batch_id_different_payload_conflicts(self):
        payload = make_payload()
        payload["samples"] = [
            {"timestamp": iso(0), "temperature": 99.0},
        ]
        r = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r.status_code, 409)

    def test_07_overlapping_segments_rejected(self):
        payload = make_payload(batch_id="B-002")
        payload["segments"][1]["start"] = iso(300)  # 与 seg-1 重叠
        r = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r.status_code, 422)

    def test_08_default_segment_ids_generated(self):
        payload = make_payload(batch_id="B-003")
        for seg in payload["segments"]:
            del seg["segment_id"]
        r = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r.status_code, 200, r.text)
        attributed = {a["attributed_segment_id"]
                      for a in r.json()["anomalies"]}
        self.assertEqual(attributed, {"seg-1", "seg-2"})

    def test_09_naive_timestamp_treated_as_utc(self):
        payload = make_payload(batch_id="B-004")
        payload["samples"] = [
            {"timestamp": "2025-01-01T00:00:00", "temperature": 9.0},
            {"timestamp": "2025-01-01T00:01:00", "temperature": 9.0},
        ]
        payload["segments"] = [
            {
                "segment_type": "loading",
                "start": "2025-01-01T00:00:00",
                "end": "2025-01-01T01:00:00",
                "lower_limit": 0.0,
                "upper_limit": 8.0,
            }
        ]
        r = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["anomaly_count"], 1)
        self.assertEqual(r.json()["anomalies"][0]["start"],
                         "2025-01-01T00:00:00.000Z")
    def test_10_persistence_rowcount(self):
        import sqlite3

        with sqlite3.connect(_TMP_DB) as conn:
            batches = conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0]
            anomalies = conn.execute(
                "SELECT COUNT(*) FROM anomalies").fetchone()[0]
        self.assertGreaterEqual(batches, 3)
        self.assertGreaterEqual(anomalies, 2)

        # 回放结果可反序列化
        r = self.client.get("/api/v1/batches/B-001")
        json.loads(r.content)

    def test_11_cross_segment_attribution_by_sample_count(self):
        """POST 新口径：跨段异常按区间内实际采样数归因，而非重叠秒数。"""
        r = self.client.post(
            "/api/v1/batches/analyze", json=make_cross_segment_payload()
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["anomaly_count"], 1)
        a = body["anomalies"][0]
        # 重叠秒数 seg-2 占优，但 seg-1 有 3 个采样、seg-2 只有 1 个
        self.assertEqual(
            a["segment_coverage_seconds"], {"seg-2": 100.0, "seg-1": 10.0}
        )
        self.assertEqual(
            a["segment_sample_counts"], {"seg-1": 3, "seg-2": 1}
        )
        self.assertEqual(a["attributed_segment_id"], "seg-1")
        self.assertEqual(a["attributed_segment_type"], "loading")
        # 峰值点 990 在 seg-1
        self.assertEqual(a["peak_segment_id"], "seg-1")
        self.assertEqual(a["peak_timestamp"], iso_out(990))

    def test_12_get_replays_legacy_row_with_recomputed_attribution(self):
        """旧口径写入的批次：GET 必须依据原始采样重算并回写修正归因。"""
        _seed_legacy_cross_segment_batch("B-LEGACY-GET")

        r = self.client.get("/api/v1/batches/B-LEGACY-GET")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertTrue(body["cached"])
        a = body["anomalies"][0]
        # 旧行误归因 seg-2，重算后修正为 seg-1
        self.assertEqual(a["attributed_segment_id"], "seg-1")
        self.assertEqual(a["attributed_segment_type"], "loading")
        self.assertEqual(
            a["segment_sample_counts"], {"seg-1": 3, "seg-2": 1}
        )

        # 修正必须持久化：再次 GET 不变化，且库里的异常行已被覆盖
        r2 = self.client.get("/api/v1/batches/B-LEGACY-GET")
        self.assertEqual(r2.json()["anomalies"], body["anomalies"])
        import sqlite3
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                """SELECT attributed_segment_id, sample_counts_json
                   FROM anomalies WHERE batch_id = ?""",
                ("B-LEGACY-GET",),
            ).fetchone()
        self.assertEqual(row[0], "seg-1")
        self.assertEqual(json.loads(row[1]), {"seg-1": 3, "seg-2": 1})

    def test_13_repeat_post_on_legacy_batch_replays_corrected(self):
        """旧口径批次相同请求重复 POST：幂等命中但返回重算后的修正结果。"""
        payload = _seed_legacy_cross_segment_batch("B-LEGACY-POST")

        r = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")
        body = r.json()
        self.assertTrue(body["cached"])
        self.assertEqual(
            body["anomalies"][0]["attributed_segment_id"], "seg-1"
        )
        self.assertEqual(
            body["anomalies"][0]["segment_sample_counts"],
            {"seg-1": 3, "seg-2": 1},
        )

        # 再 POST 一次仍然幂等：结果完全一致
        r2 = self.client.post("/api/v1/batches/analyze", json=payload)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertEqual(r2.json()["anomalies"], body["anomalies"])

        # 同 batch_id 不同内容仍然 409，重算逻辑不改变冲突语义
        conflict = make_cross_segment_payload(batch_id="B-LEGACY-POST")
        conflict["samples"] = [
            {"timestamp": iso(0), "temperature": 99.0},
        ]
        r3 = self.client.post("/api/v1/batches/analyze", json=conflict)
        self.assertEqual(r3.status_code, 409)


if __name__ == "__main__":
    unittest.main()
