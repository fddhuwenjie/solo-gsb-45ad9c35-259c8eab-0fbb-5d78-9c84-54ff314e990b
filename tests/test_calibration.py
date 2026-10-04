"""设备温度校准档案测试。

覆盖：
- 档案提交：区间校验（排序/重叠/半开/正系数/非空）、同内容幂等、换内容 409；
- 校准检测：corrected=raw*scale+offset 后的越界与峰值、无覆盖整批 422 不落库、
  档案缺失/设备不匹配 409、幂等回放与 GET 重算；
- 摘要/哈希：档案标识与内容进入 request_hash 与 analysis_digest，旧调用
  （不指定档案）摘要口径不变；
- 与处置复核、运输联单的闭环：同一档案重放得到同一摘要，released/held 结论稳定。
"""

import hashlib
import json
import os
import sqlite3
import tempfile
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-calib-"), "calib.db")
os.environ["COLDCHAIN_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.detection import (  # noqa: E402
    CalibrationGapError,
    CalibrationInterval,
    Sample,
    apply_calibration,
)
from app.main import (  # noqa: E402
    _canonical_hash,
    _profile_descriptor,
    app,
)
from app.models import AnalysisRequest  # noqa: E402


def iso(epoch_seconds) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def iso_out(epoch_seconds) -> str:
    return iso(epoch_seconds).replace("Z", ".000Z")


DEVICE = "reefer-calib-01"
PROFILE = "CAL-P1"


def profile_body(
    profile_id=PROFILE,
    device_id=DEVICE,
    intervals=None,
):
    if intervals is None:
        # 单段 [00:00, 00:10)：corrected = raw * 1.0 + 0.5
        intervals = [
            {"start": iso(0), "end": iso(600), "scale": 1.0, "offset": 0.5}
        ]
    return {
        "profile_id": profile_id,
        "device_id": device_id,
        "intervals": intervals,
    }


def analysis_body(
    batch_id,
    device_id=DEVICE,
    profile_id=None,
    samples=None,
    segments=None,
):
    if samples is None:
        # 8.0 恰好在上限（不校准则不越界）；5.0 短暂回落；9.0 校准后 9.5 为峰值
        samples = [
            {"timestamp": iso(0), "temperature": 8.0},
            {"timestamp": iso(60), "temperature": 5.0},
            {"timestamp": iso(120), "temperature": 9.0},
        ]
    if segments is None:
        segments = [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ]
    body = {
        "batch_id": batch_id,
        "device_id": device_id,
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": samples,
        "segments": segments,
    }
    if profile_id is not None:
        body["profile_id"] = profile_id
    return body


# ---------- 纯函数：区间选取与校准 ----------


class CalibrationFunctionTests(unittest.TestCase):
    def test_half_open_intervals_select_and_correct(self):
        intervals = [
            CalibrationInterval(0.0, 600.0, 1.0, 0.5),
            CalibrationInterval(600.0, 1200.0, 1.1, 0.2),
        ]
        out = apply_calibration(
            [Sample(0.0, 8.0), Sample(599.0, 10.0),
             Sample(600.0, 10.0), Sample(1199.0, 0.0)],
            intervals,
        )
        self.assertEqual([s.temperature for s in out],
                         [8.5, 10.5, 11.2, 0.2])
        # 时间戳保持不变
        self.assertEqual([s.timestamp for s in out], [0.0, 599.0, 600.0, 1199.0])

    def test_endpoint_is_exclusive(self):
        intervals = [CalibrationInterval(0.0, 100.0, 1.0, 0.5)]
        # 恰好落在 end 上：[start, end) 不覆盖
        with self.assertRaises(CalibrationGapError):
            apply_calibration([Sample(100.0, 1.0)], intervals)
        # start 端含
        self.assertEqual(
            apply_calibration([Sample(0.0, 0.0)], intervals)[0].temperature,
            0.5,
        )

    def test_unsorted_input_intervals_supported(self):
        out = apply_calibration(
            [Sample(700.0, 2.0)],
            [CalibrationInterval(600.0, 1200.0, 2.0, 0.1),
             CalibrationInterval(0.0, 600.0, 1.0, 0.5)],
        )
        self.assertEqual(out[0].temperature, 4.1)


# ---------- API：档案提交 ----------


class ProfileApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)

    def _profile_count(self, profile_id):
        with sqlite3.connect(_TMP_DB) as conn:
            p = conn.execute(
                "SELECT COUNT(*) FROM calibration_profiles WHERE profile_id = ?",
                (profile_id,),
            ).fetchone()[0]
            n = conn.execute(
                "SELECT COUNT(*) FROM calibration_intervals WHERE profile_id = ?",
                (profile_id,),
            ).fetchone()[0]
        return p, n

    def test_01_submit_profile_miss_then_idempotent_hit(self):
        pid = "CAL-API"
        r1 = self.client.post(
            "/api/v1/calibration-profiles", json=profile_body(profile_id=pid)
        )
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r1.headers.get("X-Cache"), "MISS")
        body = r1.json()
        self.assertFalse(body["cached"])
        self.assertEqual(body["profile_id"], pid)
        self.assertEqual(body["device_id"], DEVICE)
        self.assertEqual(
            body["intervals"],
            [{"start": iso_out(0), "end": iso_out(600),
              "scale": 1.0, "offset": 0.5}],
        )

        r2 = self.client.post(
            "/api/v1/calibration-profiles", json=profile_body(profile_id=pid)
        )
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        self.assertEqual(r2.json()["intervals"], body["intervals"])
        # 只有一组档案与区间行
        self.assertEqual(self._profile_count(pid), (1, 1))

    def test_02_changed_content_conflicts_and_keeps_original(self):
        pid = "CAL-API"
        changed = profile_body(
            profile_id=pid,
            intervals=[
                {"start": iso(0), "end": iso(600), "scale": 1.01, "offset": 0.5},
            ],
        )
        r = self.client.post("/api/v1/calibration-profiles", json=changed)
        self.assertEqual(r.status_code, 409)

        changed = profile_body(
            profile_id=pid,
            intervals=[
                {"start": iso(0), "end": iso(600), "scale": 1.0, "offset": 0.6},
            ],
        )
        self.assertEqual(
            self.client.post("/api/v1/calibration-profiles", json=changed).status_code,
            409,
        )

        changed = profile_body(
            profile_id=pid,
            intervals=[
                {"start": iso(0), "end": iso(599), "scale": 1.0, "offset": 0.5},
            ],
        )
        self.assertEqual(
            self.client.post("/api/v1/calibration-profiles", json=changed).status_code,
            409,
        )

        # 换设备同样视为换内容
        self.assertEqual(
            self.client.post(
                "/api/v1/calibration-profiles",
                json=profile_body(profile_id=pid, device_id="reefer-other"),
            ).status_code,
            409,
        )
        # 原档案内容保持不变
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                """SELECT start_ts, end_ts, scale, offset_value
                   FROM calibration_intervals WHERE profile_id = ? ORDER BY seq""",
                (pid,),
            ).fetchone()
        self.assertEqual(row, (0.0, 600.0, 1.0, 0.5))

    def test_03_intervals_sorted_with_adjacent_boundaries_allowed(self):
        body = profile_body(
            profile_id="CAL-SORT",
            intervals=[
                # 乱序提交、首尾相接（前一段 end == 后一段 start）允许
                {"start": iso(600), "end": iso(1200), "scale": 1.1, "offset": 0.2},
                {"start": iso(0), "end": iso(600), "scale": 1.0, "offset": 0.5},
            ],
        )
        r = self.client.post("/api/v1/calibration-profiles", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(
            [iv["start"] for iv in r.json()["intervals"]],
            [iso_out(0), iso_out(600)],
        )

    def test_04_overlapping_intervals_rejected(self):
        r = self.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(
                profile_id="CAL-OVERLAP",
                intervals=[
                    {"start": iso(0), "end": iso(700),
                     "scale": 1.0, "offset": 0.5},
                    {"start": iso(600), "end": iso(1200),
                     "scale": 1.0, "offset": 0.5},
                ],
            ),
        )
        self.assertEqual(r.status_code, 422)

    def test_05_end_not_after_start_rejected(self):
        r = self.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(
                profile_id="CAL-BADRANGE",
                intervals=[
                    {"start": iso(600), "end": iso(600),
                     "scale": 1.0, "offset": 0.5},
                ],
            ),
        )
        self.assertEqual(r.status_code, 422)

    def test_06_empty_intervals_rejected(self):
        r = self.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(profile_id="CAL-EMPTY", intervals=[]),
        )
        self.assertEqual(r.status_code, 422)

    def test_07_non_positive_or_non_finite_coefficients_rejected(self):
        for scale, offset in [
            (0.0, 0.5), (-1.0, 0.5), (1.0, 0.0), (1.0, -0.5),
        ]:
            r = self.client.post(
                "/api/v1/calibration-profiles",
                json=profile_body(
                    profile_id=f"CAL-COEF-{scale}-{offset}",
                    intervals=[
                        {"start": iso(0), "end": iso(600),
                         "scale": scale, "offset": offset},
                    ],
                ),
            )
            self.assertEqual(r.status_code, 422, (scale, offset, r.text))

        # Infinity / NaN 也必须拒绝（Python 端需 allow_nan 才能发出 JSON 令牌，
        # pydantic 在解析时按 allow_inf_nan=False 拒绝）
        r = self.client.post(
            "/api/v1/calibration-profiles",
            data=json.dumps(profile_body(
                profile_id="CAL-INF",
                intervals=[
                    {"start": iso(0), "end": iso(600),
                     "scale": 1e400, "offset": 0.5},
                ],
            ), allow_nan=True),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(r.status_code, 422)
        r = self.client.post(
            "/api/v1/calibration-profiles",
            data=json.dumps(profile_body(
                profile_id="CAL-NAN",
                intervals=[
                    {"start": iso(0), "end": iso(600),
                     "scale": 1.0, "offset": float("nan")},
                ],
            ), allow_nan=True),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(r.status_code, 422)


# ---------- API：带档案的分析 ----------


class CalibratedAnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)
        # 单段档案 CAL-P1：+0.5（由前一个用例类创建；为隔离这里确保存在）
        r = cls.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(profile_id=PROFILE, intervals=[
                {"start": iso(0), "end": iso(600), "scale": 1.0, "offset": 0.5},
            ]),
        )
        assert r.status_code in (200,), r.text
        # 两段档案：[0,600) +0.5；[600,1200) raw*1.1+0.2
        cls.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(profile_id="CAL-P2", intervals=[
                {"start": iso(0), "end": iso(600), "scale": 1.0, "offset": 0.5},
                {"start": iso(600), "end": iso(1200), "scale": 1.1, "offset": 0.2},
            ]),
        )

    def _batch_exists(self, batch_id):
        with sqlite3.connect(_TMP_DB) as conn:
            return conn.execute(
                "SELECT 1 FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone() is not None

    def test_01_calibrated_values_drive_detection_and_response(self):
        r = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("CAL-B1", profile_id=PROFILE),
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["profile_id"], PROFILE)
        self.assertEqual(body["anomaly_count"], 1)
        a = body["anomalies"][0]
        # 8.0 校准后 8.5 越界（原始值恰好在上限，不校准则不算越界），
        # 回落 5.5 被收回，峰值为 9.0+0.5=9.5
        self.assertEqual(a["start"], iso_out(0))
        self.assertEqual(a["end"], iso_out(120))
        self.assertEqual(a["peak_temperature"], 9.5)
        self.assertEqual(a["peak_timestamp"], iso_out(120))
        self.assertEqual(a["direction"], "high")
        self.assertEqual(a["attributed_segment_id"], "seg-1")

    def test_02_without_profile_keeps_legacy_behavior(self):
        r = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("CAL-RAW1"),
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertIsNone(body["profile_id"])
        # 原始口径：8.0 在限内不越界，仅 9.0 单点越界
        self.assertEqual(body["anomaly_count"], 1)
        a = body["anomalies"][0]
        self.assertEqual(a["start"], iso_out(120))
        self.assertEqual(a["peak_temperature"], 9.0)

    def test_03_digest_includes_profile_content(self):
        prof = db.get_profile(PROFILE)
        descriptor = _profile_descriptor(prof)
        cal = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("CAL-DIG", profile_id=PROFILE),
        ).json()
        raw = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("CAL-DIG-RAW"),
        ).json()
        # 同一批原始采样：带档案与不带档案的摘要必然不同
        self.assertNotEqual(
            cal["analysis_digest"], raw["analysis_digest"]
        )
        # 摘要输入为 {"anomalies": ..., "profile": 档案标识与内容}
        from app.main import _run_detection
        from app.models import AnalysisRequest

        req = AnalysisRequest.model_validate(
            analysis_body("CAL-DIG", profile_id=PROFILE)
        )
        anomalies = _run_detection(req, descriptor)
        canonical = json.dumps(
            {"anomalies": anomalies, "profile": descriptor},
            sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        )
        self.assertEqual(
            cal["analysis_digest"],
            hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )
        # 不带档案仍是旧口径：sha256(规范异常 JSON)
        from app.main import _canonical_anomaly_json

        raw_req = AnalysisRequest.model_validate(
            analysis_body("CAL-DIG-RAW")
        )
        raw_anomalies = _run_detection(raw_req)
        self.assertEqual(
            raw["analysis_digest"],
            hashlib.sha256(
                _canonical_anomaly_json(raw_anomalies).encode("utf-8")
            ).hexdigest(),
        )
        self.assertEqual(len(raw["analysis_digest"]), 64)

    def test_04_replay_and_get_use_same_profile_and_digest(self):
        body = analysis_body("CAL-REPLAY", profile_id=PROFILE)
        r1 = self.client.post("/api/v1/batches/analyze", json=body)
        r2 = self.client.post("/api/v1/batches/analyze", json=body)
        r3 = self.client.get("/api/v1/batches/CAL-REPLAY")
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        self.assertEqual(r2.json()["profile_id"], PROFILE)
        self.assertEqual(
            r1.json()["anomalies"], r2.json()["anomalies"]
        )
        self.assertEqual(
            r1.json()["analysis_digest"], r2.json()["analysis_digest"]
        )
        self.assertEqual(r3.status_code, 200)
        self.assertEqual(
            r3.json()["anomalies"], r1.json()["anomalies"]
        )
        self.assertEqual(
            r3.json()["analysis_digest"], r1.json()["analysis_digest"]
        )
        self.assertEqual(r3.json()["profile_id"], PROFILE)

    def test_05_uncovered_sample_rejected_422_and_nothing_written(self):
        # 区间 [0,600)：ts=600 恰好落在右端点，半开区间不覆盖
        body = analysis_body(
            "CAL-GAP",
            profile_id=PROFILE,
            samples=[
                {"timestamp": iso(0), "temperature": 9.0},
                {"timestamp": iso(600), "temperature": 9.0},
            ],
            segments=[
                {"segment_id": "seg-1", "segment_type": "loading",
                 "start": iso(0), "end": iso(1200),
                 "lower_limit": 0.0, "upper_limit": 8.0},
            ],
        )
        r = self.client.post("/api/v1/batches/analyze", json=body)
        self.assertEqual(r.status_code, 422, r.text)
        self.assertIn(iso_out(600), r.json()["detail"])
        self.assertFalse(self._batch_exists("CAL-GAP"))

    def test_06_two_intervals_pick_by_sample_time(self):
        # ts=0 raw=8.0 → +0.5 = 8.5 越界；ts=600 raw=8.0 → 8.0*1.1+0.2=9.0 越界
        body = analysis_body(
            "CAL-P2-B1",
            profile_id="CAL-P2",
            samples=[
                {"timestamp": iso(0), "temperature": 8.0},
                {"timestamp": iso(600), "temperature": 8.0},
            ],
            segments=[
                {"segment_id": "seg-1", "segment_type": "loading",
                 "start": iso(0), "end": iso(1200),
                 "lower_limit": 0.0, "upper_limit": 8.0},
            ],
        )
        r = self.client.post("/api/v1/batches/analyze", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        anomalies = r.json()["anomalies"]
        # 两点间隔正好 600s（>=gap）→ 拆为两条；峰值分别 8.5 与 9.0
        self.assertEqual(len(anomalies), 2)
        self.assertEqual(anomalies[0]["peak_temperature"], 8.5)
        self.assertEqual(anomalies[1]["peak_temperature"], 9.0)

    def test_07_unknown_profile_conflicts_409_and_nothing_written(self):
        r = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("CAL-NOPROF", profile_id="CAL-MISSING"),
        )
        self.assertEqual(r.status_code, 409)
        self.assertFalse(self._batch_exists("CAL-NOPROF"))

    def test_08_profile_of_other_device_conflicts_409(self):
        r = self.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body(
                "CAL-OTHERDEV", device_id="reefer-other", profile_id=PROFILE
            ),
        )
        self.assertEqual(r.status_code, 409)
        self.assertFalse(self._batch_exists("CAL-OTHERDEV"))

    def test_09_adding_or_removing_profile_changes_request_hash(self):
        body = analysis_body("CAL-HASH", profile_id=PROFILE)
        self.assertEqual(
            self.client.post("/api/v1/batches/analyze", json=body).status_code,
            200,
        )
        # 同 batch_id 去掉档案 → 409
        no_profile = {k: v for k, v in body.items() if k != "profile_id"}
        self.assertEqual(
            self.client.post(
                "/api/v1/batches/analyze", json=no_profile
            ).status_code,
            409,
        )
        # 同 batch_id 换另一份档案内容 → 409
        body["profile_id"] = "CAL-P2"
        self.assertEqual(
            self.client.post("/api/v1/batches/analyze", json=body).status_code,
            409,
        )

    def test_10_profile_id_persisted_on_batch_row(self):
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                "SELECT profile_id FROM batches WHERE batch_id = 'CAL-B1'"
            ).fetchone()
        self.assertEqual(row[0], PROFILE)
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                "SELECT profile_id FROM batches WHERE batch_id = 'CAL-RAW1'"
            ).fetchone()
        self.assertIsNone(row[0])

    def test_11_legacy_batch_hash_without_profile_key_still_replays(self):
        """升级前写入的无档案批次仍须幂等回放（未指定档案的旧调用保持不变）。

        旧版本 AnalysisRequest 没有 profile_id 字段，旧哈希与新代码对
        无 profile_id 请求算出的哈希必须一致；这里直接用当前无档案口径
        的哈希播种旧批次行（profile_id 列为 NULL、无异常行），验证回放。
        """
        import time

        from app.main import _canonical_hash
        from app.models import AnalysisRequest

        legacy_id = "CAL-LEGACY-NO-PROFILE"
        body = analysis_body(legacy_id)
        # 新代码对无 profile_id 请求的载荷不得包含 profile 键（与旧版口径一致）
        dumped = AnalysisRequest.model_validate(body).model_dump(
            mode="json", exclude={"batch_id"}
        )
        dumped.pop("profile_id", None)
        self.assertNotIn("profile", dumped)
        self.assertNotIn("profile_id", dumped)
        legacy_hash = _canonical_hash(AnalysisRequest.model_validate(body))
        stored_json = json.dumps(body, ensure_ascii=False, sort_keys=True)
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO batches
                   (batch_id, device_id, profile_id, request_hash, request_json,
                    anomaly_count, created_at)
                   VALUES (?, ?, NULL, ?, ?, 0, ?)""",
                (legacy_id, body["device_id"], legacy_hash,
                 stored_json, time.time()),
            )

        # 相同请求（无 profile_id）重复 POST：必须 HIT，而不是 409
        r = self.client.post("/api/v1/batches/analyze", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "HIT")
        self.assertTrue(r.json()["cached"])
        self.assertIsNone(r.json()["profile_id"])
        # 旧口径温度：仅 9.0 单点越界
        self.assertEqual(
            r.json()["anomalies"][0]["peak_temperature"], 9.0
        )


# ---------- 处置复核 / 联单与校准档案闭环 ----------


class CalibratedReviewShipmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)
        cls.client.post(
            "/api/v1/calibration-profiles",
            json=profile_body(
                profile_id="CAL-WF",
                intervals=[
                    {"start": iso(0), "end": iso(1800),
                     "scale": 1.0, "offset": 0.5},
                ],
            ),
        )
        r = cls.client.post(
            "/api/v1/batches/analyze",
            json=analysis_body("CAL-WF-B", profile_id="CAL-WF"),
        )
        assert r.status_code == 200, r.text
        cls.digest = r.json()["analysis_digest"]
        cls.anomaly_end = r.json()["anomalies"][0]["end"]

    def test_01_review_with_calibrated_digest_closes_case(self):
        decisions = [
            {"seq": 0, "decision": "resolved",
             "handled_at": iso(120), "note": "校准后确认复冷"},
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "CAL-WF-B",
                "case_id": "CAL-CASE-1",
                "analysis_digest": self.digest,
                "decisions": decisions,
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "closed")
        self.assertEqual(r.json()["analysis_digest"], self.digest)

    def test_02_legacy_style_digest_is_stale_for_profile_batch(self):
        # 只对异常 JSON（不含档案）取摘要：对档案批次必然过期 → 409，且不落库
        from app.main import _canonical_anomaly_json

        r_get = self.client.get("/api/v1/batches/CAL-WF-B").json()
        stale = hashlib.sha256(
            _canonical_anomaly_json(
                [
                    {
                        "start": 0,
                        "end": 120,
                        "peak_temperature": a["peak_temperature"],
                        "peak_timestamp": 120,
                        "direction": a["direction"],
                        "peak_segment_id": a["peak_segment_id"],
                        "attributed_segment_id": a["attributed_segment_id"],
                        "attributed_segment_type": a["attributed_segment_type"],
                        "duration_seconds": a["duration_seconds"],
                        "segment_coverage_seconds": a["segment_coverage_seconds"],
                        "segment_sample_counts": a["segment_sample_counts"],
                    }
                    for a in r_get["anomalies"]
                ]
            ).encode("utf-8")
        ).hexdigest()
        self.assertNotEqual(stale, self.digest)
        r = self.client.post(
            "/api/v1/cases/review",
            json={
                "batch_id": "CAL-WF-B",
                "case_id": "CAL-CASE-STALE",
                "analysis_digest": stale,
                "decisions": [
                    {"seq": 0, "decision": "resolved",
                     "handled_at": iso(120), "note": "x"},
                ],
            },
        )
        self.assertEqual(r.status_code, 409)
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM cases WHERE case_id = 'CAL-CASE-STALE'"
                ).fetchone()
            )

    def test_03_shipment_snapshot_verifies_profile_batch(self):
        r = self.client.post(
            "/api/v1/shipments/snapshot",
            json={
                "shipment_id": "CAL-SHP-1",
                "batches": [
                    {
                        "batch_id": "CAL-WF-B",
                        "analysis_digest": self.digest,
                        "case_id": "CAL-CASE-1",
                        "handoff_at": iso(1800),
                    }
                ],
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["result"], "released")
        self.assertEqual(body["batches"][0]["anomaly_count"], 1)
        self.assertEqual(
            body["batches"][0]["analysis_digest"], self.digest
        )

        # 同内容幂等回放，结论保持一致
        r2 = self.client.post(
            "/api/v1/shipments/snapshot",
            json={
                "shipment_id": "CAL-SHP-1",
                "batches": [
                    {
                        "batch_id": "CAL-WF-B",
                        "analysis_digest": self.digest,
                        "case_id": "CAL-CASE-1",
                        "handoff_at": iso(1800),
                    }
                ],
            },
        )
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertEqual(r2.json()["result"], "released")


if __name__ == "__main__":
    unittest.main()
