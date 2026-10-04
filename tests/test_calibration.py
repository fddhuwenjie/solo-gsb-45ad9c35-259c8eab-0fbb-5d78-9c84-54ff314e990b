"""设备温度校准档案测试：档案幂等/冲突、校准检测、覆盖原子性、快照一致性。"""

import json
import os
import sqlite3
import tempfile
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-cal-"), "calibration.db")
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
    return iso(epoch_seconds).replace("Z", ".000Z")


def make_profile_body(profile_id="CAL-1", device_id="reefer-01"):
    """两段校准区间：[0,1000) 放大 2 倍减 1；[1000,2000) 恒等。"""
    return {
        "device_id": device_id,
        "profile_id": profile_id,
        "intervals": [
            {"start": iso(0), "end": iso(1000), "scale": 2.0, "offset": -1.0},
            {"start": iso(1000), "end": iso(2000), "scale": 1.0, "offset": 0.0},
        ],
    }


def make_batch(batch_id, device_id="reefer-01", profile_id="CAL-1"):
    """原始温度全部在 [0,8] 内；校准后 60/120 两点越界（8.2 / 8.4）。"""
    return {
        "batch_id": batch_id,
        "device_id": device_id,
        "profile_id": profile_id,
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": [
            {"timestamp": iso(0), "temperature": 3.0},    # 校准后 5.0 正常
            {"timestamp": iso(60), "temperature": 4.6},   # 校准后 8.2 越界
            {"timestamp": iso(120), "temperature": 4.7},  # 校准后 8.4 越界（峰值）
            {"timestamp": iso(180), "temperature": 3.0},  # 校准后 5.0 正常
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "transport",
             "start": iso(0), "end": iso(1800),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


class CalibrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 与其他测试模块同进程运行时库路径可能已被固定，显式指向本模块独立库。
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)
        # 基础档案：CAL-1（reefer-01，两段区间）
        r = cls.client.post("/api/v1/devices/calibrations",
                            json=make_profile_body())
        assert r.status_code == 200, r.text
        # 缩小系数档案：CAL-2（reefer-01，[0,2000) 减半）
        r = cls.client.post("/api/v1/devices/calibrations", json={
            "device_id": "reefer-01",
            "profile_id": "CAL-2",
            "intervals": [
                {"start": iso(0), "end": iso(2000),
                 "scale": 0.5, "offset": 0.0},
            ],
        })
        assert r.status_code == 200, r.text
        # 属于另一台设备的档案：CAL-OTHER（reefer-02）
        r = cls.client.post("/api/v1/devices/calibrations",
                            json=make_profile_body("CAL-OTHER", "reefer-02"))
        assert r.status_code == 200, r.text

    # ---------- 工具 ----------

    def _profile_count(self):
        with sqlite3.connect(_TMP_DB) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM calibration_profiles"
            ).fetchone()[0]

    def _batch_row_count(self, batch_id):
        with sqlite3.connect(_TMP_DB) as conn:
            batches = conn.execute(
                "SELECT COUNT(*) FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()[0]
            anomalies = conn.execute(
                "SELECT COUNT(*) FROM anomalies WHERE batch_id = ?", (batch_id,)
            ).fetchone()[0]
        return batches, anomalies

    def _analyze(self, payload):
        return self.client.post("/api/v1/batches/analyze", json=payload)

    # ---------- 档案登记 ----------

    def test_01_register_profile_response(self):
        r = self.client.post("/api/v1/devices/calibrations",
                             json=make_profile_body("CAL-R1"))
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        body = r.json()
        self.assertFalse(body["cached"])
        self.assertEqual(body["profile_id"], "CAL-R1")
        self.assertEqual(body["device_id"], "reefer-01")
        self.assertEqual(len(body["profile_digest"]), 64)
        # 区间按提交内容回显，时间戳统一 UTC Z 后缀
        self.assertEqual(
            body["intervals"],
            [
                {"start": iso_out(0), "end": iso_out(1000),
                 "scale": 2.0, "offset": -1.0},
                {"start": iso_out(1000), "end": iso_out(2000),
                 "scale": 1.0, "offset": 0.0},
            ],
        )

    def test_02_register_same_profile_idempotent(self):
        body = make_profile_body("CAL-IDEM")
        r1 = self.client.post("/api/v1/devices/calibrations", json=body)
        self.assertEqual(r1.status_code, 200)
        count = self._profile_count()

        r2 = self.client.post("/api/v1/devices/calibrations", json=body)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        # 幂等回放不产生新行
        self.assertEqual(self._profile_count(), count)
        j1, j2 = r1.json(), r2.json()
        self.assertFalse(j1.pop("cached"))
        self.assertTrue(j2.pop("cached"))
        self.assertEqual(j1, j2)

    def test_03_register_changed_content_conflicts(self):
        body = make_profile_body("CAL-CHG")
        self.assertEqual(
            self.client.post("/api/v1/devices/calibrations",
                             json=body).status_code,
            200,
        )
        # 改 scale
        changed = json.loads(json.dumps(body))
        changed["intervals"][0]["scale"] = 1.5
        r = self.client.post("/api/v1/devices/calibrations", json=changed)
        self.assertEqual(r.status_code, 409)
        # 改 offset
        changed = json.loads(json.dumps(body))
        changed["intervals"][0]["offset"] = 0.25
        self.assertEqual(
            self.client.post("/api/v1/devices/calibrations",
                             json=changed).status_code,
            409,
        )
        # 改区间时间
        changed = json.loads(json.dumps(body))
        changed["intervals"][1]["end"] = iso(3000)
        self.assertEqual(
            self.client.post("/api/v1/devices/calibrations",
                             json=changed).status_code,
            409,
        )
        # 换设备（档案号唯一，换设备即换内容）
        changed = json.loads(json.dumps(body))
        changed["device_id"] = "reefer-99"
        self.assertEqual(
            self.client.post("/api/v1/devices/calibrations",
                             json=changed).status_code,
            409,
        )

    def test_04_profile_validation_rejected(self):
        def post(intervals):
            return self.client.post("/api/v1/devices/calibrations", json={
                "device_id": "reefer-01",
                "profile_id": "CAL-BAD",
                "intervals": intervals,
            })

        # 空区间
        self.assertEqual(post([]).status_code, 422)
        # end <= start
        self.assertEqual(
            post([{"start": iso(100), "end": iso(100),
                   "scale": 1.0, "offset": 0.0}]).status_code,
            422,
        )
        # 区间重叠
        self.assertEqual(
            post([
                {"start": iso(0), "end": iso(1000),
                 "scale": 1.0, "offset": 0.0},
                {"start": iso(500), "end": iso(1500),
                 "scale": 1.0, "offset": 0.0},
            ]).status_code,
            422,
        )
        # 未按时间排序（后一段整体在前一段之前）
        self.assertEqual(
            post([
                {"start": iso(1000), "end": iso(2000),
                 "scale": 1.0, "offset": 0.0},
                {"start": iso(0), "end": iso(500),
                 "scale": 1.0, "offset": 0.0},
            ]).status_code,
            422,
        )
        # scale 非正数
        self.assertEqual(
            post([{"start": iso(0), "end": iso(100),
                   "scale": 0.0, "offset": 0.0}]).status_code,
            422,
        )
        self.assertEqual(
            post([{"start": iso(0), "end": iso(100),
                   "scale": -1.0, "offset": 0.0}]).status_code,
            422,
        )
        # scale / offset 必须有限（手工构造含 Infinity/NaN 的 JSON 体）
        def post_raw(interval):
            body = json.dumps({
                "device_id": "reefer-01",
                "profile_id": "CAL-BAD",
                "intervals": [interval],
            })
            return self.client.post(
                "/api/v1/devices/calibrations",
                content=body,
                headers={"Content-Type": "application/json"},
            )

        self.assertEqual(
            post_raw({"start": iso(0), "end": iso(100),
                      "scale": float("inf"), "offset": 0.0}).status_code,
            422,
        )
        self.assertEqual(
            post_raw({"start": iso(0), "end": iso(100),
                      "scale": 1.0, "offset": float("nan")}).status_code,
            422,
        )
        # 全部拒绝，档案未落库
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM calibration_profiles"
                    " WHERE profile_id = 'CAL-BAD'"
                ).fetchone()
            )

    def test_05_touching_intervals_accepted(self):
        """相邻区间首尾相接（前段 end == 后段 start）不视为重叠。"""
        r = self.client.post("/api/v1/devices/calibrations", json={
            "device_id": "reefer-01",
            "profile_id": "CAL-TOUCH",
            "intervals": [
                {"start": iso(0), "end": iso(100), "scale": 1.0, "offset": 0.0},
                {"start": iso(100), "end": iso(200), "scale": 1.0, "offset": 0.0},
            ],
        })
        self.assertEqual(r.status_code, 200, r.text)

    def test_06_naive_interval_timestamps_treated_as_utc(self):
        r = self.client.post("/api/v1/devices/calibrations", json={
            "device_id": "reefer-01",
            "profile_id": "CAL-NAIVE",
            "intervals": [
                {"start": "1970-01-01T00:00:00",
                 "end": "1970-01-01T00:16:40",
                 "scale": 1.0, "offset": 0.0},
            ],
        })
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["intervals"][0]["start"],
                         "1970-01-01T00:00:00.000Z")
        self.assertEqual(r.json()["intervals"][0]["end"],
                         "1970-01-01T00:16:40.000Z")

    # ---------- 校准检测 ----------

    def test_10_analyze_applies_calibration_before_detection(self):
        """原始温度全部达标，校准后越界：异常与峰值必须使用校准值。"""
        r = self._analyze(make_batch("C-001"))
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        body = r.json()
        self.assertEqual(body["profile_id"], "CAL-1")
        self.assertEqual(body["anomaly_count"], 1)
        a = body["anomalies"][0]
        self.assertEqual(a["start"], iso_out(60))
        self.assertEqual(a["end"], iso_out(120))
        # 峰值是校准值 4.7*2-1=8.4，而非原始值 4.7
        self.assertAlmostEqual(a["peak_temperature"], 8.4, places=3)
        self.assertEqual(a["peak_timestamp"], iso_out(120))
        self.assertEqual(a["direction"], "high")

        # 同一批采样不指定档案时没有任何异常（证明检测吃的是校准值）
        plain = make_batch("C-001-PLAIN")
        del plain["profile_id"]
        r2 = self._analyze(plain)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(r2.json()["anomaly_count"], 0)
        self.assertIsNone(r2.json()["profile_id"])

    def test_11_calibration_can_remove_excursion(self):
        """原始温度越界，校准（减半）后回到限内：不产生异常。"""
        payload = make_batch("C-002", profile_id="CAL-2")
        payload["samples"] = [
            {"timestamp": iso(0), "temperature": 9.0},    # 校准后 4.5
            {"timestamp": iso(60), "temperature": 10.0},  # 校准后 5.0
        ]
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["anomaly_count"], 0)

    def test_12_interval_selected_by_sample_timestamp(self):
        """同一原始值在不同时刻落入不同区间，校准结果不同。"""
        payload = make_batch("C-003")
        payload["samples"] = [
            {"timestamp": iso(500), "temperature": 6.0},   # 区间1：6*2-1=11 越界
            {"timestamp": iso(1500), "temperature": 6.0},  # 区间2：恒等 6 正常
        ]
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["anomaly_count"], 1)
        a = body["anomalies"][0]
        self.assertEqual(a["start"], iso_out(500))
        self.assertAlmostEqual(a["peak_temperature"], 11.0, places=3)

    def test_13_interval_boundary_is_half_open(self):
        """边界时刻属于后一个区间；末段右端点无覆盖。"""
        payload = make_batch("C-004")
        payload["samples"] = [
            {"timestamp": iso(999), "temperature": 9.0},   # 区间1：9*2-1=17
            {"timestamp": iso(1000), "temperature": 9.0},  # 区间2：恒等 9
        ]
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 200, r.text)
        a = r.json()["anomalies"][0]
        # 峰值 17 证明 t=999 走区间1；t=1000 若也走区间1 峰值会同样是 17
        # 但 peak_timestamp 必须是 999（t=1000 校准后只有 9，偏离更小）
        self.assertAlmostEqual(a["peak_temperature"], 17.0, places=3)
        self.assertEqual(a["peak_timestamp"], iso_out(999))

        # 末段右端点（t=2000）不在任何区间 → 422 且整批不写入
        payload = make_batch("C-005")
        payload["samples"] = [
            {"timestamp": iso(1500), "temperature": 5.0},
            {"timestamp": iso(2000), "temperature": 5.0},
        ]
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 422)
        self.assertEqual(self._batch_row_count("C-005"), (0, 0))

    # ---------- 覆盖/归属失败：整批不得写入 ----------

    def test_14_uncovered_sample_rejects_whole_batch(self):
        payload = make_batch("C-006")
        payload["samples"] = [
            {"timestamp": iso(0), "temperature": 3.0},
            {"timestamp": iso(2500), "temperature": 5.0},  # 超出档案末段
        ]
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 422)
        self.assertIn("profile_id=CAL-1", r.json()["detail"])
        # 整批回滚：批次与异常都不落库，回放 404
        self.assertEqual(self._batch_row_count("C-006"), (0, 0))
        self.assertEqual(
            self.client.get("/api/v1/batches/C-006").status_code, 404
        )

    def test_15_sample_outside_segments_still_requires_coverage(self):
        """即使采样落在所有区段之外（本不参与检测），无校准覆盖同样整批拒绝。"""
        payload = make_batch("C-007")
        payload["samples"] = [
            {"timestamp": iso(0), "temperature": 3.0},
            {"timestamp": iso(5000), "temperature": 5.0},  # 区段外 + 档案外
        ]
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 422)
        self.assertEqual(self._batch_row_count("C-007"), (0, 0))

    def test_16_unknown_profile_404_and_nothing_persisted(self):
        payload = make_batch("C-008", profile_id="CAL-NOPE")
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 404)
        self.assertIn("CAL-NOPE", r.json()["detail"])
        self.assertEqual(self._batch_row_count("C-008"), (0, 0))

    def test_17_profile_device_mismatch_409_and_nothing_persisted(self):
        # CAL-OTHER 属于 reefer-02，却用于 reefer-01 的批次
        payload = make_batch("C-009", profile_id="CAL-OTHER")
        r = self._analyze(payload)
        self.assertEqual(r.status_code, 409)
        self.assertIn("CAL-OTHER", r.json()["detail"])
        self.assertEqual(self._batch_row_count("C-009"), (0, 0))

    # ---------- 幂等、哈希与摘要绑定 ----------

    def test_18_profiled_batch_idempotent_replay(self):
        payload = make_batch("C-010")
        r1 = self._analyze(payload)
        self.assertEqual(r1.status_code, 200)
        r2 = self._analyze(payload)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        self.assertEqual(r1.json()["anomalies"], r2.json()["anomalies"])
        self.assertEqual(
            r1.json()["analysis_digest"], r2.json()["analysis_digest"]
        )
        # GET 回放同样重算校准，结果与摘要一致
        r3 = self.client.get("/api/v1/batches/C-010")
        self.assertEqual(r3.status_code, 200)
        self.assertEqual(r3.json()["profile_id"], "CAL-1")
        self.assertEqual(r3.json()["anomalies"], r1.json()["anomalies"])
        self.assertEqual(
            r3.json()["analysis_digest"], r1.json()["analysis_digest"]
        )
        # 同号换内容仍 409
        conflict = make_batch("C-010")
        conflict["samples"] = [{"timestamp": iso(0), "temperature": 1.0}]
        self.assertEqual(self._analyze(conflict).status_code, 409)

    def test_19_profile_binds_request_hash(self):
        """档案是请求内容的一部分：同号批次有无档案、换档案都视为换内容。"""
        with_profile = make_batch("C-011")
        self.assertEqual(self._analyze(with_profile).status_code, 200)
        # 同号去掉 profile_id → 409
        plain = make_batch("C-011")
        del plain["profile_id"]
        self.assertEqual(self._analyze(plain).status_code, 409)
        # 同号换另一个档案 → 409
        other = make_batch("C-011", profile_id="CAL-2")
        self.assertEqual(self._analyze(other).status_code, 409)

        # 反向：先无档案入库，再带档案同号提交 → 409
        plain_first = make_batch("C-012")
        del plain_first["profile_id"]
        self.assertEqual(self._analyze(plain_first).status_code, 200)
        self.assertEqual(self._analyze(make_batch("C-012")).status_code, 409)

    def test_20_digest_binds_profile_content(self):
        """相同采样与区段：有无档案、档案内容不同，analysis_digest 都不同。"""
        d_with = self._analyze(make_batch("C-013")).json()["analysis_digest"]
        plain = make_batch("C-014")
        del plain["profile_id"]
        d_plain = self._analyze(plain).json()["analysis_digest"]
        d_other = self._analyze(
            make_batch("C-015", profile_id="CAL-2")
        ).json()["analysis_digest"]
        self.assertNotEqual(d_with, d_plain)
        self.assertNotEqual(d_with, d_other)
        self.assertNotEqual(d_plain, d_other)

        # 摘要确实按「异常 + 档案内容」规范 JSON 计算
        from app.main import _canonical_json
        body = self._analyze(make_batch("C-016")).json()
        anomalies_internal = [
            {
                "start": 60.0, "end": 120.0,
                "peak_temperature": 8.4, "peak_timestamp": 120.0,
                "direction": "high",
                "peak_segment_id": "seg-1",
                "attributed_segment_id": "seg-1",
                "attributed_segment_type": "transport",
                "duration_seconds": 60.0,
                "segment_coverage_seconds": {"seg-1": 60.0},
                "segment_sample_counts": {"seg-1": 2},
            }
        ]
        calibration = {
            "profile_id": "CAL-1",
            "device_id": "reefer-01",
            "intervals": [
                {"start": 0.0, "end": 1000.0, "scale": 2.0, "offset": -1.0},
                {"start": 1000.0, "end": 2000.0, "scale": 1.0, "offset": 0.0},
            ],
        }
        import hashlib

        expected = hashlib.sha256(
            _canonical_json(
                {"anomalies": anomalies_internal, "calibration": calibration}
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(body["analysis_digest"], expected)

    def test_21_legacy_batches_keep_legacy_digest(self):
        """未指定档案的批次：摘要口径与历史版本一致（仅异常规范 JSON）。"""
        import hashlib

        from app.main import _canonical_anomaly_json, _run_detection
        from app.models import AnalysisRequest

        payload = make_batch("C-017")
        del payload["profile_id"]
        body = self._analyze(payload).json()
        fresh = _run_detection(AnalysisRequest.model_validate(payload))
        expected = hashlib.sha256(
            _canonical_anomaly_json(fresh).encode("utf-8")
        ).hexdigest()
        self.assertEqual(body["analysis_digest"], expected)
        self.assertIsNone(body["profile_id"])

    # ---------- 复核与联单：同一校准口径 ----------

    def test_22_review_and_shipment_with_calibrated_batch(self):
        """校准批次走完整闭环：分析 → 处置复核 → 联单核对，摘要全程一致。"""
        digest = self._analyze(make_batch("C-018")).json()["analysis_digest"]

        # 过期摘要 → 409，不建案件
        r = self.client.post("/api/v1/cases/review", json={
            "batch_id": "C-018",
            "case_id": "CASE-C18-STALE",
            "analysis_digest": "0" * 64,
            "decisions": [
                {"seq": 0, "decision": "resolved",
                 "handled_at": iso(200), "note": "已处理"},
            ],
        })
        self.assertEqual(r.status_code, 409)

        # 正确摘要 → 200 closed
        r = self.client.post("/api/v1/cases/review", json={
            "batch_id": "C-018",
            "case_id": "CASE-C18",
            "analysis_digest": digest,
            "decisions": [
                {"seq": 0, "decision": "resolved",
                 "handled_at": iso(200), "note": "已复冷"},
            ],
        })
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "closed")

        # 联单核对同一摘要 → released
        r = self.client.post("/api/v1/shipments/snapshot", json={
            "shipment_id": "SHP-C18",
            "batches": [
                {"batch_id": "C-018", "analysis_digest": digest,
                 "case_id": "CASE-C18", "handoff_at": iso(10000)},
            ],
        })
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["result"], "released")
        self.assertEqual(r.json()["batches"][0]["anomaly_count"], 1)
        self.assertEqual(r.json()["batches"][0]["analysis_digest"], digest)

    def test_23_profile_persisted_immutably(self):
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                """SELECT device_id, profile_json FROM calibration_profiles
                   WHERE profile_id = 'CAL-1'"""
            ).fetchone()
        self.assertEqual(row[0], "reefer-01")
        content = json.loads(row[1])
        self.assertEqual(content["profile_id"], "CAL-1")
        self.assertEqual(
            content["intervals"][0],
            {"start": 0.0, "end": 1000.0, "scale": 2.0, "offset": -1.0},
        )


if __name__ == "__main__":
    unittest.main()
