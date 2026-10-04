"""运输联单风险快照 API 测试：逐批次校验、事务原子性、released/held、幂等。"""

import json
import os
import sqlite3
import tempfile
import time
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-ship-"), "shipment.db")
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


def make_two_anomaly_payload(batch_id):
    """两条异常：0–240（装货段）、1200–1260（运输段）。"""
    return {
        "batch_id": batch_id,
        "device_id": "reefer-01",
        "gap_seconds": 600,
        "dip_tolerance_seconds": 120,
        "samples": [
            {"timestamp": iso(0), "temperature": 9.0},
            {"timestamp": iso(60), "temperature": 9.5},
            {"timestamp": iso(120), "temperature": 5.0},
            {"timestamp": iso(180), "temperature": 10.0},
            {"timestamp": iso(240), "temperature": 11.0},
            {"timestamp": iso(1200), "temperature": 9.0},
            {"timestamp": iso(1260), "temperature": 9.0},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
            {"segment_id": "seg-2", "segment_type": "transport",
             "start": iso(600), "end": iso(1800),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


def make_normal_payload(batch_id):
    """无异常批次：空 decisions 即可闭环处置。"""
    return {
        "batch_id": batch_id,
        "device_id": "reefer-01",
        "samples": [
            {"timestamp": iso(0), "temperature": 5.0},
            {"timestamp": iso(60), "temperature": 6.0},
        ],
        "segments": [
            {"segment_id": "seg-1", "segment_type": "loading",
             "start": iso(0), "end": iso(600),
             "lower_limit": 0.0, "upper_limit": 8.0},
        ],
    }


class ShipmentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 与其他测试模块同进程运行时库路径可能已被固定，显式指向本模块独立库。
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)

        # 批次 S-A：2 条异常，案件 closed（resolved + dismissed）
        cls.digest_a = cls._analyze(make_two_anomaly_payload("S-A"))
        cls._review("CASE-A", "S-A", cls.digest_a, [
            {"seq": 0, "decision": "resolved",
             "handled_at": iso(300), "note": "已复冷"},
            {"seq": 1, "decision": "dismissed",
             "handled_at": iso(1300), "note": "传感器误报"},
        ], expected_status="closed")

        # 批次 S-B：2 条异常，案件 open（含 accepted）
        cls.digest_b = cls._analyze(make_two_anomaly_payload("S-B"))
        cls._review("CASE-B", "S-B", cls.digest_b, [
            {"seq": 0, "decision": "accepted", "handled_at": iso(300)},
            {"seq": 1, "decision": "resolved",
             "handled_at": iso(1300), "note": "已处理"},
        ], expected_status="open")

        # 批次 S-C：无异常，案件 closed（空处置）
        cls.digest_c = cls._analyze(make_normal_payload("S-C"))
        cls._review("CASE-C", "S-C", cls.digest_c, [],
                    expected_status="closed")

    @classmethod
    def _analyze(cls, payload):
        r = cls.client.post("/api/v1/batches/analyze", json=payload)
        assert r.status_code == 200, r.text
        return r.json()["analysis_digest"]

    @classmethod
    def _review(cls, case_id, batch_id, digest, decisions,
                expected_status=None):
        r = cls.client.post("/api/v1/cases/review", json={
            "batch_id": batch_id,
            "case_id": case_id,
            "analysis_digest": digest,
            "decisions": decisions,
        })
        assert r.status_code == 200, r.text
        if expected_status is not None:
            assert r.json()["status"] == expected_status, r.text

    # ---------- 工具 ----------

    def _shipment_counts(self):
        with sqlite3.connect(_TMP_DB) as conn:
            shipments = conn.execute(
                "SELECT COUNT(*) FROM shipments").fetchone()[0]
            rows = conn.execute(
                "SELECT COUNT(*) FROM shipment_batches").fetchone()[0]
        return shipments, rows

    @staticmethod
    def _item(batch_id, digest, case_id, handoff_at):
        return {
            "batch_id": batch_id,
            "analysis_digest": digest,
            "case_id": case_id,
            "handoff_at": handoff_at,
        }

    def _waybill_body(self, shipment_id="SHP-HELD"):
        return {
            "shipment_id": shipment_id,
            "batches": [
                self._item("S-A", self.digest_a, "CASE-A", iso(10000)),
                self._item("S-B", self.digest_b, "CASE-B", iso(20000)),
            ],
        }

    # ---------- 用例 ----------

    def test_01_held_when_any_case_open(self):
        r = self.client.post("/api/v1/shipments/snapshot",
                             json=self._waybill_body())
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        body = r.json()
        self.assertFalse(body["cached"])
        self.assertEqual(body["shipment_id"], "SHP-HELD")
        # CASE-B 为 open → 总体 held
        self.assertEqual(body["result"], "held")
        self.assertEqual([b["seq"] for b in body["batches"]], [0, 1])
        self.assertEqual(
            [b["batch_id"] for b in body["batches"]], ["S-A", "S-B"]
        )
        self.assertEqual(
            [b["anomaly_count"] for b in body["batches"]], [2, 2]
        )
        self.assertEqual(
            [b["case_status"] for b in body["batches"]], ["closed", "open"]
        )
        self.assertEqual(body["batches"][0]["case_id"], "CASE-A")
        self.assertEqual(body["batches"][0]["analysis_digest"], self.digest_a)
        self.assertEqual(body["batches"][0]["handoff_at"],
                         iso(10000).replace("Z", ".000Z"))

    def test_02_released_when_all_cases_closed(self):
        body = {
            "shipment_id": "SHP-REL",
            "batches": [
                self._item("S-A", self.digest_a, "CASE-A", iso(10000)),
                self._item("S-C", self.digest_c, "CASE-C", iso(30000)),
            ],
        }
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["result"], "released")
        self.assertEqual(
            [b["anomaly_count"] for b in r.json()["batches"]], [2, 0]
        )
        self.assertEqual(
            [b["case_status"] for b in r.json()["batches"]],
            ["closed", "closed"],
        )

    def test_03_same_shipment_same_content_is_idempotent(self):
        body = self._waybill_body("SHP-IDEM")
        r1 = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r1.headers.get("X-Cache"), "MISS")
        counts = self._shipment_counts()

        r2 = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        # 幂等回放不产生新行
        self.assertEqual(self._shipment_counts(), counts)

        j1, j2 = r1.json(), r2.json()
        self.assertFalse(j1.pop("cached"))
        self.assertTrue(j2.pop("cached"))
        self.assertEqual(j1, j2)

    def test_04_same_shipment_changed_content_conflicts(self):
        body = self._waybill_body("SHP-CHG")
        self.assertEqual(
            self.client.post("/api/v1/shipments/snapshot",
                             json=body).status_code,
            200,
        )
        # 改交接时间
        changed = json.loads(json.dumps(body))
        changed["batches"][1]["handoff_at"] = iso(20001)
        r = self.client.post("/api/v1/shipments/snapshot", json=changed)
        self.assertEqual(r.status_code, 409)
        # 换案件
        changed = json.loads(json.dumps(body))
        changed["batches"][0]["case_id"] = "CASE-C"
        self.assertEqual(
            self.client.post("/api/v1/shipments/snapshot",
                             json=changed).status_code,
            409,
        )
        # 换摘要
        changed = json.loads(json.dumps(body))
        changed["batches"][0]["analysis_digest"] = "0" * 64
        self.assertEqual(
            self.client.post("/api/v1/shipments/snapshot",
                             json=changed).status_code,
            409,
        )
        # 整体换顺序（连同交接时间，仍满足递增）也属换内容
        changed = json.loads(json.dumps(body))
        changed["batches"] = [
            self._item("S-B", self.digest_b, "CASE-B", iso(10000)),
            self._item("S-A", self.digest_a, "CASE-A", iso(20000)),
        ]
        self.assertEqual(
            self.client.post("/api/v1/shipments/snapshot",
                             json=changed).status_code,
            409,
        )

    def test_05_stale_digest_conflicts_and_persists_nothing(self):
        counts = self._shipment_counts()
        body = self._waybill_body("SHP-STALE")
        body["batches"][0]["analysis_digest"] = "0" * 64
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 409)
        # 冲突可定位：指明清单位置与批次
        self.assertIn("S-A", r.json()["detail"])
        self.assertEqual(self._shipment_counts(), counts)

    def test_06_unknown_batch_404_and_persists_nothing(self):
        counts = self._shipment_counts()
        body = self._waybill_body("SHP-NOBATCH")
        body["batches"][1] = self._item("S-NOPE", "0" * 64, "CASE-X",
                                        iso(20000))
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 404)
        self.assertIn("S-NOPE", r.json()["detail"])
        self.assertEqual(self._shipment_counts(), counts)

    def test_07_missing_case_conflicts_and_persists_nothing(self):
        counts = self._shipment_counts()
        body = self._waybill_body("SHP-NOCASE")
        body["batches"][1]["case_id"] = "CASE-NOPE"
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 409)
        self.assertIn("CASE-NOPE", r.json()["detail"])
        self.assertEqual(self._shipment_counts(), counts)

    def test_08_case_belonging_to_other_batch_conflicts(self):
        counts = self._shipment_counts()
        # CASE-A 属于 S-A，却挂在 S-B 上
        body = self._waybill_body("SHP-XCASE")
        body["batches"][1]["case_id"] = "CASE-A"
        body["batches"][1]["analysis_digest"] = self.digest_b
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 409)
        self.assertIn("CASE-A", r.json()["detail"])
        self.assertEqual(self._shipment_counts(), counts)

    def test_09_case_on_stale_snapshot_conflicts(self):
        """案件处置依据的摘要与当前重算摘要不一致 → 409。"""
        counts = self._shipment_counts()
        # 正常建案后直接把库里的案件摘要改坏，模拟过期处置
        digest = self._analyze(make_two_anomaly_payload("S-D"))
        self._review("CASE-D", "S-D", digest, [
            {"seq": 0, "decision": "resolved", "handled_at": iso(300)},
            {"seq": 1, "decision": "resolved", "handled_at": iso(1300)},
        ])
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                "UPDATE cases SET analysis_digest = ? WHERE case_id = ?",
                ("f" * 64, "CASE-D"),
            )
        body = {
            "shipment_id": "SHP-CASESTALE",
            "batches": [self._item("S-D", digest, "CASE-D", iso(10000))],
        }
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 409)
        self.assertIn("CASE-D", r.json()["detail"])
        self.assertEqual(self._shipment_counts(), counts)

    def test_10_case_not_covering_current_anomalies_conflicts(self):
        """案件处置行未覆盖全部当前异常 → 409（直接播种残缺案件）。"""
        counts = self._shipment_counts()
        digest = self._analyze(make_two_anomaly_payload("S-E"))
        with sqlite3.connect(_TMP_DB) as conn:
            conn.execute(
                """INSERT INTO cases
                   (case_id, batch_id, analysis_digest, content_hash,
                    status, created_at)
                   VALUES (?, ?, ?, ?, 'closed', ?)""",
                ("CASE-E", "S-E", digest, "seed", time.time()),
            )
            # 只有 seq 0 的处置，缺 seq 1
            conn.execute(
                """INSERT INTO case_decisions
                   (case_id, seq, decision, handled_at, note)
                   VALUES ('CASE-E', 0, 'resolved', 300, NULL)""",
            )
        body = {
            "shipment_id": "SHP-PARTIAL",
            "batches": [self._item("S-E", digest, "CASE-E", iso(10000))],
        }
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 409)
        self.assertIn("CASE-E", r.json()["detail"])
        self.assertEqual(self._shipment_counts(), counts)

    def test_11_duplicate_batch_rejected(self):
        body = self._waybill_body("SHP-DUP")
        body["batches"][1] = self._item("S-A", self.digest_a, "CASE-A",
                                        iso(20000))
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 422)

    def test_12_handoff_must_be_strictly_increasing(self):
        # 相等不允许
        body = self._waybill_body("SHP-EQ")
        body["batches"][1]["handoff_at"] = iso(10000)
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 422)
        # 倒退不允许
        body = self._waybill_body("SHP-BACK")
        body["batches"][1]["handoff_at"] = iso(9999)
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 422)

    def test_13_empty_batch_list_rejected(self):
        r = self.client.post("/api/v1/shipments/snapshot",
                             json={"shipment_id": "SHP-EMPTY",
                                   "batches": []})
        self.assertEqual(r.status_code, 422)

    def test_14_single_batch_waybill(self):
        body = {
            "shipment_id": "SHP-SINGLE",
            "batches": [self._item("S-C", self.digest_c, "CASE-C",
                                   iso(5000))],
        }
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["result"], "released")
        self.assertEqual(len(r.json()["batches"]), 1)
        self.assertEqual(r.json()["batches"][0]["anomaly_count"], 0)

    def test_15_shipment_persisted_immutably(self):
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                "SELECT result FROM shipments WHERE shipment_id = 'SHP-HELD'"
            ).fetchone()
            self.assertEqual(row[0], "held")
            rows = conn.execute(
                """SELECT seq, batch_id, case_id, anomaly_count, case_status
                   FROM shipment_batches
                   WHERE shipment_id = 'SHP-HELD' ORDER BY seq"""
            ).fetchall()
        self.assertEqual(
            [tuple(r) for r in rows],
            [
                (0, "S-A", "CASE-A", 2, "closed"),
                (1, "S-B", "CASE-B", 2, "open"),
            ],
        )

    def test_16_naive_handoff_timestamp_treated_as_utc(self):
        body = {
            "shipment_id": "SHP-NAIVE",
            "batches": [
                self._item("S-A", self.digest_a, "CASE-A",
                           "2026-09-22T08:00:00"),
                self._item("S-C", self.digest_c, "CASE-C",
                           "2026-09-22T09:00:00"),
            ],
        }
        r = self.client.post("/api/v1/shipments/snapshot", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["batches"][0]["handoff_at"],
                         "2026-09-22T08:00:00.000Z")


if __name__ == "__main__":
    unittest.main()
