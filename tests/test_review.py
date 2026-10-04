"""处置复核 API 测试：快照绑定、409 过期/换内容、覆盖校验、状态与幂等。"""

import hashlib
import json
import os
import sqlite3
import tempfile
import time
import unittest

_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="coldchain-review-"), "review.db")
os.environ["COLDCHAIN_DB"] = _TMP_DB

from fastapi.testclient import TestClient  # noqa: E402

from app import db  # noqa: E402
from app.main import (  # noqa: E402
    _canonical_anomaly_json,
    app,
)


def iso(epoch_seconds: int) -> str:
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def make_payload(batch_id="R-001", device_id="reefer-01"):
    """两条异常：0–240（装货段）、1200–1260（运输段）。"""
    return {
        "batch_id": batch_id,
        "device_id": device_id,
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


def make_normal_payload(batch_id="R-ZERO"):
    """无异常的批次：用于验证空异常列表也能闭环处置。"""
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


def decisions_closed():
    return [
        {"seq": 0, "decision": "resolved",
         "handled_at": iso(300), "note": "现场已复冷"},
        {"seq": 1, "decision": "dismissed",
         "handled_at": iso(1300), "note": "传感器故障，误报"},
    ]


def review_body(case_id, batch_id, digest, decisions):
    return {
        "batch_id": batch_id,
        "case_id": case_id,
        "analysis_digest": digest,
        "decisions": decisions,
    }


class ReviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 与 test_api 并行运行时库路径可能已被先导入的模块固定，这里显式指向
        # 本模块独立库，保证用例隔离。
        db._DB_PATH = _TMP_DB
        db.init_db()
        cls.client = TestClient(app)
        r = cls.client.post(
            "/api/v1/batches/analyze", json=make_payload()
        )
        assert r.status_code == 200, r.text
        cls.digest = r.json()["analysis_digest"]
        cls.anomalies = r.json()["anomalies"]

    def _case_count(self):
        with sqlite3.connect(_TMP_DB) as conn:
            return conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0]

    def test_01_analysis_carries_seq_and_digest(self):
        body = self.client.post(
            "/api/v1/batches/analyze", json=make_payload()
        ).json()
        self.assertEqual([a["seq"] for a in body["anomalies"]], [0, 1])
        # digest = 重算后异常规范 JSON 的 SHA-256
        from app.main import _run_detection
        from app.models import AnalysisRequest

        fresh = _run_detection(AnalysisRequest.model_validate(make_payload()))
        expected = hashlib.sha256(
            _canonical_anomaly_json(fresh).encode("utf-8")
        ).hexdigest()
        self.assertEqual(body["analysis_digest"], expected)
        self.assertEqual(len(body["analysis_digest"]), 64)

    def test_02_digest_stable_across_post_and_get(self):
        r_post = self.client.post(
            "/api/v1/batches/analyze", json=make_payload()
        ).json()
        r_get = self.client.get("/api/v1/batches/R-001").json()
        self.assertEqual(r_post["analysis_digest"], r_get["analysis_digest"])
        self.assertEqual(r_post["anomalies"], r_get["anomalies"])

    def test_03_review_all_resolved_or_dismissed_closes_case(self):
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-CLOSED", "R-001", self.digest,
                             decisions_closed()),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.headers.get("X-Cache"), "MISS")
        body = r.json()
        self.assertFalse(body["cached"])
        self.assertEqual(body["status"], "closed")
        self.assertEqual(body["case_id"], "CASE-CLOSED")
        self.assertEqual(body["batch_id"], "R-001")
        self.assertEqual(body["analysis_digest"], self.digest)
        self.assertEqual([d["seq"] for d in body["decisions"]], [0, 1])
        self.assertEqual(
            [d["decision"] for d in body["decisions"]],
            ["resolved", "dismissed"],
        )
        self.assertEqual(body["decisions"][0]["handled_at"],
                         iso(300).replace("Z", ".000Z"))
        self.assertEqual(body["decisions"][1]["note"], "传感器故障，误报")

    def test_04_any_accepted_keeps_case_open(self):
        decisions = [
            {"seq": 0, "decision": "accepted", "handled_at": iso(300)},
            {"seq": 1, "decision": "resolved",
             "handled_at": iso(1300), "note": "已处理"},
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-OPEN", "R-001", self.digest, decisions),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "open")
        # accepted 允许不带说明
        self.assertIsNone(r.json()["decisions"][0]["note"])

    def test_05_same_case_same_content_is_idempotent(self):
        body = review_body("CASE-IDEM", "R-001", self.digest,
                           decisions_closed())
        r1 = self.client.post("/api/v1/cases/review", json=body)
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r1.headers.get("X-Cache"), "MISS")
        rows_before = self._case_count()

        r2 = self.client.post("/api/v1/cases/review", json=body)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertTrue(r2.json()["cached"])
        self.assertEqual(self._case_count(), rows_before)

        # 除 cached 标记外回放内容完全一致
        j1, j2 = r1.json(), r2.json()
        self.assertFalse(j1.pop("cached"))
        self.assertTrue(j2.pop("cached"))
        self.assertEqual(j1, j2)

    def test_06_idempotency_is_order_insensitive_for_decisions(self):
        body = review_body("CASE-ORDER", "R-001", self.digest,
                           decisions_closed())
        r1 = self.client.post("/api/v1/cases/review", json=body)
        self.assertEqual(r1.status_code, 200)
        reordered = dict(body)
        reordered["decisions"] = list(reversed(body["decisions"]))
        r2 = self.client.post("/api/v1/cases/review", json=reordered)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.headers.get("X-Cache"), "HIT")
        self.assertEqual(
            [d["seq"] for d in r2.json()["decisions"]], [0, 1]
        )

    def test_07_same_case_changed_content_conflicts(self):
        body = review_body("CASE-CHG", "R-001", self.digest,
                           decisions_closed())
        self.assertEqual(
            self.client.post("/api/v1/cases/review", json=body).status_code,
            200,
        )
        # 改说明
        changed = json.loads(json.dumps(body))
        changed["decisions"][0]["note"] = "另一个说法"
        r = self.client.post("/api/v1/cases/review", json=changed)
        self.assertEqual(r.status_code, 409)
        # 改结论
        changed = json.loads(json.dumps(body))
        changed["decisions"][0]["decision"] = "accepted"
        self.assertEqual(
            self.client.post("/api/v1/cases/review", json=changed).status_code,
            409,
        )
        # 改处置时间
        changed = json.loads(json.dumps(body))
        changed["decisions"][0]["handled_at"] = iso(301)
        self.assertEqual(
            self.client.post("/api/v1/cases/review", json=changed).status_code,
            409,
        )
        # 换摘要（同样视为换内容）
        changed = json.loads(json.dumps(body))
        changed["analysis_digest"] = "0" * 64
        self.assertEqual(
            self.client.post("/api/v1/cases/review", json=changed).status_code,
            409,
        )
        # 换批次
        changed = json.loads(json.dumps(body))
        changed["batch_id"] = "R-OTHER"
        self.assertEqual(
            self.client.post("/api/v1/cases/review", json=changed).status_code,
            404,  # 新批次不存在 → 404；存在但案件绑定不同批次则 409
        )

    def test_08_stale_digest_conflicts_and_persists_nothing(self):
        cases_before = self._case_count()
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-STALE", "R-001", "0" * 64,
                             decisions_closed()),
        )
        self.assertEqual(r.status_code, 409)
        self.assertEqual(self._case_count(), cases_before)
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM cases WHERE case_id = 'CASE-STALE'"
                ).fetchone()
            )

    def test_09_decisions_must_cover_all_anomalies(self):
        # 缺序号 1
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-MISS", "R-001", self.digest,
                             [decisions_closed()[0]]),
        )
        self.assertEqual(r.status_code, 422)
        # 越界序号 2
        extra = decisions_closed() + [
            {"seq": 2, "decision": "resolved", "handled_at": iso(9999)}
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-EXTRA", "R-001", self.digest, extra),
        )
        self.assertEqual(r.status_code, 422)
        # 空列表对两条异常同样不通过
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-EMPTY", "R-001", self.digest, []),
        )
        self.assertEqual(r.status_code, 422)

    def test_10_duplicate_seq_rejected(self):
        dup = [decisions_closed()[0], decisions_closed()[0]]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-DUP", "R-001", self.digest, dup),
        )
        self.assertEqual(r.status_code, 422)

    def test_11_dismissed_requires_note(self):
        bad = [
            {"seq": 0, "decision": "dismissed", "handled_at": iso(300)},
            decisions_closed()[1],
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-NONOTE", "R-001", self.digest, bad),
        )
        self.assertEqual(r.status_code, 422)
        # 纯空白说明也不算有效说明
        bad[0]["note"] = "   "
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-BLANKNOTE", "R-001", self.digest, bad),
        )
        self.assertEqual(r.status_code, 422)
        # resolved / accepted 无说明是允许的
        ok = [
            {"seq": 0, "decision": "resolved", "handled_at": iso(300)},
            {"seq": 1, "decision": "accepted", "handled_at": iso(1300)},
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-NONOTE-OK", "R-001", self.digest, ok),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "open")

    def test_12_handled_at_not_before_anomaly_end(self):
        bad = [
            {"seq": 0, "decision": "resolved", "handled_at": iso(239)},
            decisions_closed()[1],
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-EARLY", "R-001", self.digest, bad),
        )
        self.assertEqual(r.status_code, 422)
        # 恰好等于异常 end 允许
        boundary = [
            {"seq": 0, "decision": "resolved", "handled_at": iso(240)},
            decisions_closed()[1],
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-BOUNDARY", "R-001", self.digest, boundary),
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "closed")

    def test_13_unknown_batch_404(self):
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-X", "R-NOPE", self.digest,
                             decisions_closed()),
        )
        self.assertEqual(r.status_code, 404)

    def test_14_zero_anomalies_closes_with_empty_decisions(self):
        r = self.client.post(
            "/api/v1/batches/analyze", json=make_normal_payload()
        )
        self.assertEqual(r.status_code, 200)
        digest = r.json()["analysis_digest"]
        self.assertEqual(r.json()["anomaly_count"], 0)
        # 空列表的规范 JSON 为 []，其摘要固定
        self.assertEqual(
            digest, hashlib.sha256(b"[]").hexdigest()
        )
        rr = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-ZERO", "R-ZERO", digest, []),
        )
        self.assertEqual(rr.status_code, 200, rr.text)
        self.assertEqual(rr.json()["status"], "closed")
        self.assertEqual(rr.json()["decisions"], [])

    def test_15_recompute_happens_inside_review_transaction(self):
        """库存异常行与当前口径不符：复核事务内先重算，再比对摘要。

        拿库存旧行 JSON 算出的摘要提交应 409（且不建案件、不保留替换）；
        拿重算摘要提交应 200，且异常行已在同一事务中被修正为 seg-1 归因。
        """
        legacy_id = "R-LEGACY-REVIEW"
        self._seed_legacy_batch(legacy_id)
        with sqlite3.connect(_TMP_DB) as conn:
            legacy_json = json.dumps(
                [{
                    "start": 990, "end": 1100, "peak_temperature": 9.0,
                    "peak_timestamp": 990, "direction": "high",
                    "peak_segment_id": "seg-1",
                    "attributed_segment_id": "seg-2",
                    "attributed_segment_type": "transport",
                    "duration_seconds": 110.0,
                    "segment_coverage_seconds": {"seg-2": 100.0, "seg-1": 10.0},
                    "segment_sample_counts": {},
                }],
                sort_keys=True, ensure_ascii=False, separators=(",", ":"),
            ).encode("utf-8")
            legacy_digest = hashlib.sha256(legacy_json).hexdigest()

        decisions = [
            {"seq": 0, "decision": "resolved",
             "handled_at": iso(1100), "note": "已确认"},
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-LEGACY-OLD", legacy_id, legacy_digest,
                             decisions),
        )
        self.assertEqual(r.status_code, 409)
        # 409 路径不能创建案件
        with sqlite3.connect(_TMP_DB) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM cases WHERE case_id = 'CASE-LEGACY-OLD'"
                ).fetchone()
            )
            # 事务回滚：库存旧行未被替换（仍是误归因 seg-2）
            row = conn.execute(
                """SELECT attributed_segment_id FROM anomalies
                   WHERE batch_id = ? AND seq = 0""",
                (legacy_id,),
            ).fetchone()
        self.assertEqual(row[0], "seg-2")

        # 取当前重算摘要后提交：200，且库内异常归因已在同一事务内修正
        current = self.client.get(f"/api/v1/batches/{legacy_id}").json()
        self.assertEqual(
            current["anomalies"][0]["attributed_segment_id"], "seg-1"
        )
        r2 = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-LEGACY-NEW", legacy_id,
                             current["analysis_digest"], decisions),
        )
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(r2.json()["status"], "closed")
        with sqlite3.connect(_TMP_DB) as conn:
            row = conn.execute(
                """SELECT a.attributed_segment_id, a.sample_counts_json
                   FROM anomalies a WHERE a.batch_id = ?""",
                (legacy_id,),
            ).fetchone()
        self.assertEqual(row[0], "seg-1")
        self.assertEqual(json.loads(row[1]), {"seg-1": 3, "seg-2": 1})

    def test_16_status_persisted_in_cases_table(self):
        with sqlite3.connect(_TMP_DB) as conn:
            closed = conn.execute(
                "SELECT status FROM cases WHERE case_id = 'CASE-CLOSED'"
            ).fetchone()[0]
            open_ = conn.execute(
                "SELECT status FROM cases WHERE case_id = 'CASE-OPEN'"
            ).fetchone()[0]
            count = conn.execute(
                "SELECT COUNT(*) FROM case_decisions WHERE case_id = 'CASE-CLOSED'"
            ).fetchone()[0]
        self.assertEqual(closed, "closed")
        self.assertEqual(open_, "open")
        self.assertEqual(count, 2)

    def test_17_invalid_decision_value_422(self):
        bad = [
            {"seq": 0, "decision": "wontfix", "handled_at": iso(300)},
            decisions_closed()[1],
        ]
        r = self.client.post(
            "/api/v1/cases/review",
            json=review_body("CASE-BADVAL", "R-001", self.digest, bad),
        )
        self.assertEqual(r.status_code, 422)

    def _seed_legacy_batch(self, batch_id):
        """写一批旧口径数据：异常行误归因 seg-2、sample_counts_json 为 NULL。"""
        payload = {
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
                {"segment_id": "seg-1", "segment_type": "loading",
                 "start": iso(0), "end": iso(1000),
                 "lower_limit": 0.0, "upper_limit": 8.0},
                {"segment_id": "seg-2", "segment_type": "transport",
                 "start": iso(1000), "end": iso(1200),
                 "lower_limit": 0.0, "upper_limit": 8.0},
            ],
        }
        from app.main import _canonical_hash
        from app.models import AnalysisRequest

        request_hash = _canonical_hash(
            AnalysisRequest.model_validate(payload)
        )
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
            conn.execute(
                """INSERT INTO anomalies
                   (batch_id, seq, start_ts, end_ts, peak_temperature, peak_ts,
                    direction, peak_segment_id, attributed_segment_id,
                    attributed_segment_type, duration_seconds, coverage_json,
                    sample_counts_json)
                   VALUES (?, 0, 990, 1100, 9.0, 990, 'high', 'seg-1', 'seg-2',
                           'transport', 110.0,
                           '{"seg-2": 100.0, "seg-1": 10.0}', '{}')""",
                (batch_id,),
            )


if __name__ == "__main__":
    unittest.main()
