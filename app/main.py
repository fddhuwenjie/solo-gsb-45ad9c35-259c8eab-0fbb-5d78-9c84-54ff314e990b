"""FastAPI 入口：冷链温区异常归因 API。"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse

from . import db
from .detection import (
    Anomaly,
    CalibrationCoverageError,
    CalibrationInterval,
    Sample,
    Segment,
    correct_samples,
    detect_anomalies,
)
from .models import (
    AnalysisRequest,
    AnalysisResponse,
    AnomalyOut,
    CalibrationIntervalOut,
    CalibrationProfileRequest,
    CalibrationProfileResponse,
    CaseReviewRequest,
    CaseReviewResponse,
    DecisionOut,
    ShipmentBatchOut,
    ShipmentSnapshotRequest,
    ShipmentSnapshotResponse,
    from_epoch,
    to_epoch,
)


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.init_db()
    yield


app = FastAPI(
    title="冷链温区异常归因 API",
    version="1.3.0",
    description=(
        "按装卸/运输区段的温度上下限检测连续越界，容忍短暂回落、"
        "跨采样缺口拆段，并按异常区间内实际采样时刻数量将异常归因到"
        "对应区段；历史回放一律依据保存的原始采样重算，修正旧口径归因。"
        "可登记设备温度校准档案（不可变、同号同内容幂等），分析时按采样"
        "时刻以 corrected=raw*scale+offset 校准后再检测，档案号与档案内容"
        "纳入请求哈希与分析摘要。处置复核绑定分析快照；运输联单在同一事务"
        "内逐批次核对快照与案件处置后落库，给出 released/held 风险结论。"
    ),
    lifespan=lifespan,
)


def _json_safe(value: Any) -> Any:
    """把校验错误详情中的非有限浮点替换为字符串，保证 422 响应可序列化。

    请求体携带 ``Infinity``/``NaN`` 时，默认处理器会把原始 inf/nan 回显到
    错误详情，Starlette 的 JSON 序列化（allow_nan=False）会因此抛错变 500。
    """
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(
    _: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=jsonable_encoder({"detail": _json_safe(exc.errors())}),
    )


# ---------- 序列化与计算 ----------


def _round3(value: float) -> float:
    return round(value, 3)


def _anomaly_to_internal(anomaly: Anomaly) -> dict[str, Any]:
    """数值型结果，用于持久化（时间戳保持 epoch 秒）。"""
    return {
        "start": anomaly.start,
        "end": anomaly.end,
        "peak_temperature": _round3(anomaly.peak_temperature),
        "peak_timestamp": anomaly.peak_timestamp,
        "direction": anomaly.direction,
        "peak_segment_id": anomaly.peak_segment_id,
        "attributed_segment_id": anomaly.attributed_segment_id,
        "attributed_segment_type": anomaly.attributed_segment_type,
        "duration_seconds": _round3(anomaly.duration_seconds),
        "segment_coverage_seconds": {
            seg_id: _round3(secs)
            for seg_id, secs in anomaly.segment_coverage_seconds.items()
        },
        "segment_sample_counts": dict(anomaly.segment_sample_counts),
    }


def _internal_to_out(seq: int, a: dict[str, Any]) -> AnomalyOut:
    """数值型记录 → 对外响应模型（时间戳转 ISO-8601 UTC）。"""
    return AnomalyOut(
        seq=seq,
        start=from_epoch(a["start"]),
        end=from_epoch(a["end"]),
        peak_temperature=a["peak_temperature"],
        peak_timestamp=from_epoch(a["peak_timestamp"]),
        direction=a["direction"],
        peak_segment_id=a["peak_segment_id"],
        attributed_segment_id=a["attributed_segment_id"],
        attributed_segment_type=a["attributed_segment_type"],
        duration_seconds=a["duration_seconds"],
        segment_coverage_seconds=a["segment_coverage_seconds"],
        segment_sample_counts=a.get("segment_sample_counts", {}),
    )


def _canonical_anomaly_json(anomalies: list[dict[str, Any]]) -> str:
    """重算后异常规范 JSON（键排序、紧凑分隔），作为确定快照的序列化口径。"""
    return json.dumps(anomalies, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def _canonical_json(payload: Any) -> str:
    """任意规范内容的确定序列化（键排序、紧凑分隔）。"""
    return json.dumps(payload, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


# ---------- 校准档案内容规范化与解析 ----------


def _calibration_content(
    profile_id: str, device_id: str, intervals: list[dict[str, Any]]
) -> dict[str, Any]:
    """档案规范内容（epoch 秒区间），请求哈希/分析摘要/持久化共用同一口径。"""
    return {
        "profile_id": profile_id,
        "device_id": device_id,
        "intervals": intervals,
    }


def _calibration_content_from_request(
    req: CalibrationProfileRequest,
) -> dict[str, Any]:
    return _calibration_content(
        req.profile_id,
        req.device_id,
        [
            {
                "start": to_epoch(iv.start),
                "end": to_epoch(iv.end),
                "scale": iv.scale,
                "offset": iv.offset,
            }
            for iv in req.intervals
        ],
    )


def _profile_digest(content: dict[str, Any]) -> str:
    """档案规范内容的 SHA-256；相同内容的档案摘要逐字节一致。"""
    return hashlib.sha256(_canonical_json(content).encode("utf-8")).hexdigest()


def _resolve_calibration_conn(conn: Any, req: AnalysisRequest) -> dict[str, Any]:
    """事务内读取请求指定的不可变校准档案并核对设备归属。

    档案不存在返回 404；档案属于其他设备返回 409（档案不匹配）。
    两种情况整批都不写入。
    """
    assert req.profile_id is not None
    row = db._select_profile(conn, req.profile_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"校准档案 profile_id={req.profile_id} 不存在，"
                "请先提交档案；整批未写入"
            ),
        )
    if row["device_id"] != req.device_id:
        raise HTTPException(
            status_code=409,
            detail=(
                f"校准档案 profile_id={req.profile_id} 属于设备 "
                f"{row['device_id']}，与请求设备 {req.device_id} 不匹配；"
                "整批未写入"
            ),
        )
    return json.loads(row["profile_json"])


def _load_calibration_conn(
    conn: Any, profile_id: str, device_id: str
) -> dict[str, Any]:
    """回放路径在事务内读取批次关联的校准档案。

    批次写入时已校验档案存在且设备匹配，档案又不可变，此处缺失只可能
    是库被外部篡改，按 500 处理。
    """
    row = db._select_profile(conn, profile_id)
    if row is None or row["device_id"] != device_id:
        raise HTTPException(
            status_code=500,
            detail=(
                f"批次关联的校准档案 profile_id={profile_id} 缺失或"
                "设备归属不一致，无法重算"
            ),
        )
    return json.loads(row["profile_json"])


def _analysis_digest(
    anomalies: list[dict[str, Any]],
    calibration: Optional[dict[str, Any]] = None,
) -> str:
    """重算后结果的 SHA-256；处置必须绑定该摘要，防快照过期。

    指定校准档案的批次把档案号与档案内容一并纳入摘要，保证回放、复核与
    联单核对绑定同一份校准依据；未指定档案的批次保持历史口径（仅异常
    规范 JSON），旧批次摘要继续有效。
    """
    if calibration is None:
        canonical = _canonical_anomaly_json(anomalies)
    else:
        canonical = _canonical_json(
            {"anomalies": anomalies, "calibration": calibration}
        )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _run_detection(
    req: AnalysisRequest, calibration: Optional[dict[str, Any]] = None
) -> list[dict[str, Any]]:
    samples = [
        Sample(timestamp=to_epoch(s.timestamp), temperature=s.temperature)
        for s in req.samples
    ]
    if calibration is not None:
        # 先按采样时刻校准（corrected=raw*scale+offset），再执行现有检测；
        # 任一采样无区间覆盖时 correct_samples 抛 CalibrationCoverageError。
        intervals = [
            CalibrationInterval(
                start=iv["start"], end=iv["end"],
                scale=iv["scale"], offset=iv["offset"],
            )
            for iv in calibration["intervals"]
        ]
        samples = correct_samples(samples, intervals)
    segments = [
        Segment(
            segment_id=s.segment_id,
            segment_type=s.segment_type,
            start=to_epoch(s.start),
            end=to_epoch(s.end),
            lower_limit=s.lower_limit,
            upper_limit=s.upper_limit,
        )
        for s in req.segments
    ]
    anomalies = detect_anomalies(
        device_id=req.device_id,
        samples=samples,
        segments=segments,
        gap_seconds=req.gap_seconds,
        dip_tolerance_seconds=req.dip_tolerance_seconds,
    )
    return [_anomaly_to_internal(a) for a in anomalies]


def _canonical_hash(
    req: AnalysisRequest, calibration: Optional[dict[str, Any]] = None
) -> str:
    """对请求体（剔除 batch_id）做规范化哈希，用于识别同号不同内容。

    指定校准档案时，档案号与档案内容一并纳入哈希；未指定档案的请求
    序列化口径与历史版本完全一致，旧批次的幂等判定不受影响。
    """
    payload = req.model_dump(mode="json", exclude={"batch_id", "profile_id"})
    if req.profile_id is not None:
        assert calibration is not None, "指定 profile_id 时必须已解析档案内容"
        payload["profile_id"] = req.profile_id
        payload["calibration"] = calibration
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _request_from_stored(stored: dict[str, Any]) -> AnalysisRequest:
    """用入库时保存的原始请求体重建 AnalysisRequest。

    回放只信任原始采样与区段，不信任历史异常行——后者可能按旧口径
    （区间重叠秒数）归因。
    """
    raw = json.loads(stored["batch"]["request_json"])
    return AnalysisRequest.model_validate(raw)


def _stored_row_signatures(stored: dict[str, Any]) -> list[str]:
    """已存异常行的规范化签名，用于判断是否需要重算覆盖。"""
    signatures: list[str] = []
    for r in stored["anomalies"]:
        counts: dict[str, Any] = {}
        if r["sample_counts_json"]:
            counts = json.loads(r["sample_counts_json"])
        signatures.append(
            json.dumps(
                {
                    "start": r["start_ts"],
                    "end": r["end_ts"],
                    "peak_temperature": _round3(r["peak_temperature"]),
                    "peak_timestamp": r["peak_ts"],
                    "direction": r["direction"],
                    "peak_segment_id": r["peak_segment_id"],
                    "attributed_segment_id": r["attributed_segment_id"],
                    "attributed_segment_type": r["attributed_segment_type"],
                    "duration_seconds": _round3(r["duration_seconds"]),
                    "segment_coverage_seconds": {
                        k: _round3(v) for k, v in json.loads(r["coverage_json"]).items()
                    },
                    "segment_sample_counts": counts,
                },
                sort_keys=True,
                ensure_ascii=False,
            )
        )
    return signatures


def _recompute_batch_conn(
    conn: Any, stored: dict[str, Any]
) -> tuple[str, list[dict[str, Any]], Optional[dict[str, Any]]]:
    """在给定事务/连接内依据原始采样重算；已存行属旧口径时原子覆盖。

    返回 (device_id, 最新口径的异常列表, 校准档案内容或 None)。重复 POST、
    按批次 GET 与处置复核共用此路径，保证旧口径写入的数据不会被直接回放；
    指定档案的批次在同一事务内读取不可变档案并校准后重算。
    """
    req = _request_from_stored(stored)
    calibration = (
        _load_calibration_conn(conn, req.profile_id, req.device_id)
        if req.profile_id is not None
        else None
    )
    fresh = _run_detection(req, calibration)

    # 与已存行比较；旧口径行（缺少 sample_counts 或归因不同）整体替换。
    stored_signatures = _stored_row_signatures(stored)
    fresh_signatures = [
        json.dumps(a, sort_keys=True, ensure_ascii=False) for a in fresh
    ]
    if stored_signatures != fresh_signatures:
        db._replace_anomalies_conn(conn, req.batch_id, fresh)

    return req.device_id, fresh, calibration


def _replay_batch(
    stored: dict[str, Any]
) -> tuple[str, list[dict[str, Any]], Optional[dict[str, Any]]]:
    """独立连接上的重算回放（POST 幂等命中与 GET 使用）。"""
    with db.transaction() as conn:
        device_id, fresh, calibration = _recompute_batch_conn(conn, stored)
    return device_id, fresh, calibration


def _build_response(
    batch_id: str,
    device_id: str,
    anomalies: list[dict[str, Any]],
    cached: bool,
    calibration: Optional[dict[str, Any]] = None,
) -> AnalysisResponse:
    return AnalysisResponse(
        batch_id=batch_id,
        device_id=device_id,
        profile_id=calibration["profile_id"] if calibration else None,
        cached=cached,
        anomaly_count=len(anomalies),
        anomalies=[_internal_to_out(i, a) for i, a in enumerate(anomalies)],
        analysis_digest=_analysis_digest(anomalies, calibration),
    )


# ---------- 路由 ----------


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/batches/analyze", response_model=AnalysisResponse)
def analyze(req: AnalysisRequest, response: Response) -> AnalysisResponse:
    """提交一批采样序列进行异常归因；相同 batch_id 重复提交幂等回放。

    命中历史批次时，结果不是直接读取旧异常行，而是依据保存的原始采样与
    区段重算——旧口径写入的错误归因会在回放中被修正，并回写最新结果。

    指定 ``profile_id`` 时，服务在**同一 SQLite 事务**内读取该设备的
    不可变校准档案，按采样时刻以 ``corrected=raw*scale+offset`` 校准后
    再检测；任一采样无区间覆盖（422）或档案不存在（404）/与设备不匹配
    （409）时整批回滚、绝不部分写入。档案号与档案内容纳入请求哈希与
    ``analysis_digest``，回放、处置复核与联单核对保持同一结果。
    """
    with db.transaction() as conn:
        # 指定档案时先在事务内读取不可变档案并核对设备归属
        calibration = (
            _resolve_calibration_conn(conn, req)
            if req.profile_id is not None
            else None
        )
        request_hash = _canonical_hash(req, calibration)

        batch_row = db._select_batch(conn, req.batch_id)
        if batch_row is not None:
            if batch_row["request_hash"] != request_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"batch_id={req.batch_id} 已存在但请求内容不同；"
                        "幂等要求同一批次号只对应一份数据"
                    ),
                )
            response.headers["X-Cache"] = "HIT"
            stored = {
                "batch": dict(batch_row),
                "anomalies": db._select_anomalies(conn, req.batch_id),
            }
            device_id, anomalies, cal = _recompute_batch_conn(conn, stored)
            return _build_response(
                req.batch_id, device_id, anomalies, cached=True, calibration=cal
            )

        try:
            anomalies = _run_detection(req, calibration)
        except CalibrationCoverageError as exc:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"采样时刻 {from_epoch(exc.timestamp)} 不在校准档案 "
                    f"profile_id={req.profile_id} 的任何生效区间内；"
                    "整批未写入"
                ),
            ) from exc

        request_json = json.dumps(
            req.model_dump(mode="json"), ensure_ascii=False, sort_keys=True
        )
        try:
            db._insert_batch_conn(
                conn,
                batch_id=req.batch_id,
                device_id=req.device_id,
                request_hash=request_hash,
                request_json=request_json,
                anomalies=anomalies,
            )
        except sqlite3.IntegrityError:
            # 并发下另一请求抢先写入：按幂等规则回放（同样重算，不盲信旧行）
            row = db._select_batch(conn, req.batch_id)
            assert row is not None
            stored = {
                "batch": dict(row),
                "anomalies": db._select_anomalies(conn, req.batch_id),
            }
            if stored["batch"]["request_hash"] != request_hash:
                raise HTTPException(
                    status_code=409, detail="批次号已被其他内容占用"
                )
            response.headers["X-Cache"] = "HIT"
            device_id, replayed, cal = _recompute_batch_conn(conn, stored)
            return _build_response(
                req.batch_id, device_id, replayed, cached=True, calibration=cal
            )

        response.headers["X-Cache"] = "MISS"
        return _build_response(
            req.batch_id, req.device_id, anomalies,
            cached=False, calibration=calibration,
        )


@app.get("/api/v1/batches/{batch_id}", response_model=AnalysisResponse)
def get_batch(batch_id: str) -> AnalysisResponse:
    """按批次号回放历史分析结果（依据原始采样重算，自动修正旧口径归因）。"""
    stored = db.get_batch(batch_id)
    if stored is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 不存在")
    device_id, anomalies, calibration = _replay_batch(stored)
    return _build_response(
        batch_id, device_id, anomalies, cached=True, calibration=calibration
    )


# ---------- 处置复核 ----------


def _review_content_hash(req: CaseReviewRequest) -> str:
    """处置请求体（含案件号、批次、摘要、逐条结论/时间/说明）的规范化哈希。

    相同 case_id 同内容回放必须逐字节一致；任一处置字段变化都会得到不同哈希。
    """
    payload = {
        "batch_id": req.batch_id,
        "case_id": req.case_id,
        "analysis_digest": req.analysis_digest,
        "decisions": [
            {
                "seq": d.seq,
                "decision": d.decision,
                "handled_at": to_epoch(d.handled_at),
                "note": d.note,
            }
            for d in sorted(req.decisions, key=lambda d: d.seq)
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_review_response(
    case: dict[str, Any],
    decision_rows: list[dict[str, Any]],
    cached: bool,
) -> CaseReviewResponse:
    return CaseReviewResponse(
        batch_id=case["batch_id"],
        case_id=case["case_id"],
        analysis_digest=case["analysis_digest"],
        status=case["status"],
        cached=cached,
        decisions=[
            DecisionOut(
                seq=r["seq"],
                decision=r["decision"],
                handled_at=from_epoch(r["handled_at"]),
                note=r["note"],
            )
            for r in sorted(decision_rows, key=lambda r: r["seq"])
        ],
    )


@app.post("/api/v1/cases/review", response_model=CaseReviewResponse)
def submit_case_review(req: CaseReviewRequest, response: Response) -> CaseReviewResponse:
    """提交冷链异常处置复核；处置必须绑定由分析响应给出的确定快照。

    服务在**同一 SQLite 事务**内完成：重算异常规范 JSON → 比对
    ``analysis_digest``（过期返回 409，绝不落库）→ 校验逐条处置
    （恰好覆盖全部当前异常、序号不重复、dismissed 必填说明、handled_at
    不早于异常结束）→ 写入案件。全部异常为 resolved/dismissed 案件 closed，
    否则 open。相同 case_id 同内容幂等回放；换内容返回 409。
    """
    content_hash = _review_content_hash(req)

    with db.transaction() as conn:
        batch_row = db._select_batch(conn, req.batch_id)
        if batch_row is None:
            raise HTTPException(
                status_code=404, detail=f"批次 {req.batch_id} 不存在"
            )
        stored_batch = {
            "batch": dict(batch_row),
            "anomalies": db._select_anomalies(conn, req.batch_id),
        }

        existing = db.get_case_for_update(conn, req.case_id)
        if existing is not None:
            # 相同案件号：幂等只认同内容；换内容（含换批次/换快照/换结论）409
            if existing["case"]["content_hash"] != content_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"case_id={req.case_id} 已存在但提交内容不同；"
                        "案件处置不可变更，仅支持同内容幂等回放"
                    ),
                )
            response.headers["X-Cache"] = "HIT"
            return _build_review_response(
                existing["case"], existing["decisions"], cached=True
            )

        # 重算并在同一事务内比对摘要：快照过期直接 409（事务随异常回滚）。
        # 与 GET 一样，若库存异常行属旧口径会先被重算结果覆盖，保证摘要口径统一。
        _device_id, anomalies, calibration = _recompute_batch_conn(
            conn, stored_batch
        )
        current_digest = _analysis_digest(anomalies, calibration)
        if current_digest != req.analysis_digest:
            raise HTTPException(
                status_code=409,
                detail=(
                    "analysis_digest 与重算后的当前异常快照不一致；"
                    "处置依据已过期，请重新获取分析结果后再提交"
                ),
            )

        # 逐条校验：恰好覆盖全部当前异常、序号不重复
        # （重复序号在模型层已拒绝，这里再按当前序号集合复核）
        expected_seqs = set(range(len(anomalies)))
        given_seqs = {d.seq for d in req.decisions}
        if given_seqs != expected_seqs:
            missing = sorted(expected_seqs - given_seqs)
            extra = sorted(given_seqs - expected_seqs)
            raise HTTPException(
                status_code=422,
                detail=(
                    f"处置记录必须恰好覆盖全部当前异常（共 {len(anomalies)} 条）；"
                    f"缺失序号={missing}，越界序号={extra}"
                ),
            )

        by_seq = {d.seq: d for d in req.decisions}
        rows: list[tuple[int, str, float, Optional[str]]] = []
        for seq, anomaly in enumerate(anomalies):
            item = by_seq[seq]
            handled_at = to_epoch(item.handled_at)
            if handled_at < anomaly["end"]:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"序号 {seq} 的 handled_at 不得早于该异常结束时间 "
                        f"{from_epoch(anomaly['end'])}"
                    ),
                )
            note = item.note.strip() if item.note else None
            if item.decision == "dismissed" and not note:
                raise HTTPException(
                    status_code=422,
                    detail=f"序号 {seq} 的 decision=dismissed 必须填写说明 note",
                )
            rows.append((seq, item.decision, handled_at, note))

        status = (
            "closed"
            if all(
                d.decision in ("resolved", "dismissed") for d in req.decisions
            )
            else "open"
        )

        try:
            db.save_case(
                conn,
                case_id=req.case_id,
                batch_id=req.batch_id,
                analysis_digest=current_digest,
                content_hash=content_hash,
                status=status,
                decisions=rows,
            )
        except sqlite3.IntegrityError as exc:
            # 并发下案件已被其他内容抢先创建：按内容判定幂等/冲突
            existing = db.get_case_for_update(conn, req.case_id)
            assert existing is not None
            if existing["case"]["content_hash"] != content_hash:
                raise HTTPException(
                    status_code=409, detail="案件号已被其他处置内容占用"
                ) from exc
            response.headers["X-Cache"] = "HIT"
            return _build_review_response(
                existing["case"], existing["decisions"], cached=True
            )

        response.headers["X-Cache"] = "MISS"
        result = _build_review_response(
            {
                "case_id": req.case_id,
                "batch_id": req.batch_id,
                "analysis_digest": current_digest,
                "status": status,
            },
            [
                {"seq": seq, "decision": decision,
                 "handled_at": handled_at, "note": note}
                for seq, decision, handled_at, note in rows
            ],
            cached=False,
        )
    return result


# ---------- 运输联单风险快照 ----------


def _shipment_content_hash(req: ShipmentSnapshotRequest) -> str:
    """联单内容（按交接顺序的批次清单）的规范化哈希。

    清单顺序本身即联单内容：相同 shipment_id 只有逐字节一致才幂等回放，
    换内容（含换顺序、换案件、换摘要、换交接时间）返回 409。
    """
    payload = {
        "shipment_id": req.shipment_id,
        "batches": [
            {
                "batch_id": b.batch_id,
                "analysis_digest": b.analysis_digest,
                "case_id": b.case_id,
                "handoff_at": to_epoch(b.handoff_at),
            }
            for b in req.batches
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_shipment_response(
    ship: dict[str, Any],
    rows: list[dict[str, Any]],
    cached: bool,
) -> ShipmentSnapshotResponse:
    return ShipmentSnapshotResponse(
        shipment_id=ship["shipment_id"],
        cached=cached,
        result=ship["result"],
        batches=[
            ShipmentBatchOut(
                seq=r["seq"],
                batch_id=r["batch_id"],
                case_id=r["case_id"],
                analysis_digest=r["analysis_digest"],
                handoff_at=from_epoch(r["handoff_at"]),
                anomaly_count=r["anomaly_count"],
                case_status=r["case_status"],
            )
            for r in sorted(rows, key=lambda r: r["seq"])
        ],
    )


@app.post("/api/v1/shipments/snapshot", response_model=ShipmentSnapshotResponse)
def create_shipment_snapshot(
    req: ShipmentSnapshotRequest, response: Response
) -> ShipmentSnapshotResponse:
    """提交运输联单风险快照；全部批次校验通过后在同一事务内落库。

    服务在**同一 SQLite 事务**内对清单中每个批次：依据保存的原始采样重算
    异常并核对 ``analysis_digest``（过期即 409）→ 核对案件存在、属于该批次、
    处置依据当前快照且覆盖全部当前异常（缺失/不匹配即 409）。交接时间严格
    递增与批次不重复在请求模型层校验（422）。任一批次未通过校验时整个事务
    回滚，绝不部分写入，错误详情可定位到具体清单项。

    全部通过后保存不可变联单，按交接顺序返回各批次异常数与案件状态；存在
    open 案件时总体结果为 held，否则 released。相同 shipment_id 同内容幂等
    回放；换内容返回 409。
    """
    content_hash = _shipment_content_hash(req)

    with db.transaction() as conn:
        existing = db.get_shipment_for_update(conn, req.shipment_id)
        if existing is not None:
            # 相同联单号：幂等只认同内容；换内容 409（联单不可变更）
            if existing["shipment"]["content_hash"] != content_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"shipment_id={req.shipment_id} 已存在但提交内容不同；"
                        "联单不可变更，仅支持同内容幂等回放"
                    ),
                )
            response.headers["X-Cache"] = "HIT"
            return _build_shipment_response(
                existing["shipment"], existing["batches"], cached=True
            )

        items: list[tuple[int, str, str, str, float, int, str]] = []
        any_open = False
        for index, item in enumerate(req.batches):
            pos = f"联单第 {index + 1} 项（batch_id={item.batch_id}）"
            batch_row = db._select_batch(conn, item.batch_id)
            if batch_row is None:
                raise HTTPException(
                    status_code=404, detail=f"{pos} 不存在，请先提交分析"
                )
            stored_batch = {
                "batch": dict(batch_row),
                "anomalies": db._select_anomalies(conn, item.batch_id),
            }

            # 重算并在同一事务内比对摘要：批次过期直接 409（事务随异常回滚，
            # 旧口径行的重算覆盖也不会落库）。
            _device_id, anomalies, calibration = _recompute_batch_conn(
                conn, stored_batch
            )
            current_digest = _analysis_digest(anomalies, calibration)
            if current_digest != item.analysis_digest:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"{pos} 的 analysis_digest 与重算后的当前异常快照"
                        "不一致；批次快照已过期，请重新获取分析结果"
                    ),
                )

            case = db.get_case_for_update(conn, item.case_id)
            if case is None:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"{pos} 缺少处置：案件 case_id={item.case_id} 不存在，"
                        "请先提交处置复核"
                    ),
                )
            case_row = case["case"]
            if case_row["batch_id"] != item.batch_id:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"{pos} 的案件 case_id={item.case_id} 实际属于批次 "
                        f"{case_row['batch_id']}，案件与批次不匹配"
                    ),
                )
            if case_row["analysis_digest"] != current_digest:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"{pos} 的案件 case_id={item.case_id} 处置依据的快照"
                        "与当前异常不一致，未覆盖当前异常"
                    ),
                )
            covered = {d["seq"] for d in case["decisions"]}
            if covered != set(range(len(anomalies))):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"{pos} 的案件 case_id={item.case_id} 处置未覆盖当前"
                        f"全部异常（共 {len(anomalies)} 条）"
                    ),
                )

            if case_row["status"] == "open":
                any_open = True
            items.append(
                (
                    index,
                    item.batch_id,
                    item.case_id,
                    current_digest,
                    to_epoch(item.handoff_at),
                    len(anomalies),
                    case_row["status"],
                )
            )

        result = "held" if any_open else "released"
        try:
            db.save_shipment(conn, req.shipment_id, content_hash, result, items)
        except sqlite3.IntegrityError as exc:
            # 并发下联单已被其他内容抢先创建：按内容判定幂等/冲突
            existing = db.get_shipment_for_update(conn, req.shipment_id)
            assert existing is not None
            if existing["shipment"]["content_hash"] != content_hash:
                raise HTTPException(
                    status_code=409, detail="联单号已被其他内容占用"
                ) from exc
            response.headers["X-Cache"] = "HIT"
            return _build_shipment_response(
                existing["shipment"], existing["batches"], cached=True
            )

        response.headers["X-Cache"] = "MISS"
        result_body = _build_shipment_response(
            {"shipment_id": req.shipment_id, "result": result},
            [
                {
                    "seq": seq,
                    "batch_id": batch_id,
                    "case_id": case_id,
                    "analysis_digest": digest,
                    "handoff_at": handoff_at,
                    "anomaly_count": anomaly_count,
                    "case_status": case_status,
                }
                for seq, batch_id, case_id, digest, handoff_at,
                anomaly_count, case_status in items
            ],
            cached=False,
        )
    return result_body


# ---------- 设备温度校准档案 ----------


def _build_profile_response(
    content: dict[str, Any], cached: bool
) -> CalibrationProfileResponse:
    return CalibrationProfileResponse(
        device_id=content["device_id"],
        profile_id=content["profile_id"],
        cached=cached,
        intervals=[
            CalibrationIntervalOut(
                start=from_epoch(iv["start"]),
                end=from_epoch(iv["end"]),
                scale=iv["scale"],
                offset=iv["offset"],
            )
            for iv in content["intervals"]
        ],
        profile_digest=_profile_digest(content),
    )


@app.post(
    "/api/v1/devices/calibrations",
    response_model=CalibrationProfileResponse,
)
def register_calibration_profile(
    req: CalibrationProfileRequest, response: Response
) -> CalibrationProfileResponse:
    """登记设备温度校准档案（不可变）；相同 profile_id 同内容幂等回放。

    生效区间必须按时间排序、互不重叠（``[start, end)`` 半开，相邻区间允许
    首尾相接），每段 ``scale`` 为有限的正数、``offset`` 为有限数，否则
    返回 422。档案落库后不可变更：相同 ``profile_id`` 提交相同内容幂等
    回放（``cached: true``、``X-Cache: HIT``），换内容返回 409。
    """
    content = _calibration_content_from_request(req)
    canonical = _canonical_json(content)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    with db.transaction() as conn:
        existing = db._select_profile(conn, req.profile_id)
        if existing is not None:
            # 相同档案号：幂等只认同内容；换内容 409（档案不可变更）
            if existing["content_hash"] != digest:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"profile_id={req.profile_id} 已存在但档案内容不同；"
                        "校准档案不可变更，仅支持同内容幂等回放"
                    ),
                )
            response.headers["X-Cache"] = "HIT"
            return _build_profile_response(
                json.loads(existing["profile_json"]), cached=True
            )

        try:
            db._insert_profile_conn(
                conn, req.profile_id, req.device_id, digest, canonical
            )
        except sqlite3.IntegrityError as exc:
            # 并发下档案已被其他内容抢先创建：按内容判定幂等/冲突
            existing = db._select_profile(conn, req.profile_id)
            assert existing is not None
            if existing["content_hash"] != digest:
                raise HTTPException(
                    status_code=409, detail="档案号已被其他内容占用"
                ) from exc
            response.headers["X-Cache"] = "HIT"
            return _build_profile_response(
                json.loads(existing["profile_json"]), cached=True
            )

        response.headers["X-Cache"] = "MISS"
        return _build_profile_response(content, cached=False)
