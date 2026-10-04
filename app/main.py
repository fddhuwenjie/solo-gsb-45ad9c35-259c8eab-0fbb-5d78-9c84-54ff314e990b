"""FastAPI 入口：冷链温区异常归因 API。"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse

from . import db
from .detection import (
    Anomaly,
    CalibrationGapError,
    CalibrationInterval,
    Sample,
    Segment,
    apply_calibration,
    detect_anomalies,
)
from .models import (
    AnalysisRequest,
    AnalysisResponse,
    AnomalyOut,
    CalibrationProfileIn,
    CalibrationProfileResponse,
    CalibrationIntervalOut,
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
        "可选设备温度校准档案：按采样时刻以 raw*scale+offset 校准后检测，"
        "档案标识与内容进入请求哈希与分析摘要。处置复核绑定分析快照；"
        "运输联单在同一事务内逐批次核对快照与案件处置后落库，"
        "给出 released/held 风险结论。"
    ),
    lifespan=lifespan,
)


# ---------- 序列化与计算 ----------


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """422 错误体里回显的非有限浮点（inf/NaN，如非法校准系数）无法被
    ``JSONResponse`` 序列化，会导致 500；这里将错误回显中的非有限值替换为
    字符串占位，保持 422 语义与现有 detail 结构不变。
    """

    def _sanitize(value: Any) -> Any:
        if isinstance(value, float) and not math.isfinite(value):
            return "non-finite"
        if isinstance(value, dict):
            # pydantic v2 的 ctx 会携带原始异常对象，JSON 不可序列化且无需回显
            return {
                k: _sanitize(v)
                for k, v in value.items()
                if k != "ctx"
            }
        if isinstance(value, list):
            return [_sanitize(v) for v in value]
        return value

    return JSONResponse(
        status_code=422,
        content={"detail": _sanitize(exc.errors())},
    )


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


# ---------- 设备温度校准档案 ----------


def _profile_descriptor(profile: dict[str, Any]) -> dict[str, Any]:
    """档案标识与内容的确定结构：进入请求哈希与 analysis_digest。

    时间用 epoch 秒、系数为有限数值，保证历史回放、处置复核与联单核对
    拿到逐字节一致的序列化结果。
    """
    return {
        "profile_id": profile["profile"]["profile_id"],
        "intervals": [
            {
                "start": iv["start_ts"],
                "end": iv["end_ts"],
                "scale": iv["scale"],
                "offset": iv["offset_value"],
            }
            for iv in profile["intervals"]
        ],
    }


def _calibration_intervals(
    descriptor: Optional[dict[str, Any]],
) -> Optional[list[CalibrationInterval]]:
    if descriptor is None:
        return None
    return [
        CalibrationInterval(
            start=iv["start"],
            end=iv["end"],
            scale=iv["scale"],
            offset=iv["offset"],
        )
        for iv in descriptor["intervals"]
    ]


def _resolve_profile_conn(
    conn: Any, profile_id: str, device_id: str
) -> dict[str, Any]:
    """事务内读取设备的不可变校准档案；不存在或档案属于其他设备即 409。"""
    profile = db.get_profile_for_update(conn, profile_id)
    if profile is None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"profile_id={profile_id} 不存在，请先提交设备温度校准档案"
            ),
        )
    if profile["profile"]["device_id"] != device_id:
        raise HTTPException(
            status_code=409,
            detail=(
                f"profile_id={profile_id} 属于设备 "
                f"{profile['profile']['device_id']}，与本次 device_id="
                f"{device_id} 不匹配"
            ),
        )
    return profile


def _analysis_digest(
    anomalies: list[dict[str, Any]],
    profile_descriptor: Optional[dict[str, Any]] = None,
) -> str:
    """重算后异常规范 JSON 的 SHA-256；处置必须绑定该摘要，防快照过期。

    使用校准档案时，摘要输入为 ``{"anomalies": ..., "profile": 档案标识与内容}``；
    未指定档案时保持只对异常列表取摘要的旧口径，旧调用结果不变。
    """
    if profile_descriptor is None:
        canonical = _canonical_anomaly_json(anomalies)
    else:
        canonical = json.dumps(
            {"anomalies": anomalies, "profile": profile_descriptor},
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _run_detection(
    req: AnalysisRequest,
    profile_descriptor: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    samples = [
        Sample(timestamp=to_epoch(s.timestamp), temperature=s.temperature)
        for s in req.samples
    ]
    # 指定档案时先按采样时刻校准（corrected=raw*scale+offset），再做越界检测；
    # 任一采样无区间覆盖抛 CalibrationGapError，由路由层整体拒绝（422）。
    intervals = _calibration_intervals(profile_descriptor)
    if intervals is not None:
        try:
            samples = apply_calibration(samples, intervals)
        except CalibrationGapError as exc:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"采样时刻 {from_epoch(exc.timestamp)} 未被校准档案 "
                    f"{req.profile_id} 的任何 [start,end) 生效区间覆盖；"
                    "整批不得写入，请补齐档案后重试"
                ),
            ) from exc
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
    req: AnalysisRequest,
    profile_descriptor: Optional[dict[str, Any]] = None,
) -> str:
    """对请求体（剔除 batch_id）做规范化哈希，用于识别同号不同内容。

    指定校准档案时，档案标识与内容（解析后的确定结构）一并入哈希：
    换档案、档案区间不同都会得到不同哈希。未指定档案时，载荷剔除
    profile_id 键，使规范化结果与档案特性上线前的旧调用逐字节一致——
    旧批次（旧库中无 profile_id 字段时算出的哈希）仍能正常幂等回放。
    """
    payload = req.model_dump(mode="json", exclude={"batch_id"})
    if profile_descriptor is not None:
        # 以解析后的档案内容覆盖入参里的 profile_id 引用
        payload["profile"] = profile_descriptor
    else:
        payload.pop("profile_id", None)
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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

    返回 (device_id, 最新口径的异常列表, 档案描述)。档案描述来自保存的
    原始请求：批次使用校准时在同一事务内读取不可变档案再按档案重放，
    保证历史回放、处置复核与联单核对使用同一份校准结果。重复 POST、
    按批次 GET、处置复核与联单核对共用此路径。
    """
    req = _request_from_stored(stored)
    descriptor: Optional[dict[str, Any]] = None
    if req.profile_id:
        profile = _resolve_profile_conn(conn, req.profile_id, req.device_id)
        descriptor = _profile_descriptor(profile)
    fresh = _run_detection(req, descriptor)

    # 与已存行比较；旧口径行（缺少 sample_counts 或归因不同）整体替换。
    stored_signatures = _stored_row_signatures(stored)
    fresh_signatures = [
        json.dumps(a, sort_keys=True, ensure_ascii=False) for a in fresh
    ]
    if stored_signatures != fresh_signatures:
        db._replace_anomalies_conn(conn, req.batch_id, fresh)

    return req.device_id, fresh, descriptor


def _build_response(
    batch_id: str,
    device_id: str,
    anomalies: list[dict[str, Any]],
    cached: bool,
    profile_id: Optional[str] = None,
    profile_descriptor: Optional[dict[str, Any]] = None,
) -> AnalysisResponse:
    return AnalysisResponse(
        batch_id=batch_id,
        device_id=device_id,
        profile_id=profile_id,
        cached=cached,
        anomaly_count=len(anomalies),
        anomalies=[_internal_to_out(i, a) for i, a in enumerate(anomalies)],
        analysis_digest=_analysis_digest(anomalies, profile_descriptor),
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

    指定 ``profile_id`` 时在**同一 SQLite 事务**内读取该设备的不可变
    校准档案，按采样时刻选取 ``[start,end)`` 区间，以
    ``corrected=raw*scale+offset`` 校准后再做越界检测；档案不存在、档案
    属于其他设备（409）或任一采样无区间覆盖（422）时整批拒绝、绝不写入。

    命中历史批次时，结果不是直接读取旧异常行，而是依据保存的原始采样与
    区段重算——旧口径写入的错误归因会在回放中被修正，并回写最新结果。
    """
    with db.transaction() as conn:
        batch_row = db._select_batch(conn, req.batch_id)
        if batch_row is not None:
            stored = {
                "batch": dict(batch_row),
                "anomalies": db._select_anomalies(conn, req.batch_id),
            }
            # 回放同样需要档案描述（档案缺失/不匹配在事务内即 409），
            # 保证哈希比对与摘要口径一致。
            descriptor: Optional[dict[str, Any]] = None
            if req.profile_id:
                profile = _resolve_profile_conn(
                    conn, req.profile_id, req.device_id
                )
                descriptor = _profile_descriptor(profile)
            request_hash = _canonical_hash(req, descriptor)
            if stored["batch"]["request_hash"] != request_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"batch_id={req.batch_id} 已存在但请求内容不同；"
                        "幂等要求同一批次号只对应一份数据"
                    ),
                )
            _device_id, anomalies, replay_descriptor = (
                _recompute_batch_conn(conn, stored)
            )
            response.headers["X-Cache"] = "HIT"
            return _build_response(
                req.batch_id,
                _device_id,
                anomalies,
                cached=True,
                profile_id=stored["batch"].get("profile_id"),
                profile_descriptor=replay_descriptor,
            )

        # 新批次：先在事务内解析档案（不存在/不匹配即 409 回滚），
        # 再校准检测（无覆盖即 422 回滚），最后与批次行同事务落库。
        descriptor = None
        if req.profile_id:
            profile = _resolve_profile_conn(conn, req.profile_id, req.device_id)
            descriptor = _profile_descriptor(profile)

        anomalies = _run_detection(req, descriptor)
        request_hash = _canonical_hash(req, descriptor)
        request_json = json.dumps(
            req.model_dump(mode="json"), ensure_ascii=False, sort_keys=True
        )

        try:
            db._save_batch_conn(
                conn,
                batch_id=req.batch_id,
                device_id=req.device_id,
                request_hash=request_hash,
                request_json=request_json,
                anomalies=anomalies,
                profile_id=req.profile_id,
            )
        except sqlite3.IntegrityError as exc:
            # 并发下另一请求抢先写入：按幂等规则回放（同样重算，不盲信旧行）
            batch_row = db._select_batch(conn, req.batch_id)
            assert batch_row is not None
            stored = {
                "batch": dict(batch_row),
                "anomalies": db._select_anomalies(conn, req.batch_id),
            }
            if stored["batch"]["request_hash"] != request_hash:
                raise HTTPException(
                    status_code=409, detail="批次号已被其他内容占用"
                ) from exc
            _device_id, replayed, replay_descriptor = (
                _recompute_batch_conn(conn, stored)
            )
            response.headers["X-Cache"] = "HIT"
            return _build_response(
                req.batch_id,
                _device_id,
                replayed,
                cached=True,
                profile_id=stored["batch"].get("profile_id"),
                profile_descriptor=replay_descriptor,
            )

        response.headers["X-Cache"] = "MISS"
        return _build_response(
            req.batch_id,
            req.device_id,
            anomalies,
            cached=False,
            profile_id=req.profile_id,
            profile_descriptor=descriptor,
        )


@app.get("/api/v1/batches/{batch_id}", response_model=AnalysisResponse)
def get_batch(batch_id: str) -> AnalysisResponse:
    """按批次号回放历史分析结果（依据原始采样重算，自动修正旧口径归因）。"""
    with db.transaction() as conn:
        batch_row = db._select_batch(conn, batch_id)
        if batch_row is None:
            raise HTTPException(status_code=404, detail=f"批次 {batch_id} 不存在")
        stored = {
            "batch": dict(batch_row),
            "anomalies": db._select_anomalies(conn, batch_id),
        }
        device_id, anomalies, descriptor = _recompute_batch_conn(conn, stored)
        return _build_response(
            batch_id,
            device_id,
            anomalies,
            cached=True,
            profile_id=stored["batch"].get("profile_id"),
            profile_descriptor=descriptor,
        )


# ---------- 设备温度校准档案 ----------


def _profile_content_hash(req: CalibrationProfileIn) -> str:
    """档案内容（设备 + 按序生效区间及系数）的规范化哈希。

    相同 profile_id 只有逐字节一致才幂等回放；任一区间边界或系数变化
    都会得到不同哈希（档案不可变，换内容返回 409）。
    """
    payload = {
        "device_id": req.device_id,
        "intervals": [
            {
                "start": to_epoch(iv.start),
                "end": to_epoch(iv.end),
                "scale": iv.scale,
                "offset": iv.offset,
            }
            for iv in req.intervals
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_profile_response(
    profile: dict[str, Any], cached: bool
) -> CalibrationProfileResponse:
    return CalibrationProfileResponse(
        device_id=profile["profile"]["device_id"],
        profile_id=profile["profile"]["profile_id"],
        cached=cached,
        intervals=[
            CalibrationIntervalOut(
                start=from_epoch(iv["start_ts"]),
                end=from_epoch(iv["end_ts"]),
                scale=iv["scale"],
                offset=iv["offset_value"],
            )
            for iv in profile["intervals"]
        ],
    )


@app.post(
    "/api/v1/calibration-profiles",
    response_model=CalibrationProfileResponse,
)
def submit_calibration_profile(
    req: CalibrationProfileIn, response: Response
) -> CalibrationProfileResponse:
    """提交设备温度校准档案（不可变）：同 profile_id 同内容幂等，换内容 409。

    区间按 ``[start, end)`` 半开生效、按时间排序且互不重叠（允许首尾相接），
    每段含有限正 ``scale`` 与正 ``offset``。档案一旦写入不可变更，分析时
    按采样时刻选取区间做 ``corrected=raw*scale+offset`` 校准。
    """
    content_hash = _profile_content_hash(req)

    with db.transaction() as conn:
        existing = db.get_profile_for_update(conn, req.profile_id)
        if existing is not None:
            if existing["profile"]["content_hash"] != content_hash:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"profile_id={req.profile_id} 已存在但提交内容不同；"
                        "校准档案不可变更，仅支持同内容幂等回放"
                    ),
                )
            response.headers["X-Cache"] = "HIT"
            return _build_profile_response(existing, cached=True)

        intervals = [
            (
                to_epoch(iv.start),
                to_epoch(iv.end),
                iv.scale,
                iv.offset,
            )
            for iv in req.intervals
        ]
        try:
            db.save_profile(
                conn,
                profile_id=req.profile_id,
                device_id=req.device_id,
                content_hash=content_hash,
                intervals=intervals,
            )
        except sqlite3.IntegrityError as exc:
            # 并发下档案已被其他内容抢先创建：按内容判定幂等/冲突
            existing = db.get_profile_for_update(conn, req.profile_id)
            assert existing is not None
            if existing["profile"]["content_hash"] != content_hash:
                raise HTTPException(
                    status_code=409, detail="档案号已被其他内容占用"
                ) from exc
            response.headers["X-Cache"] = "HIT"
            return _build_profile_response(existing, cached=True)

        response.headers["X-Cache"] = "MISS"
        return CalibrationProfileResponse(
            device_id=req.device_id,
            profile_id=req.profile_id,
            cached=False,
            intervals=[
                CalibrationIntervalOut(
                    start=from_epoch(start),
                    end=from_epoch(end),
                    scale=scale,
                    offset=offset,
                )
                for start, end, scale, offset in intervals
            ],
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
        _device_id, anomalies, profile_descriptor = _recompute_batch_conn(
            conn, stored_batch
        )
        current_digest = _analysis_digest(anomalies, profile_descriptor)
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
            _device_id, anomalies, profile_descriptor = (
                _recompute_batch_conn(conn, stored_batch)
            )
            current_digest = _analysis_digest(anomalies, profile_descriptor)
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
