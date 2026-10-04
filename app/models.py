"""请求/响应数据模型。

时间戳入参接受 ISO-8601（含时区）；不带时区的时间戳按 UTC 处理。
所有出参时间戳统一为带 ``Z`` 后缀的 UTC ISO-8601 字符串。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SegmentType = Literal["loading", "unloading", "transport"]


def to_epoch(dt: datetime) -> float:
    """归一化为 epoch 秒；naive datetime 视为 UTC。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def from_epoch(ts: float) -> str:
    """epoch 秒 → UTC ISO-8601（Z 后缀，保留毫秒）。"""
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class SampleIn(BaseModel):
    timestamp: datetime
    temperature: float = Field(..., description="温度（摄氏度）")


class SegmentIn(BaseModel):
    segment_id: Optional[str] = Field(
        default=None, description="区段标识；省略时按顺序生成 seg-1、seg-2 …"
    )
    segment_type: SegmentType = Field(
        ..., description="区段类型：loading 装货 / unloading 卸货 / transport 运输"
    )
    start: datetime
    end: datetime
    lower_limit: float = Field(..., description="区段温度下限（摄氏度，含）")
    upper_limit: float = Field(..., description="区段温度上限（摄氏度，含）")


# ---------- 设备温度校准档案 ----------


class CalibrationIntervalIn(BaseModel):
    """档案中一段按时间生效的校准系数：corrected = raw * scale + offset。"""

    model_config = ConfigDict(allow_inf_nan=False)

    start: datetime
    end: datetime
    scale: float = Field(
        ..., gt=0, description="校准斜率（有限正数）；corrected = raw * scale + offset"
    )
    offset: float = Field(..., gt=0, description="校准偏移（有限正数）")


class CalibrationProfileIn(BaseModel):
    """设备温度校准档案提交：按时间排序、互不重叠的 [start,end) 生效区间。"""

    device_id: str = Field(..., min_length=1, max_length=128)
    profile_id: str = Field(
        ..., min_length=1, max_length=128,
        description="校准档案号；相同 profile_id 同内容幂等，换内容返回 409",
    )
    intervals: list[CalibrationIntervalIn] = Field(
        ..., min_length=1, description="按时间排序、互不重叠的 [start,end) 生效区间（至少 1 段）"
    )

    @model_validator(mode="after")
    def _validate_intervals(self) -> "CalibrationProfileIn":
        ordered = sorted(self.intervals, key=lambda iv: to_epoch(iv.start))
        for index, iv in enumerate(ordered, start=1):
            start, end = to_epoch(iv.start), to_epoch(iv.end)
            if end <= start:
                raise ValueError(f"校准区间 {index} 的 end 必须晚于 start")
            if index > 1:
                prev_end = to_epoch(ordered[index - 2].end)
                if prev_end > start:
                    raise ValueError(f"校准区间 {index - 1} 与 {index} 时间重叠")
        object.__setattr__(self, "intervals", ordered)
        return self


class CalibrationIntervalOut(BaseModel):
    start: str
    end: str
    scale: float
    offset: float


class CalibrationProfileResponse(BaseModel):
    device_id: str
    profile_id: str
    cached: bool = Field(..., description="true 表示相同档案号同内容幂等回放")
    intervals: list[CalibrationIntervalOut]


class AnalysisRequest(BaseModel):
    batch_id: str = Field(
        ..., min_length=1, max_length=128, description="客户端批次号，重复提交用于幂等回放"
    )
    device_id: str = Field(..., min_length=1, max_length=128)
    profile_id: Optional[str] = Field(
        default=None,
        max_length=128,
        description=(
            "设备温度校准档案号（可选）。指定后服务在事务内读取该设备的不可变档案，"
            "按采样时刻选取 [start,end) 区间，以 corrected=raw*scale+offset 校准后再做"
            "越界检测；任一采样无覆盖或档案与设备不匹配则整批拒绝（不落库）。"
            "不指定时保持旧调用口径，温度按原始值直接检测。"
        ),
    )
    samples: list[SampleIn] = Field(..., min_length=1)
    segments: list[SegmentIn] = Field(..., min_length=1)
    gap_seconds: float = Field(
        default=600.0,
        gt=0,
        description="采样缺口阈值，相邻采样间隔达到该秒数则异常拆段",
    )
    dip_tolerance_seconds: float = Field(
        default=120.0,
        ge=0,
        description="短暂回落容忍秒数，回落不超过该时长则不切断异常",
    )

    @field_validator("samples")
    @classmethod
    def _validate_samples(cls, v: list[SampleIn]) -> list[SampleIn]:
        if not v:
            raise ValueError("samples 不能为空")
        return v

    @model_validator(mode="after")
    def _validate_segments(self) -> "AnalysisRequest":
        if self.dip_tolerance_seconds > self.gap_seconds:
            raise ValueError("dip_tolerance_seconds 不能大于 gap_seconds")

        # 按开始时间排序后整体校验
        ordered = sorted(self.segments, key=lambda s: to_epoch(s.start))
        seen_ids: set[str] = set()
        for index, seg in enumerate(ordered, start=1):
            seg_id = seg.segment_id or f"seg-{index}"
            if seg_id in seen_ids:
                raise ValueError(f"segment_id 重复: {seg_id}")
            seen_ids.add(seg_id)
            start, end = to_epoch(seg.start), to_epoch(seg.end)
            if end <= start:
                raise ValueError(f"区段 {seg_id} 的 end 必须晚于 start")
            if seg.lower_limit > seg.upper_limit:
                raise ValueError(f"区段 {seg_id} 的 lower_limit 不能大于 upper_limit")
            if index > 1:
                prev = ordered[index - 2]
                if to_epoch(prev.end) > start:
                    raise ValueError(f"区段 {prev.segment_id} 与 {seg_id} 时间重叠")
        # 回填默认 id 与排序结果
        for index, seg in enumerate(ordered, start=1):
            if not seg.segment_id:
                seg.segment_id = f"seg-{index}"
        object.__setattr__(self, "segments", ordered)
        return self


class AnomalyOut(BaseModel):
    seq: int = Field(
        ..., description="批次内异常序号（从 0 起），处置复核按该序号逐条给结论"
    )
    start: str
    end: str
    peak_temperature: float
    peak_timestamp: str
    direction: Literal["high", "low"]
    peak_segment_id: str
    attributed_segment_id: str
    attributed_segment_type: SegmentType
    duration_seconds: float
    segment_coverage_seconds: dict[str, float]
    segment_sample_counts: dict[str, int] = Field(
        default_factory=dict,
        description="异常区间内实际落入各区段的采样时刻数量（跨段归因依据）",
    )


class AnalysisResponse(BaseModel):
    batch_id: str
    device_id: str
    profile_id: Optional[str] = Field(
        default=None,
        description="本次分析使用的设备温度校准档案号；未指定档案时为 null",
    )
    cached: bool = Field(..., description="true 表示幂等命中，结果来自存储回放")
    anomaly_count: int
    anomalies: list[AnomalyOut]
    analysis_digest: str = Field(
        ...,
        description=(
            "重算后异常规范 JSON 的 SHA-256（十六进制）；处置复核必须原样回传，"
            "服务端在同一事务中重算比对，不一致即快照过期（409）"
        ),
    )


Decision = Literal["resolved", "accepted", "dismissed"]


class DecisionIn(BaseModel):
    seq: int = Field(..., ge=0, description="异常序号，对应分析响应中 anomalies[].seq")
    decision: Decision
    handled_at: datetime = Field(..., description="处置时间；不得早于该异常的 end")
    note: Optional[str] = Field(
        default=None, max_length=2000, description="处置说明；decision=dismissed 时必填"
    )


class CaseReviewRequest(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=128)
    case_id: str = Field(
        ..., min_length=1, max_length=128, description="案件号；相同案件号同内容幂等回放"
    )
    analysis_digest: str = Field(
        ..., min_length=1, description="分析响应中的 analysis_digest，绑定处置时的确定快照"
    )
    decisions: list[DecisionIn] = Field(
        ..., description="逐条异常处置；必须恰好覆盖当前全部异常且序号不重复"
    )

    @model_validator(mode="after")
    def _validate_unique_seq(self) -> "CaseReviewRequest":
        seqs = [d.seq for d in self.decisions]
        if len(set(seqs)) != len(seqs):
            raise ValueError("decisions 中 seq 不能重复")
        return self


class DecisionOut(BaseModel):
    seq: int
    decision: Decision
    handled_at: str
    note: Optional[str] = None


CaseStatus = Literal["open", "closed"]


class CaseReviewResponse(BaseModel):
    batch_id: str
    case_id: str
    analysis_digest: str
    status: CaseStatus = Field(
        ...,
        description="全部异常为 resolved/dismissed 时 closed；存在 accepted 时 open",
    )
    cached: bool = Field(..., description="true 表示相同案件同内容幂等回放")
    decisions: list[DecisionOut]


# ---------- 运输联单风险快照 ----------


class WaybillItemIn(BaseModel):
    batch_id: str = Field(..., min_length=1, max_length=128, description="分析批次号")
    analysis_digest: str = Field(
        ..., min_length=1, description="该批次分析响应中的 analysis_digest，绑定当前异常快照"
    )
    case_id: str = Field(
        ..., min_length=1, max_length=128, description="覆盖该批次全部当前异常的处置案件号"
    )
    handoff_at: datetime = Field(
        ..., description="该批次交接时间；联单内必须按交接顺序严格递增"
    )


class ShipmentSnapshotRequest(BaseModel):
    shipment_id: str = Field(
        ..., min_length=1, max_length=128,
        description="运输联单号；相同联单号同内容幂等回放，换内容返回 409",
    )
    batches: list[WaybillItemIn] = Field(
        ..., min_length=1, description="按交接顺序排列的批次清单；批次不可重复"
    )

    @model_validator(mode="after")
    def _validate_handoff_order(self) -> "ShipmentSnapshotRequest":
        seen: set[str] = set()
        prev: Optional[float] = None
        for index, item in enumerate(self.batches):
            if item.batch_id in seen:
                raise ValueError(f"batches 中 batch_id 重复: {item.batch_id}")
            seen.add(item.batch_id)
            ts = to_epoch(item.handoff_at)
            if prev is not None and ts <= prev:
                raise ValueError(
                    f"batches[{index}] 的 handoff_at 必须晚于前一批次"
                    "（交接时间按清单顺序严格递增）"
                )
            prev = ts
        return self


class ShipmentBatchOut(BaseModel):
    seq: int = Field(..., description="联单内交接序号（从 0 起，与提交顺序一致）")
    batch_id: str
    case_id: str
    analysis_digest: str
    handoff_at: str
    anomaly_count: int = Field(..., description="该批次重算后的当前异常数")
    case_status: CaseStatus = Field(..., description="该批次处置案件状态")


ShipmentResult = Literal["released", "held"]


class ShipmentSnapshotResponse(BaseModel):
    shipment_id: str
    cached: bool = Field(..., description="true 表示相同联单同内容幂等回放")
    result: ShipmentResult = Field(
        ..., description="总体结果：全部案件 closed 为 released；存在 open 案件为 held"
    )
    batches: list[ShipmentBatchOut]
