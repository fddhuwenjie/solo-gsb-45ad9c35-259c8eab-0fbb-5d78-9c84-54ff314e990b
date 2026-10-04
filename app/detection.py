"""核心检测算法。

规则
----
1. 每个采样点按时间戳落入一个装卸/运输区段，采用该区段的上下限。
   相邻区段按 ``[start, end)`` 半开区间划分时间轴（最后一个区段右端
   闭合），因此交界时刻无歧义地属于后一个区段。
2. 温度超出该区段 [lower_limit, upper_limit] 即为越界。
3. 连续越界构成异常：越界过程中允许短暂回落（不超过
   ``dip_tolerance_seconds`` 且回落期间没有跨采样缺口），异常不被切断。
4. 相邻采样间隔达到 ``gap_seconds`` 即视为采样缺口，缺口两侧拆成独立异常。
5. 异常可跨越区段边界（属于同一连续异常），归因于**异常时间区间内实际
   采样时刻数量**最多的区段（统计该区间内按规则 1 落入该区段的采样点，
   含被收回的短暂回落点）；采样数相同则取峰值所属区段，再相同取最早区段。
6. 未被任何区段覆盖的采样点不参与检测，并且其两侧的异常连续性被切断。

时间统一使用 epoch 秒（浮点），由 API 层负责 ISO-8601 转换。
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Literal, Optional, Sequence

Direction = Literal["high", "low"]


class CalibrationGapError(ValueError):
    """采样时刻未被校准档案的任何 [start, end) 区间覆盖。"""

    def __init__(self, timestamp: float):
        self.timestamp = timestamp
        super().__init__(f"采样时刻 {timestamp} 无校准区间覆盖")


@dataclass(frozen=True)
class Sample:
    timestamp: float  # epoch 秒
    temperature: float


@dataclass(frozen=True)
class CalibrationInterval:
    """校准系数生效区间（半开 [start, end)）。"""

    start: float  # epoch 秒，含
    end: float  # epoch 秒，不含
    scale: float
    offset: float


def apply_calibration(
    samples: Sequence[Sample], intervals: Sequence[CalibrationInterval]
) -> list[Sample]:
    """按 ``corrected = raw * scale + offset`` 校准全部采样。

    区间按 ``[start, end)`` 半开划分时间轴（按 start 排序、互不重叠，
    由 API 层保证）；任一采样时刻未被覆盖即抛 :class:`CalibrationGapError`，
    由调用方整体拒绝、绝不部分写入。
    """
    ordered = sorted(intervals, key=lambda iv: iv.start)
    starts = [iv.start for iv in ordered]
    calibrated: list[Sample] = []
    for s in samples:
        # 最后一个 start <= ts 的候选区间，再校验 ts < end
        idx = bisect_right(starts, s.timestamp) - 1
        if idx < 0 or s.timestamp >= ordered[idx].end:
            raise CalibrationGapError(s.timestamp)
        iv = ordered[idx]
        calibrated.append(
            Sample(s.timestamp, s.temperature * iv.scale + iv.offset)
        )
    return calibrated



@dataclass(frozen=True)
class Segment:
    segment_id: str
    segment_type: str
    start: float  # epoch 秒，含
    end: float  # epoch 秒；最后一个区段含，其余按半开 [start, end)
    lower_limit: float
    upper_limit: float


@dataclass
class Anomaly:
    device_id: str
    start: float
    end: float
    peak_temperature: float
    peak_timestamp: float
    direction: Direction
    peak_segment_id: str
    attributed_segment_id: str
    attributed_segment_type: str
    duration_seconds: float
    segment_coverage_seconds: dict[str, float] = field(default_factory=dict)
    # 异常时间区间内实际落入各区段的采样时刻数量（归因依据）
    segment_sample_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class _Group:
    """状态机中一次连续越界的累积状态。"""

    start: float
    end: float
    violating: list[tuple[float, float, Segment]] = field(default_factory=list)
    # 异常时间区间内被区段覆盖的全部采样点（含被收回的短暂回落点），
    # 用于按实际采样时刻数量做跨区段归因
    points: list[tuple[float, float, Segment]] = field(default_factory=list)
    # 短暂回落的正常点（可能被后续越界收回）
    dip_samples: list[tuple[float, float, Optional[Segment]]] = field(
        default_factory=list
    )


def _segment_at(
    segments: Sequence[Segment], ts: float, last_end: float
) -> Optional[Segment]:
    """定位 ts 所属区段。

    时间轴按 ``[start, end)`` 半开区间无歧义划分；恰好落在最后一个区段
    右端点的时刻仍属于最后一个区段。区段不重叠（API 层保证）。
    """
    if ts > last_end:
        return None
    lo, hi = 0, len(segments) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        seg = segments[mid]
        if ts < seg.start:
            hi = mid - 1
        elif ts >= seg.end and not (mid == len(segments) - 1 and ts == seg.end):
            lo = mid + 1
        else:
            return seg
    return None


def _is_violation(temperature: float, seg: Segment) -> bool:
    return temperature < seg.lower_limit or temperature > seg.upper_limit


def _build_anomaly(device_id: str, group: _Group, segments: Sequence[Segment]) -> Anomaly:
    """把一次连续越界组转换为异常结果，完成峰值与归因计算。"""
    # 峰值只看真正越界的采样点；偏离自身所在区段幅度最大者为峰值
    peak_ts, peak_temp, peak_seg = max(
        group.violating,
        key=lambda item: _deviation(item[1], item[2]),
    )
    direction: Direction = (
        "high" if peak_temp > peak_seg.upper_limit else "low"
    )

    # 异常时间区间与每个区段的覆盖时长（区间交集，仅用于结果展示）。
    # 零时长异常（start==end，单个越界点）也计入所属区段，交集记为 0。
    coverage: dict[str, float] = {}
    for seg in segments:
        overlap_start = max(group.start, seg.start)
        overlap_end = min(group.end, seg.end)
        if overlap_end >= overlap_start:
            coverage[seg.segment_id] = overlap_end - overlap_start

    # 归因依据：异常时间区间 [start, end] 内实际落入各区段的采样时刻数量
    # （含被收回的短暂回落点）。区间闭合统计，保证首尾越界点都计入；
    # 未收口的尾随回落点超出 end，天然不在区间内。
    sample_counts: dict[str, int] = {}
    for point_ts, _, point_seg in group.points:
        if group.start <= point_ts <= group.end:
            sample_counts[point_seg.segment_id] = (
                sample_counts.get(point_seg.segment_id, 0) + 1
            )

    # 归因：实际采样时刻数最多 → 峰值所在区段 → 最早出现的区段
    def _attr_key(seg: Segment) -> tuple[int, float, float]:
        return (
            sample_counts.get(seg.segment_id, 0),
            1.0 if seg.segment_id == peak_seg.segment_id else 0.0,
            -seg.start,  # max 取值，起点越早键越大
        )

    # 只有区间内确实含采样点的区段才参与归因（连续异常至少有一个越界点）
    covered = [s for s in segments if s.segment_id in sample_counts]
    attributed = max(covered, key=_attr_key)

    return Anomaly(
        device_id=device_id,
        start=group.start,
        end=group.end,
        peak_temperature=peak_temp,
        peak_timestamp=peak_ts,
        direction=direction,
        peak_segment_id=peak_seg.segment_id,
        attributed_segment_id=attributed.segment_id,
        attributed_segment_type=attributed.segment_type,
        duration_seconds=group.end - group.start,
        segment_coverage_seconds=dict(
            sorted(coverage.items(), key=lambda kv: kv[1], reverse=True)
        ),
        segment_sample_counts=dict(
            sorted(sample_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ),
    )


def _deviation(temperature: float, seg: Segment) -> float:
    if temperature > seg.upper_limit:
        return temperature - seg.upper_limit
    return seg.lower_limit - temperature


def detect_anomalies(
    device_id: str,
    samples: Sequence[Sample],
    segments: Sequence[Segment],
    gap_seconds: float = 600.0,
    dip_tolerance_seconds: float = 120.0,
) -> list[Anomaly]:
    """检测连续越界异常并完成区段归因。

    参数语义见模块文档串。返回按开始时间排序的异常列表。
    """
    if dip_tolerance_seconds > gap_seconds:
        raise ValueError("dip_tolerance_seconds 不能大于 gap_seconds")

    ordered_segments = sorted(segments, key=lambda s: s.start)
    last_end = ordered_segments[-1].end

    # 时间戳去重（同一时刻保留最后一个温度值），再按时间排序
    by_ts: dict[float, float] = {}
    for s in samples:
        by_ts[s.timestamp] = s.temperature
    timeline = sorted(by_ts.items())

    anomalies: list[Anomaly] = []
    current: Optional[_Group] = None
    prev_ts: Optional[float] = None

    def finalize(group: Optional[_Group]) -> None:
        if group is not None and group.violating:
            anomalies.append(_build_anomaly(device_id, group, ordered_segments))

    for ts, temp in timeline:
        seg = _segment_at(ordered_segments, ts, last_end)
        gap = None if prev_ts is None else ts - prev_ts

        # 采样缺口优先判定：无论本点是否越界，两侧连续性都被切断。
        # 已进入回落的组无法再被收回，立即收口；回落为空的组在最后一个
        # 越界点收口，当前组置空，本点若继续越界则在下面另起新组。
        # 间隔达到 gap_seconds 即视为缺口（>=）。
        if gap is not None and gap >= gap_seconds and current is not None:
            finalize(current)
            current = None

        if seg is None:
            # 未被任何区段覆盖：不参与检测，并切断两侧连续性
            if current is not None:
                finalize(current)
                current = None
            prev_ts = ts
            continue

        violating = _is_violation(temp, seg)

        if violating:
            if current is None:
                current = _Group(start=ts, end=ts)
            current.violating.append((ts, temp, seg))
            current.points.append((ts, temp, seg))
            current.end = ts
            # 回落被后续越界收回，回落点已在 points 中，成为区间内部点
            current.dip_samples.clear()
        elif current is not None:
            # 正常点：可能是越界中的短暂回落
            elapsed = ts - current.end
            if elapsed <= dip_tolerance_seconds:
                current.dip_samples.append((ts, temp, seg))
                # 先计入候选点；若回落未被收回（点在 end 之后），
                # 归因统计时按 [start, end] 闭区间自然剔除
                current.points.append((ts, temp, seg))
            else:
                finalize(current)
                current = None

        prev_ts = ts

    finalize(current)
    anomalies.sort(key=lambda a: a.start)
    return anomalies
