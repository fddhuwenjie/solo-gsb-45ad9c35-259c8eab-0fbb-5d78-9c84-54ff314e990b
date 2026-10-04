"""detect_anomalies 核心算法单元测试。"""

import unittest

from app.detection import Sample, Segment, detect_anomalies

DEVICE = "reefer-01"


def make_segments():
    # 两个区段：0–600 装货(0–8°C)，600–1200 运输(-20–-15°C)
    return [
        Segment("seg-1", "loading", 0, 600, 0.0, 8.0),
        Segment("seg-2", "transport", 600, 1200, -20.0, -15.0),
    ]


class DetectionTests(unittest.TestCase):
    def test_short_dip_does_not_break_anomaly(self):
        """短暂回落到限内不切断异常，异常合并为一段。"""
        samples = [
            Sample(0, 9.0),    # 高 1
            Sample(60, 9.5),   # 高 1.5
            Sample(120, 5.0),  # 回落正常（间隔 60s <= 120s）
            Sample(180, 10.0), # 继续越界 → 回落被收回
            Sample(240, 11.0),
        ]
        result = detect_anomalies(DEVICE, samples, make_segments(),
                                  gap_seconds=600, dip_tolerance_seconds=120)
        self.assertEqual(len(result), 1)
        a = result[0]
        self.assertEqual((a.start, a.end), (0, 240))
        self.assertEqual(a.peak_temperature, 11.0)
        self.assertEqual(a.peak_timestamp, 240)
        self.assertEqual(a.direction, "high")
        self.assertEqual(a.duration_seconds, 240)
        self.assertEqual(a.attributed_segment_id, "seg-1")

    def test_long_normal_gap_closes_anomaly(self):
        """正常点距离最后越界点超过容忍时长 → 收口为一段。"""
        samples = [
            Sample(0, 9.0),
            Sample(60, 9.0),
            Sample(300, 5.0),   # 已超 120s 容忍
            Sample(360, 10.0),  # 新异常
        ]
        result = detect_anomalies(DEVICE, samples, make_segments(),
                                  gap_seconds=600, dip_tolerance_seconds=120)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].end, 60)
        self.assertEqual(result[1].start, 360)

    def test_sampling_gap_splits_anomaly(self):
        """采样缺口（>=gap_seconds）两侧越界必须拆段。"""
        samples = [
            Sample(0, 9.0),
            Sample(60, 9.0),
            Sample(900, 9.0),   # 与上一点间隔 840s > 600s
            Sample(960, 9.0),
        ]
        result = detect_anomalies(DEVICE, samples, make_segments(),
                                  gap_seconds=600, dip_tolerance_seconds=120)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].start, 0)
        self.assertEqual(result[0].end, 60)
        self.assertEqual(result[1].start, 900)
        self.assertEqual(result[1].end, 960)

    def test_gap_boundary_is_inclusive(self):
        """相邻采样间隔恰好等于 gap_seconds 也拆段。"""
        samples = [
            Sample(0, 9.0),
            Sample(600, 9.0),  # 间隔恰好 600s
        ]
        result = detect_anomalies(DEVICE, samples, make_segments(),
                                  gap_seconds=600, dip_tolerance_seconds=120)
        self.assertEqual(len(result), 2)

    def test_dip_boundary_is_inclusive(self):
        """回落间隔恰好等于 dip_tolerance_seconds 不切断。"""
        samples = [
            Sample(0, 9.0),
            Sample(120, 5.0),   # 恰好 120s 回落
            Sample(180, 9.0),
        ]
        result = detect_anomalies(DEVICE, samples, make_segments(),
                                  gap_seconds=600, dip_tolerance_seconds=120)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].end, 180)

    def test_gap_after_dip_finalizes_immediately(self):
        """越界→回落→缺口→越界：回落后缺口必须立即收口。"""
        samples = [
            Sample(0, 9.0),
            Sample(60, 5.0),    # 回落
            Sample(900, 9.0),   # 缺口后再越界
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].start, 0)
        self.assertEqual(result[0].end, 0)

    def test_cross_segment_attribution_by_coverage(self):
        """异常跨区段时归因到异常区间内采样时刻最多的区段。"""
        # seg-1 中 1 个越界点，seg-2 中 4 个越界点 → 归因 seg-2
        samples = [
            Sample(540, 9.0),    # 仍在 seg-1（0–600）
            Sample(600, -10.0),  # seg-2 起点，上限 -15，越界
            Sample(700, -10.0),
            Sample(800, -10.0),
            Sample(900, -10.0),
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(len(result), 1)
        a = result[0]
        self.assertEqual((a.start, a.end), (540, 900))
        self.assertEqual(a.attributed_segment_id, "seg-2")
        self.assertEqual(a.attributed_segment_type, "transport")
        # 覆盖时长仅展示：seg-1 交 [540,600]=60，seg-2 交 [600,900]=300
        self.assertAlmostEqual(a.segment_coverage_seconds["seg-1"], 60.0)
        self.assertAlmostEqual(a.segment_coverage_seconds["seg-2"], 300.0)
        # 归因依据：异常区间内实际采样时刻数量 seg-1=1、seg-2=4
        self.assertEqual(a.segment_sample_counts, {"seg-2": 4, "seg-1": 1})
        # 峰值在 seg-2
        self.assertEqual(a.peak_segment_id, "seg-2")
        self.assertEqual(a.peak_temperature, -10.0)

    def test_attribution_counts_samples_not_overlap_seconds(self):
        """采样时刻数与区间重叠秒数指向不同区段时，按采样数归因。

        seg-1=[0,1000)、seg-2=[1000,1200)，异常区间 [990,1100]：
        旧口径（重叠秒数）seg-1=10 < seg-2=100 会归因 seg-2；
        seg-1 内实际有 3 个越界采样、seg-2 仅 1 个，新口径归因 seg-1。
        """
        segments = [
            Segment("seg-1", "loading", 0, 1000, 0.0, 8.0),
            Segment("seg-2", "transport", 1000, 1200, 0.0, 8.0),
        ]
        samples = [
            Sample(990, 9.0),   # seg-1
            Sample(995, 9.0),   # seg-1
            Sample(999, 9.0),   # seg-1（1000 恰好属于 seg-2）
            Sample(1100, 9.0),  # seg-2
        ]
        result = detect_anomalies(DEVICE, samples, segments)
        self.assertEqual(len(result), 1)
        a = result[0]
        self.assertEqual((a.start, a.end), (990, 1100))
        # 重叠秒数仍然 seg-2 占优，但归因以采样数为准
        self.assertLess(
            a.segment_coverage_seconds["seg-1"],
            a.segment_coverage_seconds["seg-2"],
        )
        self.assertEqual(a.segment_sample_counts, {"seg-1": 3, "seg-2": 1})
        self.assertEqual(a.attributed_segment_id, "seg-1")
        self.assertEqual(a.attributed_segment_type, "loading")

    def test_attribution_tie_counts_breaks_by_peak_segment(self):
        """采样数相同：取峰值所在区段，再相同取开始最早的区段。"""
        segments = [
            Segment("seg-1", "loading", 0, 1000, 0.0, 8.0),
            Segment("seg-2", "transport", 1000, 1200, 0.0, 8.0),
        ]
        # seg-1 与 seg-2 各 1 个越界点（平局），峰值在 seg-1 → seg-1
        samples = [
            Sample(990, 12.0),   # seg-1，偏离 4 → 峰值
            Sample(1100, 9.0),   # seg-2，偏离 1
        ]
        result = detect_anomalies(DEVICE, samples, segments)
        self.assertEqual(len(result), 1)
        a = result[0]
        self.assertEqual(a.segment_sample_counts, {"seg-1": 1, "seg-2": 1})
        self.assertEqual(a.peak_segment_id, "seg-1")
        self.assertEqual(a.attributed_segment_id, "seg-1")

    def test_attribution_counts_include_reclaimed_dip(self):
        """被收回的短暂回落点按其所属区段计入采样统计。"""
        # seg-1=[0,600)、seg-2=[600,1200)
        samples = [
            Sample(480, 9.0),    # seg-1 越界
            Sample(540, 5.0),    # seg-1 回落（60s <= 120s），计入 seg-1
            Sample(600, -10.0),  # seg-2 越界，回落被收回
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(len(result), 1)
        a = result[0]
        self.assertEqual((a.start, a.end), (480, 600))
        self.assertEqual(a.segment_sample_counts, {"seg-1": 2, "seg-2": 1})
        self.assertEqual(a.attributed_segment_id, "seg-1")

    def test_attribution_counts_exclude_unreclaimed_trailing_dip(self):
        """未被收回的尾随回落点超出异常 end，不计入任何区段。"""
        samples = [
            Sample(480, 9.0),    # seg-1 越界
            Sample(540, 5.0),    # seg-1 回落，之后再无越界 → 不收回
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(len(result), 1)
        a = result[0]
        self.assertEqual(a.end, 480)
        self.assertEqual(a.segment_sample_counts, {"seg-1": 1})
        self.assertEqual(a.attributed_segment_id, "seg-1")

    def test_low_excursion_direction(self):
        samples = [
            Sample(650, -25.0),  # seg-2 下限 -20，低越界
            Sample(700, -24.0),
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].direction, "low")
        self.assertEqual(result[0].peak_temperature, -25.0)
        self.assertEqual(result[0].attributed_segment_id, "seg-2")

    def test_uncovered_samples_cut_continuity(self):
        """区段覆盖不到的采样点不参与检测，并切断两侧连续性。"""
        # seg-1 只到 600，中间 700 无区段，seg-2 调整为 800–1200
        segments = [
            Segment("seg-1", "loading", 0, 600, 0.0, 8.0),
            Segment("seg-2", "transport", 800, 1200, -20.0, -15.0),
        ]
        samples = [
            Sample(0, 9.0),
            Sample(60, 9.0),
            Sample(700, 5.0),   # 无区段
            Sample(900, -10.0),
        ]
        result = detect_anomalies(DEVICE, samples, segments)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].attributed_segment_id, "seg-1")
        self.assertEqual(result[1].attributed_segment_id, "seg-2")

    def test_no_anomaly_when_all_normal(self):
        samples = [Sample(0, 4.0), Sample(60, 5.0), Sample(120, 7.0)]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(result, [])

    def test_boundary_values_are_inclusive(self):
        """恰好等于上下限不算越界。"""
        samples = [Sample(0, 0.0), Sample(60, 8.0), Sample(120, 4.0)]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(result, [])

    def test_duplicate_timestamps_keep_last(self):
        samples = [
            Sample(0, 9.0),
            Sample(0, 5.0),  # 同时刻后者覆盖
            Sample(60, 5.0),
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(result, [])

    def test_peak_uses_largest_deviation_from_own_band(self):
        """高/低温混在同一连续异常中，峰值按对自身区段的偏离幅度取。"""
        samples = [
            Sample(0, 9.0),     # 偏离 1
            Sample(60, 5.0),    # 回落
            Sample(120, 10.5),  # 偏离 2.5 → 峰值
        ]
        result = detect_anomalies(DEVICE, samples, make_segments())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].peak_temperature, 10.5)
        self.assertEqual(result[0].direction, "high")


if __name__ == "__main__":
    unittest.main()
