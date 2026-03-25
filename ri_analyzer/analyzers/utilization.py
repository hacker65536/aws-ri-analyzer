"""CE 利用率データの集計・整形"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from ri_analyzer.fetchers.cost_explorer import RiUtilizationRecord


@dataclass
class UtilizationSummary:
    """サブスクリプション単位の集計"""
    subscription_id: str
    instance_type: str
    region: str
    platform: str
    periods: list[RiUtilizationRecord]

    @property
    def avg_utilization_pct(self) -> float:
        if not self.periods:
            return 0.0
        return sum(p.utilization_pct for p in self.periods) / len(self.periods)

    @property
    def total_unused_hours(self) -> float:
        return sum(p.unused_hours for p in self.periods)

    @property
    def total_net_savings(self) -> float:
        return sum(p.net_savings for p in self.periods)

    @property
    def total_on_demand_cost(self) -> float:
        return sum(p.on_demand_cost_if_used for p in self.periods)

    @property
    def total_amortized_fee(self) -> float:
        return sum(p.amortized_fee for p in self.periods)

    @property
    def savings_status(self) -> str:
        """Net Savings の判定"""
        if self.total_net_savings > 0:
            return "saving"
        return "losing"

    @property
    def status(self) -> str:
        pct = self.avg_utilization_pct
        if pct >= 80:
            return "ok"
        if pct >= 50:
            return "warning"
        return "low"


def summarize(records: list[RiUtilizationRecord]) -> list[UtilizationSummary]:
    """
    レコードをサブスクリプション ID ごとにまとめ、
    平均利用率・未使用時間・削減額を算出する。
    """
    grouped: defaultdict[str, list[RiUtilizationRecord]] = defaultdict(list)
    meta: dict[str, tuple[str, str, str]] = {}  # subscription_id → (instance_type, region, platform)

    for rec in records:
        grouped[rec.subscription_id].append(rec)
        if rec.subscription_id not in meta:
            meta[rec.subscription_id] = (rec.instance_type, rec.region, rec.platform)

    summaries = []
    for sub_id, periods in grouped.items():
        itype, region, platform = meta[sub_id]
        summaries.append(UtilizationSummary(
            subscription_id = sub_id,
            instance_type   = itype,
            region          = region,
            platform        = platform,
            periods         = sorted(periods, key=lambda r: r.period_start),
        ))

    return sorted(summaries, key=lambda s: s.avg_utilization_pct)
