"""CE 利用率データの集計・整形"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from ri_analyzer.fetchers.cost_explorer import RiUtilizationRecord

# AWS 正規化ユニット係数テーブル（RDS / ElastiCache 共通）
_NORM_FACTOR: dict[str, float] = {
    "nano":     0.25,
    "micro":    0.5,
    "small":    1,
    "medium":   2,
    "large":    4,
    "xlarge":   8,
    "2xlarge":  16,
    "4xlarge":  32,
    "8xlarge":  64,
    "10xlarge": 80,
    "12xlarge": 96,
    "16xlarge": 128,
    "24xlarge": 192,
    "32xlarge": 256,
}


def _parse_instance_family(instance_type: str) -> str:
    """'db.r5.large' → 'r5'"""
    parts = instance_type.split(".")
    return parts[1] if len(parts) >= 3 else instance_type


def _parse_instance_size(instance_type: str) -> str:
    """'db.r5.large' → 'large'"""
    parts = instance_type.split(".")
    return parts[2] if len(parts) >= 3 else ""


def _norm_factor(instance_type: str) -> float:
    size = _parse_instance_size(instance_type)
    return _NORM_FACTOR.get(size, 1.0)


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
    def count(self) -> int:
        """RI インスタンス個数（最初の期間の値を使用）"""
        return self.periods[0].count if self.periods else 0

    @property
    def normalized_units(self) -> float:
        """count × 正規化係数（例: db.r5.large x10 → 10 × 4 = 40）"""
        return self.count * _norm_factor(self.instance_type)

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

    return sorted(
        summaries,
        key=lambda s: (_parse_instance_family(s.instance_type), _norm_factor(s.instance_type)),
    )
