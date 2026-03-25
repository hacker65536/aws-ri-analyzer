"""RI カバレッジ分析

CE GetReservationCoverage のレコードを集計し、
アカウント × リージョン × インスタンスタイプ単位で
「RI カバー済み時間」vs「オンデマンド時間」を整理する。

TODO: 実行中インスタンスの詳細一覧（インスタンス ID 等）が必要な場合は
      Athena 経由で CUR にクエリする方式（案B）で実装予定。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from ri_analyzer.fetchers.cost_explorer import RiCoverageRecord
from ri_analyzer.analyzers.utilization import _parse_instance_family, _norm_factor


@dataclass
class CoverageSummary:
    """アカウント × リージョン × インスタンスタイプ の集計"""
    account_id: str
    region: str
    instance_type: str
    covered_hours: float
    on_demand_hours: float
    total_hours: float

    @property
    def norm_factor(self) -> float:
        return _norm_factor(self.instance_type)

    @property
    def covered_nus(self) -> float:
        return self.covered_hours * self.norm_factor

    @property
    def on_demand_nus(self) -> float:
        return self.on_demand_hours * self.norm_factor

    @property
    def total_nus(self) -> float:
        return self.total_hours * self.norm_factor

    @property
    def coverage_pct(self) -> float:
        if self.total_hours == 0:
            return 0.0
        return self.covered_hours / self.total_hours * 100

    @property
    def status(self) -> str:
        pct = self.coverage_pct
        if pct >= 90:
            return "ok"
        if pct >= 50:
            return "warning"
        return "low"


def analyze(records: list[RiCoverageRecord]) -> list[CoverageSummary]:
    """
    期間をまたぐレコードを集計し、CoverageSummary のリストを返す。
    on_demand_hours > 0 のものが「RI 未カバーあり」を意味する。
    """
    # (account_id, region, instance_type) → 集計値
    agg: defaultdict[tuple, dict] = defaultdict(
        lambda: {"covered": 0.0, "on_demand": 0.0, "total": 0.0}
    )

    for rec in records:
        key = (rec.account_id, rec.region, rec.instance_type)
        agg[key]["covered"]   += rec.covered_hours
        agg[key]["on_demand"] += rec.on_demand_hours
        agg[key]["total"]     += rec.total_hours

    summaries = [
        CoverageSummary(
            account_id      = key[0],
            region          = key[1],
            instance_type   = key[2],
            covered_hours   = v["covered"],
            on_demand_hours = v["on_demand"],
            total_hours     = v["total"],
        )
        for key, v in agg.items()
    ]

    # instance family → サイズ（norm_factor 昇順）→ account_id
    return sorted(
        summaries,
        key=lambda s: (_parse_instance_family(s.instance_type), s.norm_factor, s.account_id),
    )
