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
from ri_analyzer.analyzers.utilization import _parse_instance_family, _norm_factor_for_engine

# ElastiCache: Redis と Valkey は size-flexible RI で互換性があるため同一グループとして扱う
_ELASTICACHE_COMPATIBLE_ENGINES = {"redis", "valkey"}
_ELASTICACHE_UNIFIED_PLATFORM = "Redis/Valkey"


def _normalize_platform(platform: str) -> str:
    """Redis / Valkey を統一プラットフォーム名に正規化する。

    CE が "Redis 7.x" や "redis6.x" のようなバージョン付き文字列を返す場合も対応する。
    """
    lower = platform.lower()
    for engine in _ELASTICACHE_COMPATIBLE_ENGINES:
        # 完全一致 / "redis " で始まる / "redis." で始まる
        if lower == engine or lower.startswith(engine + " ") or lower.startswith(engine + "."):
            return _ELASTICACHE_UNIFIED_PLATFORM
    return platform


@dataclass
class CoverageSummary:
    """アカウント × リージョン × インスタンスタイプ × プラットフォーム の集計

    NUs はレコードごとにエンジン別係数を掛けて事前集計済み。
    Redis/Valkey 混在グループでは各エンジンの係数が正しく反映される。
    """
    account_id: str
    region: str
    instance_type: str
    platform: str
    covered_hours: float
    on_demand_hours: float
    total_hours: float
    covered_nus: float       # 事前計算済み（エンジン別係数を使用）
    on_demand_nus: float
    total_nus: float

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


def analyze(records: list[RiCoverageRecord], split_engine: bool = False) -> list[CoverageSummary]:
    """
    期間をまたぐレコードを集計し、CoverageSummary のリストを返す。
    on_demand_hours > 0 のものが「RI 未カバーあり」を意味する。

    Parameters
    ----------
    split_engine : True の場合、Redis/Valkey を別グループとして表示する（デフォルト: 統合）
    """
    # (account_id, region, instance_type, normalized_platform) → 集計値
    agg: defaultdict[tuple, dict] = defaultdict(
        lambda: {"covered": 0.0, "on_demand": 0.0, "total": 0.0,
                 "covered_nus": 0.0, "on_demand_nus": 0.0, "total_nus": 0.0}
    )

    for rec in records:
        platform = rec.platform if split_engine else _normalize_platform(rec.platform)
        key = (rec.account_id, rec.region, rec.instance_type, platform)
        # エンジン別係数で NUs を算出（Redis と Valkey は係数が異なる）
        factor = _norm_factor_for_engine(rec.instance_type, rec.platform)
        agg[key]["covered"]       += rec.covered_hours
        agg[key]["on_demand"]     += rec.on_demand_hours
        agg[key]["total"]         += rec.total_hours
        agg[key]["covered_nus"]   += rec.covered_hours * factor
        agg[key]["on_demand_nus"] += rec.on_demand_hours * factor
        agg[key]["total_nus"]     += rec.total_hours * factor

    summaries = [
        CoverageSummary(
            account_id      = key[0],
            region          = key[1],
            instance_type   = key[2],
            platform        = key[3],
            covered_hours   = v["covered"],
            on_demand_hours = v["on_demand"],
            total_hours     = v["total"],
            covered_nus     = v["covered_nus"],
            on_demand_nus   = v["on_demand_nus"],
            total_nus       = v["total_nus"],
        )
        for key, v in agg.items()
    ]

    # platform → instance family → サイズ（NU昇順）→ account_id
    return sorted(
        summaries,
        key=lambda s: (s.platform, _parse_instance_family(s.instance_type),
                       s.total_nus / s.total_hours if s.total_hours > 0 else 0, s.account_id),
    )
