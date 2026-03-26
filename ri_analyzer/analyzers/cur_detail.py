"""CUR (Athena) データの構造体・分析関数

AthenaClient から返された raw dict リストを構造体に変換し、
CE Recommendation とのファクトチェックなどを行う。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


# ---------------------------------------------------------------------------
# データ構造体
# ---------------------------------------------------------------------------

@dataclass
class CurInstanceRow:
    """稼働中インスタンス 1 行（account × region × instance_type × engine）"""
    account_id: str
    region: str
    instance_type: str
    engine: str         # RDS: "Aurora MySQL" / ElastiCache: "redis"
    deployment: str     # RDS: "Single-AZ" / ElastiCache: ""
    usage_hours: float
    unblended_cost: float

    @property
    def avg_instances(self) -> float:
        """月間の平均稼働台数（usage_hours ÷ 720）"""
        return self.usage_hours / 720.0


@dataclass
class CurCoverageRow:
    """CUR ベースの RI カバレッジ（account × region × instance_type）"""
    account_id: str
    region: str
    instance_type: str
    ri_hours: float
    od_hours: float
    total_hours: float
    coverage_pct: float

    @property
    def status(self) -> str:
        if self.coverage_pct >= 90:
            return "ok"
        if self.coverage_pct >= 50:
            return "warning"
        return "low"


@dataclass
class UnusedRiRow:
    """未使用 RI 費用 1 行（RIFee 行から集計）"""
    reservation_arn: str
    account_id: str
    region: str
    usage_type: str
    ri_fee_cost: float
    quantity: float


@dataclass
class RecommendationFactcheck:
    """CE Recommendation 1 件に対する CUR 実績の突き合わせ結果"""
    instance_type: str
    region: str
    platform: str               # CE の platform 文字列
    ce_count: int               # CE が推奨する購入台数
    cur_usage_hours: float      # CUR の実績使用時間（当月）
    cur_avg_instances: float    # usage_hours ÷ 720
    cur_ri_hours: float
    cur_od_hours: float

    @property
    def gap(self) -> float:
        """CE 推奨台数 - CUR 実績台数（プラスなら CE が多め）"""
        return self.ce_count - self.cur_avg_instances


# ---------------------------------------------------------------------------
# パーサー
# ---------------------------------------------------------------------------

def parse_rds_instances(rows: list[dict[str, Any]]) -> list[CurInstanceRow]:
    """running_rds_instances() の結果を CurInstanceRow に変換する。"""
    result = []
    for r in rows:
        result.append(CurInstanceRow(
            account_id=r.get("account_id", ""),
            region=r.get("region", ""),
            instance_type=r.get("instance_type", ""),
            engine=r.get("engine", ""),
            deployment=r.get("deployment", ""),
            usage_hours=_f(r.get("usage_hours")),
            unblended_cost=_f(r.get("unblended_cost")),
        ))
    return result


def parse_elasticache_nodes(rows: list[dict[str, Any]]) -> list[CurInstanceRow]:
    """running_elasticache_nodes() の結果を CurInstanceRow に変換する。"""
    result = []
    for r in rows:
        result.append(CurInstanceRow(
            account_id=r.get("account_id", ""),
            region=r.get("region", ""),
            instance_type=r.get("instance_type", ""),
            engine=r.get("cache_engine", ""),
            deployment="",
            usage_hours=_f(r.get("usage_hours")),
            unblended_cost=_f(r.get("unblended_cost")),
        ))
    return result


def parse_cur_coverage(rows: list[dict[str, Any]]) -> list[CurCoverageRow]:
    """ri_coverage_detail() の結果を CurCoverageRow に変換する。"""
    result = []
    for r in rows:
        result.append(CurCoverageRow(
            account_id=r.get("account_id", ""),
            region=r.get("region", ""),
            instance_type=r.get("instance_type", ""),
            ri_hours=_f(r.get("ri_hours")),
            od_hours=_f(r.get("od_hours")),
            total_hours=_f(r.get("total_hours")),
            coverage_pct=_f(r.get("coverage_pct")),
        ))
    return result


def parse_unused_ri(rows: list[dict[str, Any]]) -> list[UnusedRiRow]:
    """unused_ri_cost() の結果を UnusedRiRow に変換する。"""
    result = []
    for r in rows:
        result.append(UnusedRiRow(
            reservation_arn=r.get("reservation_arn", ""),
            account_id=r.get("account_id", ""),
            region=r.get("region", ""),
            usage_type=r.get("usage_type", ""),
            ri_fee_cost=_f(r.get("ri_fee_cost")),
            quantity=_f(r.get("quantity")),
        ))
    return result


# ---------------------------------------------------------------------------
# ファクトチェック
# ---------------------------------------------------------------------------

def factcheck_recommendations(
    rec_details: list,              # list[RiRecommendationDetail]
    cur_rows: list[CurInstanceRow],
) -> list[RecommendationFactcheck]:
    """CE Recommendation の推奨内容を CUR 実績で検証する。

    マッチング条件:
        instance_type == instance_type
        AND region == region
        AND cur.engine が rec.platform に部分一致（大文字小文字無視）
    """
    results: list[RecommendationFactcheck] = []

    for rec in rec_details:
        matched = [
            c for c in cur_rows
            if c.instance_type == rec.instance_type
            and c.region == rec.region
            and c.engine.lower() in rec.platform.lower()
        ]
        usage_hours = sum(c.usage_hours for c in matched)
        ri_hours = sum(
            c.usage_hours * (c.usage_hours - _od_hours(c)) / c.usage_hours
            if c.usage_hours > 0 else 0.0
            for c in matched
        )
        od_hours = sum(_od_hours(c) for c in matched)

        results.append(RecommendationFactcheck(
            instance_type=rec.instance_type,
            region=rec.region,
            platform=rec.platform,
            ce_count=rec.count,
            cur_usage_hours=usage_hours,
            cur_avg_instances=usage_hours / 720.0,
            cur_ri_hours=ri_hours,
            cur_od_hours=od_hours,
        ))

    return results


def _od_hours(row: CurInstanceRow) -> float:
    """CurInstanceRow は RI/OD 内訳を持たないので 0 を返す（cur_queries の DetailQuery 向け）。"""
    return 0.0


def _f(val: Any) -> float:
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0
