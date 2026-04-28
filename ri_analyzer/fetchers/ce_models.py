"""Cost Explorer のデータモデル（dataclass 定義のみ）

API 呼び出しと型定義を分離することで、テストやアナライザーが
boto3 に依存せずにデータ型を使えるようにする。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class RiSubscription:
    """
    RI サブスクリプション 1 件。
    CE の Attributes から組み立てる。
    """
    subscription_id: str
    account_id: str
    account_name: str
    region: str
    instance_type: str      # db.t4g.large
    platform: str           # Aurora / MySQL / PostgreSQL ...
    count: int              # numberOfInstances
    start_time: datetime
    end_time: datetime      # endDateTime → 有効期限
    status: str             # Active / Expired
    size_flexibility: str   # FlexRI / "" など
    offering_type: str      # All Upfront / Partial Upfront / No Upfront

    # CE averageOnDemandHourlyRate — CE が内部的に使用する現在のオンデマンド単価。
    # Pricing API と比較することで Multi-AZ かどうかを判定するために使う。キャッシュに含める。
    avg_od_rate: float = 0.0

    # Multi-AZ 判定結果。avg_od_rate と Pricing API 単価の比較で実行時に設定される。
    # キャッシュには保存しない（毎回 annotate_multi_az() で再計算）。
    multi_az: Optional[bool] = None

    @property
    def days_remaining(self) -> int:
        return (self.end_time - datetime.now(timezone.utc)).days

    @property
    def engine(self) -> str:
        """platform → 小文字エンジン名（coverage 分析用）"""
        return self.platform.lower()

    @property
    def instance_class(self) -> str:
        return self.instance_type


@dataclass
class RiUtilizationRecord:
    """1 サブスクリプション × 1 期間 の利用率レコード"""
    subscription_id: str
    period_start: str
    period_end: str
    instance_type: str
    region: str
    platform: str               # Aurora / MySQL / PostgreSQL ...
    count: int                  # numberOfInstances
    utilization_pct: float
    purchased_hours: float
    used_hours: float
    unused_hours: float
    net_savings: float              # = on_demand_cost_if_used - amortized_fee
    on_demand_cost_if_used: float   # 使用時間分をオンデマンドで払った場合のコスト
    amortized_fee: float            # RI の償却コスト（按分）


@dataclass
class RiCoverageRecord:
    """アカウント × リージョン × インスタンスタイプ × プラットフォーム単位のカバレッジレコード"""
    account_id: str
    region: str
    instance_type: str
    platform: str
    period_start: str
    period_end: str
    covered_hours: float       # RI 適用済み時間
    on_demand_hours: float     # RI 未適用（オンデマンド）時間
    total_hours: float
    coverage_pct: float        # = covered / total * 100


@dataclass
class RiRecommendationDetail:
    """1インスタンスタイプあたりの購入推奨レコード"""
    instance_type: str
    region: str
    platform: str           # Redis / Valkey / Aurora MySQL / etc.
    count: int              # 推奨購入数
    normalized_units: float
    upfront_cost: float
    estimated_monthly_savings: float
    estimated_savings_pct: float
    breakeven_months: float
    avg_utilization: float  # 直近の平均使用率（推奨根拠）


@dataclass
class RiRecommendationGroup:
    """サービス × 期間 × 支払いタイプ単位の推奨グループ"""
    service: str
    term: str           # "ONE_YEAR" / "THREE_YEARS"
    payment_option: str # "ALL_UPFRONT" / "PARTIAL_UPFRONT" / "NO_UPFRONT"
    details: list[RiRecommendationDetail]
    total_monthly_savings: float
    total_savings_pct: float
    currency: str
