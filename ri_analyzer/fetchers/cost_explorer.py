"""Cost Explorer から RI データを取得する

使用 API: ce:GetReservationUtilization
  - Payer アカウントから呼び出す（全アカウント集計）
  - GroupBy: SUBSCRIPTION_ID → サブスクリプション単位
  - Attributes に endDateTime / numberOfInstances 等が含まれるため
    DescribeReservedDBInstances は不要

CE は us-east-1 エンドポイントのみ。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any


def _ce_time_period(lookback_days: int) -> tuple[str, str]:
    """
    CE クエリ用の (start, end) 日付文字列を返す。

    CE のデータには最大 48 時間のタイムラグがあるため、
      end   = UTC 現在時刻 - 48 時間 の日付
      start = end - lookback_days
    """
    now_utc  = datetime.now(timezone.utc)
    end_dt   = now_utc - timedelta(hours=48)
    end_date = end_dt.date()
    start_date = end_date - timedelta(days=lookback_days)
    return str(start_date), str(end_date)

import boto3
from botocore.exceptions import ClientError

# CE に渡す Service 名
_CE_SERVICE_NAMES: dict[str, str] = {
    "rds":         "Amazon Relational Database Service",
    "elasticache": "Amazon ElastiCache",
    "opensearch":  "Amazon OpenSearch Service",
    "redshift":    "Amazon Redshift",
    "ec2":         "Amazon Elastic Compute Cloud - Compute",
}


# ──────────────────────────────────────────────
# データ型
# ──────────────────────────────────────────────

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
    utilization_pct: float
    purchased_hours: float
    used_hours: float
    unused_hours: float
    net_savings: float              # = on_demand_cost_if_used - amortized_fee
    on_demand_cost_if_used: float   # 使用時間分をオンデマンドで払った場合のコスト
    amortized_fee: float            # RI の償却コスト（按分）


# ──────────────────────────────────────────────
# 取得関数
# ──────────────────────────────────────────────

def fetch_ri_subscriptions(
    payer_profile: str,
    service: str,
    lookback_days: int = 30,
) -> tuple[list[RiSubscription], list[RiUtilizationRecord]]:
    """
    CE GetReservationUtilization を呼び出し、
    RI サブスクリプション一覧と利用率レコードを同時に返す。

    Parameters
    ----------
    payer_profile : Payer アカウントのプロファイル名
    service       : 対象サービスキー ("rds" など)
    lookback_days : 集計期間（日数）

    Returns
    -------
    (subscriptions, utilization_records) のタプル
    """
    ce_service = _CE_SERVICE_NAMES.get(service)
    if not ce_service:
        raise ValueError(f"未対応のサービスです: {service}。対応: {list(_CE_SERVICE_NAMES)}")

    start_date, end_date = _ce_time_period(lookback_days)

    session = boto3.Session(profile_name=payer_profile, region_name="us-east-1")
    ce = session.client("ce")

    try:
        resp = ce.get_reservation_utilization(
            TimePeriod={
                "Start": start_date,
                "End":   end_date,
            },
            Filter={
                "Dimensions": {
                    "Key":    "SERVICE",
                    "Values": [ce_service],
                }
            },
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SUBSCRIPTION_ID"},
            ],
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDeniedException", "OptInRequired"):
            raise PermissionError(
                f"Cost Explorer へのアクセスが拒否されました ({code})。"
                "Payer アカウントのプロファイルと IAM 権限を確認してください。"
            ) from e
        raise

    # サブスクリプションは期間をまたいで重複するため、subscription_id で dedup
    seen_subs: dict[str, RiSubscription] = {}
    util_records: list[RiUtilizationRecord] = []

    for period in resp.get("UtilizationsByTime", []):
        tp = period["TimePeriod"]
        for group in period.get("Groups", []):
            sub_id = group.get("Value", "")   # "Key" は次元名(例:"SubscriptionId")、"Value" が実際のID
            attrs  = group.get("Attributes", {})
            util   = group.get("Utilization", {})

            # ── サブスクリプション（初出時のみ登録）──────────
            if sub_id not in seen_subs:
                seen_subs[sub_id] = _parse_subscription(sub_id, attrs)

            # ── 利用率レコード ─────────────────────────────
            util_records.append(RiUtilizationRecord(
                subscription_id       = sub_id,
                period_start          = tp["Start"],
                period_end            = tp["End"],
                instance_type         = attrs.get("instanceType", ""),
                region                = attrs.get("region", ""),
                platform              = attrs.get("platform", ""),
                utilization_pct       = float(util.get("UtilizationPercentage", 0)),
                purchased_hours       = float(util.get("PurchasedHours", 0)),
                used_hours            = float(util.get("TotalActualHours", 0)),
                unused_hours          = float(util.get("UnusedHours", 0)),
                net_savings           = float(util.get("NetRISavings", 0)),
                on_demand_cost_if_used= float(util.get("OnDemandCostOfRIHoursUsed", 0)),
                amortized_fee         = float(util.get("TotalAmortizedFee", 0)),
            ))

    return list(seen_subs.values()), util_records


@dataclass
class RiCoverageRecord:
    """アカウント × リージョン × インスタンスタイプ単位のカバレッジレコード"""
    account_id: str
    region: str
    instance_type: str
    period_start: str
    period_end: str
    covered_hours: float       # RI 適用済み時間
    on_demand_hours: float     # RI 未適用（オンデマンド）時間
    total_hours: float
    coverage_pct: float        # = covered / total * 100


def fetch_ri_coverage(
    payer_profile: str,
    service: str,
    lookback_days: int = 30,
) -> list[RiCoverageRecord]:
    """
    CE GetReservationCoverage を呼び出し、
    アカウント × リージョン × インスタンスタイプ単位のカバレッジを返す。

    DescribeDBInstances を全アカウントで呼ぶ代わりに使用する。
    オンデマンド時間 > 0 のレコードが「RI でカバーされていない実行中インスタンスあり」を意味する。

    TODO: さらに詳細な実行中インスタンス一覧が必要な場合は
          Athena 経由で CUR にクエリする（案B）で実装予定。
    """
    ce_service = _CE_SERVICE_NAMES.get(service)
    if not ce_service:
        raise ValueError(f"未対応のサービスです: {service}。対応: {list(_CE_SERVICE_NAMES)}")

    start_date, end_date = _ce_time_period(lookback_days)

    session = boto3.Session(profile_name=payer_profile, region_name="us-east-1")
    ce = session.client("ce")

    try:
        resp = ce.get_reservation_coverage(
            TimePeriod={
                "Start": start_date,
                "End":   end_date,
            },
            Filter={
                "Dimensions": {
                    "Key":    "SERVICE",
                    "Values": [ce_service],
                }
            },
            GroupBy=[
                {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
                {"Type": "DIMENSION", "Key": "REGION"},
                {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
            ],
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDeniedException", "OptInRequired"):
            raise PermissionError(
                f"Cost Explorer へのアクセスが拒否されました ({code})。"
            ) from e
        raise

    records: list[RiCoverageRecord] = []
    for period in resp.get("CoveragesByTime", []):
        tp = period["TimePeriod"]
        for group in period.get("Groups", []):
            attrs    = group.get("Attributes", {})
            coverage = group.get("Coverage", {}).get("CoverageHours", {})
            account_id    = attrs.get("linkedAccount", "")
            region        = attrs.get("region", "")
            instance_type = attrs.get("instanceType", "")
            covered   = float(coverage.get("ReservedHours", 0))
            on_demand = float(coverage.get("OnDemandHours", 0))
            total     = float(coverage.get("TotalRunningHours", 0))
            pct       = float(coverage.get("CoverageHoursPercentage", 0))
            records.append(RiCoverageRecord(
                account_id    = account_id,
                region        = region,
                instance_type = instance_type,
                period_start  = tp["Start"],
                period_end    = tp["End"],
                covered_hours = covered,
                on_demand_hours = on_demand,
                total_hours   = total,
                coverage_pct  = pct,
            ))

    return records


def _parse_subscription(sub_id: str, attrs: dict[str, Any]) -> RiSubscription:
    """CE Attributes から RiSubscription を組み立てる"""

    def parse_dt(s: str) -> datetime:
        # "2026-12-24T08:55:38.000Z" → datetime (UTC)
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)

    return RiSubscription(
        subscription_id  = sub_id,
        account_id       = attrs.get("accountId", ""),
        account_name     = attrs.get("accountName", ""),
        region           = attrs.get("region", ""),
        instance_type    = attrs.get("instanceType", ""),
        platform         = attrs.get("platform", ""),
        count            = int(attrs.get("numberOfInstances", 1)),
        start_time       = parse_dt(attrs["startDateTime"]) if "startDateTime" in attrs else datetime.min.replace(tzinfo=timezone.utc),
        end_time         = parse_dt(attrs["endDateTime"])   if "endDateTime"   in attrs else datetime.max.replace(tzinfo=timezone.utc),
        status           = attrs.get("subscriptionStatus", ""),
        size_flexibility = attrs.get("sizeFlexibility", ""),
        offering_type    = attrs.get("subscriptionType", attrs.get("offeringType", "")),
    )
