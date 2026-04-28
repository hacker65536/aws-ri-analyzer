"""Cost Explorer から RI データを取得する

使用 API: ce:GetReservationUtilization
  - Payer アカウントから呼び出す（全アカウント集計）
  - GroupBy: SUBSCRIPTION_ID → サブスクリプション単位
  - Attributes に endDateTime / numberOfInstances 等が含まれるため
    DescribeReservedDBInstances は不要

CE は us-east-1 エンドポイントのみ。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ri_analyzer.fetchers.ce_models import (  # noqa: F401  (re-export for backward compat)
    RiSubscription,
    RiUtilizationRecord,
    RiCoverageRecord,
    RiRecommendationDetail,
    RiRecommendationGroup,
)
from ri_analyzer.service_registry import get_service


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
    svc_cfg = get_service(service)
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
                    "Values": [svc_cfg.ce_service_name],
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
                count                 = int(attrs.get("numberOfInstances", 0)),
                utilization_pct       = float(util.get("UtilizationPercentage", 0)),
                purchased_hours       = float(util.get("PurchasedHours", 0)),
                used_hours            = float(util.get("TotalActualHours", 0)),
                unused_hours          = float(util.get("UnusedHours", 0)),
                net_savings           = float(util.get("NetRISavings", 0)),
                on_demand_cost_if_used= float(util.get("OnDemandCostOfRIHoursUsed", 0)),
                amortized_fee         = float(util.get("TotalAmortizedFee", 0)),
            ))

    return list(seen_subs.values()), util_records


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
    svc_cfg = get_service(service)
    start_date, end_date = _ce_time_period(lookback_days)

    session = boto3.Session(profile_name=payer_profile, region_name="us-east-1")
    ce = session.client("ce")

    group_by = [
        {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
        {"Type": "DIMENSION", "Key": "REGION"},
        {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
    ]
    if svc_cfg.engine_dimension:
        group_by.append({"Type": "DIMENSION", "Key": svc_cfg.engine_dimension})

    try:
        resp = ce.get_reservation_coverage(
            TimePeriod={
                "Start": start_date,
                "End":   end_date,
            },
            Filter={
                "Dimensions": {
                    "Key":    "SERVICE",
                    "Values": [svc_cfg.ce_service_name],
                }
            },
            GroupBy=group_by,
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDeniedException", "OptInRequired"):
            raise PermissionError(
                f"Cost Explorer へのアクセスが拒否されました ({code})。"
            ) from e
        raise

    records: list[RiCoverageRecord] = []
    engine_attr = svc_cfg.engine_attr
    for period in resp.get("CoveragesByTime", []):
        tp = period["TimePeriod"]
        for group in period.get("Groups", []):
            attrs    = group.get("Attributes", {})
            keys     = group.get("Keys", [])
            coverage = group.get("Coverage", {}).get("CoverageHours", {})
            account_id    = attrs.get("linkedAccount", "")
            region        = attrs.get("region", "")
            instance_type = attrs.get("instanceType", "")
            # engine_attr がある場合は Attributes から取得、なければ Keys[3] をフォールバック
            if engine_attr:
                platform = attrs.get(engine_attr) or (keys[3] if len(keys) > 3 else "")
            else:
                platform = ""
            covered   = float(coverage.get("ReservedHours", 0))
            on_demand = float(coverage.get("OnDemandHours", 0))
            total     = float(coverage.get("TotalRunningHours", 0))
            pct       = float(coverage.get("CoverageHoursPercentage", 0))
            records.append(RiCoverageRecord(
                account_id    = account_id,
                region        = region,
                instance_type = instance_type,
                platform      = platform,
                period_start  = tp["Start"],
                period_end    = tp["End"],
                covered_hours = covered,
                on_demand_hours = on_demand,
                total_hours   = total,
                coverage_pct  = pct,
            ))

    return records


def fetch_ri_coverage_range(
    payer_profile: str | None,
    service: str,
    start_date: str,
    end_date: str,
) -> list[RiCoverageRecord]:
    """期間を直接指定して CE GetReservationCoverage を呼ぶ。

    Parameters
    ----------
    start_date : "YYYY-MM-DD"（inclusive）
    end_date   : "YYYY-MM-DD"（exclusive、CE の仕様に合わせて翌日を渡す）

    CUR の line_item_usage_start_date と同じ期間を渡すことで突き合わせが可能。
    """

    svc_cfg = get_service(service)

    session = boto3.Session(profile_name=payer_profile, region_name="us-east-1")
    ce = session.client("ce")

    group_by = [
        {"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"},
        {"Type": "DIMENSION", "Key": "REGION"},
        {"Type": "DIMENSION", "Key": "INSTANCE_TYPE"},
    ]
    if svc_cfg.engine_dimension:
        group_by.append({"Type": "DIMENSION", "Key": svc_cfg.engine_dimension})

    try:
        resp = ce.get_reservation_coverage(
            TimePeriod={"Start": start_date, "End": end_date},
            Filter={
                "Dimensions": {
                    "Key":    "SERVICE",
                    "Values": [svc_cfg.ce_service_name],
                }
            },
            GroupBy=group_by,
        )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDeniedException", "OptInRequired"):
            raise PermissionError(
                f"Cost Explorer へのアクセスが拒否されました ({code})。"
            ) from e
        raise

    records: list[RiCoverageRecord] = []
    engine_attr = svc_cfg.engine_attr
    for period in resp.get("CoveragesByTime", []):
        tp = period["TimePeriod"]
        for group in period.get("Groups", []):
            attrs    = group.get("Attributes", {})
            keys     = group.get("Keys", [])
            coverage = group.get("Coverage", {}).get("CoverageHours", {})
            covered   = float(coverage.get("ReservedHours", 0))
            on_demand = float(coverage.get("OnDemandHours", 0))
            total     = float(coverage.get("TotalRunningHours", 0))
            pct       = float(coverage.get("CoverageHoursPercentage", 0))
            if engine_attr:
                platform = attrs.get(engine_attr) or (keys[3] if len(keys) > 3 else "")
            else:
                platform = ""
            records.append(RiCoverageRecord(
                account_id      = attrs.get("linkedAccount", ""),
                region          = attrs.get("region", ""),
                instance_type   = attrs.get("instanceType", ""),
                platform        = platform,
                period_start    = tp["Start"],
                period_end      = tp["End"],
                covered_hours   = covered,
                on_demand_hours = on_demand,
                total_hours     = total,
                coverage_pct    = pct,
            ))

    return records


# ──────────────────────────────────────────────
# Recommendations
# ──────────────────────────────────────────────

_CE_LOOKBACK_MAP: dict[int, str] = {7: "SEVEN_DAYS", 30: "THIRTY_DAYS", 60: "SIXTY_DAYS"}

def _parse_instance_detail(service: str, instance_details: dict[str, Any]) -> tuple[str, str, str]:
    """InstanceDetails から (instance_type, region, platform) を返す"""
    from ri_analyzer.service_registry import SERVICES
    key = SERVICES[service].instance_detail_key if service in SERVICES else ""
    d = instance_details.get(key, {})
    if service == "rds":
        instance_type = d.get("InstanceType", "")
        region        = d.get("Region", "")
        engine        = d.get("DatabaseEngine", "")
        deploy        = d.get("DeploymentOption", "")
        platform      = f"{engine} {deploy}".strip() if deploy else engine
    elif service == "elasticache":
        instance_type = d.get("NodeType", "")
        region        = d.get("Region", "")
        platform      = d.get("ProductDescription", "")
    elif service == "opensearch":
        inst_class    = d.get("InstanceClass", "")
        inst_size     = d.get("InstanceSize", "")
        instance_type = f"{inst_class}.{inst_size}.search" if inst_class and inst_size else d.get("InstanceType", "")
        region        = d.get("Region", "")
        platform      = ""
    else:
        instance_type = d.get("InstanceType", d.get("NodeType", ""))
        region        = d.get("Region", "")
        platform      = ""
    return instance_type, region, platform


def fetch_ri_recommendations(
    payer_profile: str,
    service: str,
    term: str = "ONE_YEAR",
    payment_option: str = "ALL_UPFRONT",
    lookback_days: int = 30,
) -> list[RiRecommendationGroup]:
    """
    CE GetReservationPurchaseRecommendation を呼び出し、
    RI 購入推奨を返す。

    Parameters
    ----------
    payer_profile  : Payer アカウントのプロファイル名
    service        : 対象サービスキー ("rds" / "elasticache")
    term           : "ONE_YEAR" / "THREE_YEARS"
    payment_option : "ALL_UPFRONT" / "PARTIAL_UPFRONT" / "NO_UPFRONT"
    lookback_days  : 7 / 30 / 60（CE が受け付ける値のみ）
    """
    svc_cfg = get_service(service)
    lookback = _CE_LOOKBACK_MAP.get(lookback_days, "THIRTY_DAYS")

    session = boto3.Session(profile_name=payer_profile, region_name="us-east-1")
    ce = session.client("ce")

    groups: list[RiRecommendationGroup] = []
    next_token: str | None = None

    while True:
        kwargs: dict[str, Any] = dict(
            Service=svc_cfg.ce_service_name,
            TermInYears=term,
            PaymentOption=payment_option,
            LookbackPeriodInDays=lookback,
            AccountScope="PAYER",
        )
        if next_token:
            kwargs["NextPageToken"] = next_token

        try:
            resp = ce.get_reservation_purchase_recommendation(**kwargs)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("AccessDeniedException", "OptInRequired"):
                raise PermissionError(
                    f"Cost Explorer へのアクセスが拒否されました ({code})。"
                    "Payer アカウントのプロファイルと IAM 権限を確認してください。"
                ) from e
            raise

        for rec in resp.get("Recommendations", []):
            summary = rec.get("RecommendationSummary", {})
            details: list[RiRecommendationDetail] = []
            for d in rec.get("RecommendationDetails", []):
                itype, region, platform = _parse_instance_detail(
                    service, d.get("InstanceDetails", {})
                )
                details.append(RiRecommendationDetail(
                    instance_type             = itype,
                    region                    = region,
                    platform                  = platform,
                    count                     = int(float(d.get("RecommendedNumberOfInstancesToPurchase", 0))),
                    normalized_units          = float(d.get("RecommendedNormalizedUnitsToPurchase", 0)),
                    upfront_cost              = float(d.get("UpfrontCost", 0)),
                    estimated_monthly_savings = float(d.get("EstimatedMonthlySavingsAmount", 0)),
                    estimated_savings_pct     = float(d.get("EstimatedMonthlySavingsPercentage", 0)),
                    breakeven_months          = float(d.get("EstimatedBreakEvenInMonths", 0)),
                    avg_utilization           = float(d.get("AverageUtilization", 0)),
                ))
            groups.append(RiRecommendationGroup(
                service               = service,
                term                  = rec.get("Term", term),
                payment_option        = rec.get("PaymentOption", payment_option),
                details               = details,
                total_monthly_savings = float(summary.get("TotalEstimatedMonthlySavingsAmount", 0)),
                total_savings_pct     = float(summary.get("TotalEstimatedMonthlySavingsPercentage", 0)),
                currency              = summary.get("CurrencyCode", "USD"),
            ))

        next_token = resp.get("NextPageToken")
        if not next_token:
            break

    return groups


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
        avg_od_rate      = float(attrs.get("averageOnDemandHourlyRate") or 0.0),
    )
