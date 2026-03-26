"""CUR (Cost and Usage Report) よく使うクエリ集

全クエリは AthenaClient.run_query() を通して実行する。
パーティション条件（year / month）は各クエリに明示的に埋め込む。

使い方:
    from ri_analyzer.fetchers.athena import AthenaClient, last_month_filter
    from ri_analyzer.fetchers.cur_queries import (
        running_rds_instances,
        ce_recommendation_factcheck,
    )
    from ri_analyzer.config import Config

    cfg = Config.load()
    client = AthenaClient(cfg.athena, payer_profile="...")
    rows = running_rds_instances(client, year=2024, month=1)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ri_analyzer.fetchers.athena import AthenaClient, partition_filter


# ---------------------------------------------------------------------------
# 1. 稼働中 RDS インスタンス一覧
# ---------------------------------------------------------------------------

def running_rds_instances(
    client: AthenaClient,
    year: int | str,
    month: int | str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """稼働中 RDS インスタンスの usage hours を account / region / instance_type 別に集計する。

    Returns
    -------
    list of dict with keys:
        line_item_usage_account_id, product_region, product_instance_type,
        product_database_engine, product_deployment_option,
        sum_usage_hours, sum_unblended_cost
    """
    pf = partition_filter(year, month)

    region_filter = ""
    if regions:
        quoted = ", ".join(f"'{r}'" for r in regions)
        region_filter = f"AND product_region IN ({quoted})"

    account_filter = ""
    if account_ids:
        quoted = ", ".join(f"'{a}'" for a in account_ids)
        account_filter = f"AND line_item_usage_account_id IN ({quoted})"

    cfg = client._cfg
    sql = f"""
SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type              AS instance_type,
    product_database_engine             AS engine,
    product_deployment_option           AS deployment,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {cfg.database}.{cfg.table}
WHERE {pf}
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage')
  AND product_instance_type != ''
  AND line_item_usage_type LIKE '%InstanceUsage%'
  {region_filter}
  {account_filter}
GROUP BY 1, 2, 3, 4, 5
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 2. 稼働中 ElastiCache インスタンス一覧
# ---------------------------------------------------------------------------

def running_elasticache_nodes(
    client: AthenaClient,
    year: int | str,
    month: int | str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """稼働中 ElastiCache ノードの usage hours を集計する。

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type, cache_engine,
        usage_hours, unblended_cost
    """
    pf = partition_filter(year, month)

    region_filter = ""
    if regions:
        quoted = ", ".join(f"'{r}'" for r in regions)
        region_filter = f"AND product_region IN ({quoted})"

    account_filter = ""
    if account_ids:
        quoted = ", ".join(f"'{a}'" for a in account_ids)
        account_filter = f"AND line_item_usage_account_id IN ({quoted})"

    cfg = client._cfg
    sql = f"""
SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type             AS instance_type,
    product_cache_engine                AS cache_engine,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {cfg.database}.{cfg.table}
WHERE {pf}
  AND line_item_product_code = 'AmazonElastiCache'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage')
  AND product_instance_type != ''
  {region_filter}
  {account_filter}
GROUP BY 1, 2, 3, 4
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 3. CE Recommendation ファクトチェック（RDS RI）
#    CE が推奨した RI タイプ・台数が、実際の CUR と合っているか確認する
# ---------------------------------------------------------------------------

def ce_recommendation_factcheck_rds(
    client: AthenaClient,
    year: int | str,
    month: int | str,
    instance_type: str,
    region: str,
    engine: str,
) -> List[Dict[str, Any]]:
    """指定した instance_type / region / engine の実績使用時間を返す。

    CE Recommendation と突き合わせることで推奨精度を検証できる。

    典型的な使い方:
        ce が "db.r5.xlarge (MySQL, ap-northeast-1) を 2 台推奨" と言っているとき、
        CUR 側で実際に何時間稼働していたかを確認する。
        720h/month * 2 台 = 1440h が基準値。

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type, engine, deployment,
        usage_hours, ri_hours, od_hours
    """
    pf = partition_filter(year, month)
    cfg = client._cfg
    sql = f"""
SELECT
    line_item_usage_account_id              AS account_id,
    product_region                          AS region,
    product_instance_type                  AS instance_type,
    product_database_engine                 AS engine,
    product_deployment_option               AS deployment,
    SUM(line_item_usage_amount)             AS usage_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'DiscountedUsage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                       AS ri_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                       AS od_hours
FROM {cfg.database}.{cfg.table}
WHERE {pf}
  AND line_item_product_code = 'AmazonRDS'
  AND product_instance_type = '{instance_type}'
  AND product_region = '{region}'
  AND product_database_engine LIKE '%{engine}%'
  AND line_item_usage_type LIKE '%InstanceUsage%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 4. RI カバレッジ詳細（RI 使用時間 vs OD 使用時間）
# ---------------------------------------------------------------------------

def ri_coverage_detail(
    client: AthenaClient,
    year: int | str,
    month: int | str,
    service: str = "AmazonRDS",
    *,
    regions: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """RI カバレッジを account / region / instance_type 別に CUR から直接計算する。

    CE API の Coverage と突き合わせるための詳細データ。

    Parameters
    ----------
    service : 'AmazonRDS' | 'AmazonElastiCache'

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type,
        ri_hours, od_hours, total_hours, coverage_pct
    """
    pf = partition_filter(year, month)

    region_filter = ""
    if regions:
        quoted = ", ".join(f"'{r}'" for r in regions)
        region_filter = f"AND product_region IN ({quoted})"

    # サービスごとのインスタンスタイプ列名
    instance_col = (
        "product_instance_type"
        if service == "AmazonRDS"
        else "product_instance_type"
    )

    cfg = client._cfg
    sql = f"""
SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    {instance_col}                      AS instance_type,
    SUM(
        CASE WHEN line_item_line_item_type = 'DiscountedUsage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS ri_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS od_hours,
    SUM(line_item_usage_amount)         AS total_hours,
    ROUND(
        100.0 * SUM(
            CASE WHEN line_item_line_item_type = 'DiscountedUsage'
                 THEN line_item_usage_amount ELSE 0.0 END
        ) / NULLIF(SUM(line_item_usage_amount), 0),
        1
    )                                   AS coverage_pct
FROM {cfg.database}.{cfg.table}
WHERE {pf}
  AND line_item_product_code = '{service}'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND {instance_col} != ''
  {region_filter}
GROUP BY 1, 2, 3
ORDER BY total_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 5. 未使用 RI 費用（RI は購入したが使われなかった時間）
# ---------------------------------------------------------------------------

def unused_ri_cost(
    client: AthenaClient,
    year: int | str,
    month: int | str,
    service: str = "AmazonRDS",
) -> List[Dict[str, Any]]:
    """未使用 RI の費用を subscription_id 別に集計する。

    line_item_line_item_type = 'RIFee' が RI 固定費（使用有無に関わらず課金）。
    この金額が大きい = 無駄な RI がある。

    Returns
    -------
    list of dict with keys:
        reservation_id, account_id, region, usage_type,
        ri_fee_cost, quantity
    """
    pf = partition_filter(year, month)
    cfg = client._cfg
    sql = f"""
SELECT
    reservation_reservation_a_r_n       AS reservation_arn,
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    line_item_usage_type                AS usage_type,
    SUM(line_item_unblended_cost)       AS ri_fee_cost,
    SUM(line_item_usage_amount)         AS quantity
FROM {cfg.database}.{cfg.table}
WHERE {pf}
  AND line_item_product_code = '{service}'
  AND line_item_line_item_type = 'RIFee'
GROUP BY 1, 2, 3, 4
ORDER BY ri_fee_cost DESC
"""
    return client.run_query(sql)
