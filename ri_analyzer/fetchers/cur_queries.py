"""CUR (Cost and Usage Report) よく使うクエリ集

全クエリは AthenaClient.run_query() を通して実行する。
日付範囲は CE API と同じ period (start_date / end_date) で指定する。
パーティション条件（year / month）は date_range_filter() が自動生成する。

使い方:
    from ri_analyzer.fetchers.athena import AthenaClient, date_range_filter
    from ri_analyzer.fetchers.cur_queries import rds_instance_detail
    from ri_analyzer.config import Config

    cfg = Config.load()
    client = AthenaClient(cfg.athena, payer_profile="...")
    rows = rds_instance_detail(client, start_date="2026-03-18", end_date="2026-03-25")
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ri_analyzer.fetchers.athena import AthenaClient, date_range_filter


# ---------------------------------------------------------------------------
# 1. 稼働中 RDS インスタンス一覧（instance_type 集計）
# ---------------------------------------------------------------------------

def running_rds_instances(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """稼働中 RDS インスタンスの usage hours を account / region / instance_type 別に集計する。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type, engine, deployment,
        usage_hours, unblended_cost
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

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
    product_instance_type               AS instance_type,
    product_database_engine             AS engine,
    product_deployment_option           AS deployment,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {cfg.database}.{cfg.table}
WHERE {partition_cond}
  AND {date_cond}
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
# 2. 稼働中 ElastiCache インスタンス一覧（instance_type 集計）
# ---------------------------------------------------------------------------

def running_elasticache_nodes(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """稼働中 ElastiCache ノードの usage hours を集計する。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type, cache_engine,
        usage_hours, unblended_cost
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

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
    product_instance_type               AS instance_type,
    product_cache_engine                AS cache_engine,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {cfg.database}.{cfg.table}
WHERE {partition_cond}
  AND {date_cond}
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
    start_date: str,
    end_date: str,
    instance_type: str,
    region: str,
    engine: str,
) -> List[Dict[str, Any]]:
    """指定した instance_type / region / engine の実績使用時間を返す。

    CE Recommendation と突き合わせることで推奨精度を検証できる。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type, engine, deployment,
        usage_hours, ri_hours, od_hours
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)
    cfg = client._cfg
    sql = f"""
SELECT
    line_item_usage_account_id              AS account_id,
    product_region                          AS region,
    product_instance_type                   AS instance_type,
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
WHERE {partition_cond}
  AND {date_cond}
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
# 4. resource_id 単位のインスタンス稼働実績（RI 購入精査の基本機能）
# ---------------------------------------------------------------------------

def rds_instance_detail(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """RDS インスタンスを resource_id 単位で集計し、RI/OD の内訳を返す。

    CE Coverage では個体識別ができないため、CUR の line_item_resource_id を使って
    各インスタンスが実際に何時間稼働したかを取得する。
    短命なインスタンス（usage_hours が少ない）を RI 購入候補から除外する判断に使う。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        resource_id, account_id, region, instance_type, engine, deployment,
        usage_hours, ri_hours, od_hours
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

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
    line_item_resource_id               AS resource_id,
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
    product_database_engine             AS engine,
    product_deployment_option           AS deployment,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(
        CASE WHEN line_item_line_item_type IN ('DiscountedUsage', 'SavingsPlanCoveredUsage')
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS ri_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS od_hours
FROM {cfg.database}.{cfg.table}
WHERE {partition_cond}
  AND {date_cond}
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage')
  AND product_instance_type != ''
  AND line_item_usage_type LIKE '%InstanceUsage%'
  {region_filter}
  {account_filter}
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


def elasticache_node_detail(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """ElastiCache ノードを resource_id 単位で集計し、RI/OD の内訳を返す。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        resource_id, account_id, region, instance_type, engine,
        usage_hours, ri_hours, od_hours
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

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
    line_item_resource_id               AS resource_id,
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
    product_cache_engine                AS engine,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(
        CASE WHEN line_item_line_item_type IN ('DiscountedUsage', 'SavingsPlanCoveredUsage')
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS ri_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS od_hours
FROM {cfg.database}.{cfg.table}
WHERE {partition_cond}
  AND {date_cond}
  AND line_item_product_code = 'AmazonElastiCache'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage')
  AND product_instance_type != ''
  {region_filter}
  {account_filter}
GROUP BY 1, 2, 3, 4, 5
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 5a. 稼働中 OpenSearch ドメイン一覧（instance_type 集計）
# ---------------------------------------------------------------------------

def running_opensearch_domains(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """稼働中 OpenSearch インスタンスの usage hours を account / region / instance_type 別に集計する。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type, usage_hours, unblended_cost
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

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
    product_instance_type               AS instance_type,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {cfg.database}.{cfg.table}
WHERE {partition_cond}
  AND {date_cond}
  AND line_item_product_code = 'AmazonES'
  AND line_item_line_item_type IN ('Usage', 'SavingsPlanCoveredUsage')
  AND product_instance_type LIKE '%.search'
  {region_filter}
  {account_filter}
GROUP BY 1, 2, 3
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


def opensearch_domain_detail(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    *,
    regions: Optional[List[str]] = None,
    account_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """OpenSearch インスタンスを resource_id 単位で集計し、RI/OD の内訳を返す。

    CE Coverage では個体識別ができないため、CUR の line_item_resource_id を使って
    各ドメインが実際に何時間稼働したかを取得する。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）

    Returns
    -------
    list of dict with keys:
        resource_id, account_id, region, instance_type,
        usage_hours, ri_hours, od_hours
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

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
    line_item_resource_id               AS resource_id,
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'DiscountedUsage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS ri_hours,
    SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    )                                   AS od_hours
FROM {cfg.database}.{cfg.table}
WHERE {partition_cond}
  AND {date_cond}
  AND line_item_product_code = 'AmazonES'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND product_instance_type LIKE '%.search'
  {region_filter}
  {account_filter}
GROUP BY 1, 2, 3, 4
ORDER BY usage_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 6. RI カバレッジ詳細（RI 使用時間 vs OD 使用時間）
# ---------------------------------------------------------------------------

def ri_coverage_detail(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    service: str = "AmazonRDS",
    *,
    regions: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """RI カバレッジを account / region / instance_type 別に CUR から直接計算する。

    CE API の Coverage と突き合わせるための詳細データ。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（inclusive）
    end_date   : 'YYYY-MM-DD' 形式（exclusive）
    service    : 'AmazonRDS' | 'AmazonElastiCache'

    Returns
    -------
    list of dict with keys:
        account_id, region, instance_type,
        ri_hours, od_hours, total_hours, coverage_pct
    """
    partition_cond, date_cond = date_range_filter(start_date, end_date)

    region_filter = ""
    if regions:
        quoted = ", ".join(f"'{r}'" for r in regions)
        region_filter = f"AND product_region IN ({quoted})"

    instance_col = "product_instance_type"

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
WHERE {partition_cond}
  AND {date_cond}
  AND line_item_product_code = '{service}'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND {instance_col} != ''
  {region_filter}
GROUP BY 1, 2, 3
ORDER BY total_hours DESC
"""
    return client.run_query(sql)


# ---------------------------------------------------------------------------
# 7. 未使用 RI 費用（RI は購入したが使われなかった時間）
#    RIFee は月次固定費のため、パーティション（年月）のみでフィルタする
# ---------------------------------------------------------------------------

def unused_ri_cost(
    client: AthenaClient,
    start_date: str,
    end_date: str,
    service: str = "AmazonRDS",
) -> List[Dict[str, Any]]:
    """未使用 RI の費用を subscription_id 別に集計する。

    line_item_line_item_type = 'RIFee' が RI 固定費（使用有無に関わらず課金）。
    RIFee は月次レコードのため、start_date/end_date から対象月のパーティションを
    決定し、月全体のデータを返す（日付範囲フィルタは適用しない）。

    Parameters
    ----------
    start_date : 'YYYY-MM-DD' 形式（対象月の特定に使用）
    end_date   : 'YYYY-MM-DD' 形式（対象月の特定に使用）

    Returns
    -------
    list of dict with keys:
        reservation_arn, account_id, region, usage_type,
        ri_fee_cost, quantity
    """
    # RIFee は月次固定費なので partition のみ（日付範囲フィルタなし）
    partition_cond, _ = date_range_filter(start_date, end_date)
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
WHERE {partition_cond}
  AND line_item_product_code = '{service}'
  AND line_item_line_item_type = 'RIFee'
GROUP BY 1, 2, 3, 4
ORDER BY ri_fee_cost DESC
"""
    return client.run_query(sql)
