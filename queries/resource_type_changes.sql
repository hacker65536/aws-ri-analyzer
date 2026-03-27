-- CE 期間内に instance_type が変わったリソースを検出する
-- 同一 resource_id で複数の instance_type が存在するものを抽出
-- 変数: year, month, start_date, end_date, product_code, usage_type_pattern, engine_col
--
-- 例（RDS）:
--   python athena_run.py queries/resource_type_changes.sql \
--     -p product_code=AmazonRDS \
--     -p usage_type_pattern=%InstanceUsage% \
--     -p engine_col=product_database_engine
--
-- 例（ElastiCache）:
--   python athena_run.py queries/resource_type_changes.sql \
--     -p product_code=AmazonElastiCache \
--     -p usage_type_pattern=%NodeUsage% \
--     -p engine_col=product_cache_engine
SELECT
    regexp_extract(line_item_resource_id, '[^:]+$') AS resource_id,
    line_item_usage_account_id                      AS account_id,
    {{ engine_col }}                                AS engine,
    COUNT(DISTINCT product_instance_type)           AS type_count,
    ROUND(SUM(line_item_usage_amount), 2)           AS total_hours,
    array_join(
        array_agg(DISTINCT product_instance_type),
        ' → '
    )                                               AS instance_types
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = '{{ product_code }}'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND line_item_usage_type LIKE '{{ usage_type_pattern }}'
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date }} 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '{{ end_date }} 00:00:00'
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT product_instance_type) > 1
   AND SUM(line_item_usage_amount) >= 150
ORDER BY total_hours DESC
