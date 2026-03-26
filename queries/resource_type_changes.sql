-- CE 期間内に instance_type が変わったリソースを検出する
-- 同一 resource_id で複数の instance_type が存在するものを抽出
SELECT
    regexp_extract(line_item_resource_id, '[^:]+$') AS resource_id,
    line_item_usage_account_id                      AS account_id,
    product_database_engine                         AS engine,
    COUNT(DISTINCT product_instance_type)           AS type_count,
    ROUND(SUM(line_item_usage_amount), 2)           AS total_hours,
    array_join(
        array_agg(DISTINCT product_instance_type),
        ' → '
    )                                               AS instance_types
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND line_item_usage_type LIKE '%InstanceUsage%'
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date }} 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '{{ end_date }} 00:00:00'
GROUP BY 1, 2, 3
HAVING COUNT(DISTINCT product_instance_type) > 1
   AND SUM(line_item_usage_amount) >= 150
ORDER BY total_hours DESC
