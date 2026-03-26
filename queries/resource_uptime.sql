-- resource_id 別の日別稼働時間（CE 期間内）
-- 24h 未満の日 = 一部停止、欠損日 = 終日停止
SELECT
    DATE(line_item_usage_start_date)        AS usage_date,
    ROUND(SUM(line_item_usage_amount), 2)   AS hours,
    ROUND(24.0 - SUM(line_item_usage_amount), 2) AS missing_hours
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND line_item_usage_type LIKE '%InstanceUsage%'
  AND regexp_extract(line_item_resource_id, '[^:]+$') = '{{ resource_id }}'
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date }} 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '{{ end_date }} 00:00:00'
GROUP BY 1
ORDER BY 1
