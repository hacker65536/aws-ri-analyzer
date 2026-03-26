-- resource_id の product_database_engine 別稼働時間（エンジン表記ゆれ確認用）
SELECT
    product_database_engine                 AS engine,
    product_instance_type                   AS instance_type,
    ROUND(SUM(line_item_usage_amount), 2)   AS hours
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND line_item_usage_type LIKE '%InstanceUsage%'
  AND regexp_extract(line_item_resource_id, '[^:]+$') = '{{ resource_id }}'
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date }} 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '{{ end_date }} 00:00:00'
GROUP BY 1, 2
ORDER BY hours DESC
