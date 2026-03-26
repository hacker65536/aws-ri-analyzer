-- テンプレート: 稼働中 ElastiCache ノード（account / region / type 別 usage hours）
-- 変数: {{ year }}, {{ month }}
--
-- 例:
--   python athena_run.py elasticache_nodes -p year=2026 -p month=3

SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
    product_cache_engine                AS cache_engine,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonElastiCache'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND product_instance_type != ''
GROUP BY 1, 2, 3, 4
ORDER BY usage_hours DESC
