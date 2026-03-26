-- テンプレート: 稼働中 RDS インスタンス（account / region / type 別 usage hours）
-- 変数: year, month, [region], [account_id]
--
-- 例:
--   python athena_run.py rds_instances -p year=2026 -p month=3
--   python athena_run.py rds_instances -p year=2026 -p month=3 -p region=ap-northeast-1

SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
    product_database_engine             AS engine,
    product_deployment_option           AS deployment,
    SUM(line_item_usage_amount)         AS usage_hours,
    SUM(line_item_unblended_cost)       AS unblended_cost
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND product_instance_type != ''
  AND line_item_usage_type LIKE '%InstanceUsage%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY usage_hours DESC
