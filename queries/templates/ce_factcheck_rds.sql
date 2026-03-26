-- テンプレート: CE Recommendation ファクトチェック（RDS）
-- CE が推奨した instance_type / region / engine の実績使用時間を CUR で確認する
-- 変数: {{ year }}, {{ month }}, {{ instance_type }}, {{ region }}, {{ engine }}
--
-- 例:
--   python athena_run.py ce_factcheck_rds \
--     -p year=2026 -p month=3 \
--     -p instance_type=db.r6g.large \
--     -p region=ap-northeast-1 \
--     -p engine=Aurora MySQL
--
-- 解釈:
--   usage_hours ÷ 720 ≒ 月間の平均稼働台数
--   ri_hours が多い = RI でカバー済み
--   od_hours が多い = OD 稼働 → RI 購入候補

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
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonRDS'
  AND product_instance_type = '{{ instance_type }}'
  AND product_region = '{{ region }}'
  AND product_database_engine LIKE '%{{ engine }}%'
  AND line_item_usage_type LIKE '%InstanceUsage%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY usage_hours DESC
