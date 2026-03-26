-- テンプレート: RI カバレッジ詳細（CUR から直接計算）
-- CE API の Coverage と突き合わせ用
-- 変数: {{ year }}, {{ month }}, {{ service }} (AmazonRDS | AmazonElastiCache)
--
-- 例:
--   python athena_run.py ri_coverage -p year=2026 -p month=3 -p service=AmazonRDS

SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
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
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = '{{ service }}'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND product_instance_type != ''
GROUP BY 1, 2, 3
ORDER BY total_hours DESC
