-- テンプレート: RI カバレッジ詳細（指定期間、CE API 突き合わせ用）
-- year/month パーティション + line_item_usage_start_date で CE と同じ期間に絞る
-- 変数: {{ year }}, {{ month }}, {{ service }}, {{ start_date }}, {{ end_date }}
--
-- 例:
--   python athena_run.py ri_coverage_period \
--     -p year=2026 -p month=3 -p service=AmazonRDS \
--     -p start_date=2026-03-17 -p end_date=2026-03-24

SELECT
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    product_instance_type               AS instance_type,
    product_database_engine             AS engine,
    ROUND(SUM(
        CASE WHEN line_item_line_item_type = 'DiscountedUsage'
             THEN line_item_usage_amount ELSE 0.0 END
    ), 2)                               AS ri_hours,
    ROUND(SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    ), 2)                               AS od_hours,
    ROUND(SUM(line_item_usage_amount), 2)
                                        AS total_hours,
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
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date }} 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '{{ end_date }} 00:00:00'
GROUP BY 1, 2, 3, 4
ORDER BY total_hours DESC
