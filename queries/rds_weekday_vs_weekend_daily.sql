-- RDS 日別コスト（平日/土日ラベル付き・タイムゾーン対応）
-- 使い方: --tz でタイムゾーンを指定すると partition_cond / start_date_utc / end_date_utc が自動注入される
--
-- 変数: partition_cond, start_date_utc, end_date_utc
--
-- 例（JST 2025-12-01〜12-28）:
--   cur-analyzer queries/rds_weekday_vs_weekend_daily.sql \
--     -p start_date=2025-12-01 -p end_date=2025-12-28 \
--     --tz JST \
--     --format csv > notes/rds_weekday_daily_2025-12.csv

SELECT
    CAST(date_add('hour', 9, line_item_usage_start_date) AS DATE)   AS usage_date_jst,
    CASE day_of_week(date_add('hour', 9, line_item_usage_start_date))
        WHEN 6 THEN '土曜'
        WHEN 7 THEN '日曜'
        ELSE '平日'
    END                                                              AS day_type,
    SUM(reservation_effective_cost)                                  AS effective_cost,
    SUM(pricing_public_on_demand_cost)                               AS od_equivalent_cost
FROM {{ database }}.{{ table }}
WHERE {{ partition_cond }}
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date_utc }}'
  AND line_item_usage_start_date <= TIMESTAMP '{{ end_date_utc }}'
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND product_instance_type != ''
  AND line_item_usage_type NOT LIKE '%Storage%'
  AND line_item_usage_type NOT LIKE '%StorageIO%'
  AND line_item_usage_type NOT LIKE '%IOPS%'
  AND line_item_usage_type NOT LIKE '%Backup%'
GROUP BY 1, 2
ORDER BY 1
