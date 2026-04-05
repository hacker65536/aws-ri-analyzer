-- RDS 平日/土日サマリー（1日あたり平均コスト付き・タイムゾーン対応）
-- 使い方: --tz でタイムゾーンを指定すると partition_cond / start_date_utc / end_date_utc が自動注入される
--
-- 変数: partition_cond, start_date_utc, end_date_utc
--
-- 例（JST 2025-12-01〜12-28）:
--   cur-analyzer queries/rds_weekday_vs_weekend_summary.sql \
--     -p start_date=2025-12-01 -p end_date=2025-12-28 \
--     --tz JST

SELECT
    CASE day_of_week(date_add('hour', 9, line_item_usage_start_date))
        WHEN 6 THEN '土日'
        WHEN 7 THEN '土日'
        ELSE '平日'
    END                                                              AS day_type,
    COUNT(DISTINCT CAST(date_add('hour', 9, line_item_usage_start_date) AS DATE))
                                                                     AS day_count,
    SUM(reservation_effective_cost)                                  AS total_effective_cost,
    SUM(pricing_public_on_demand_cost)                               AS total_od_equivalent_cost,
    SUM(reservation_effective_cost)
      / COUNT(DISTINCT CAST(date_add('hour', 9, line_item_usage_start_date) AS DATE))
                                                                     AS avg_cost_per_day
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
GROUP BY 1
ORDER BY 1
