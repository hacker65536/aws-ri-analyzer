-- テンプレート: 未使用 RI 費用
-- RIFee 行 = RI の固定費（使用有無に関わらず課金）
-- この金額が大きい subscription = 無駄な RI
-- 変数: {{ year }}, {{ month }}, {{ service }} (AmazonRDS | AmazonElastiCache)
--
-- 例:
--   python athena_run.py unused_ri -p year=2026 -p month=3 -p service=AmazonRDS

SELECT
    reservation_reservation_a_r_n       AS reservation_arn,
    line_item_usage_account_id          AS account_id,
    product_region                      AS region,
    line_item_usage_type                AS usage_type,
    SUM(line_item_unblended_cost)       AS ri_fee_cost,
    SUM(line_item_usage_amount)         AS quantity
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = '{{ service }}'
  AND line_item_line_item_type = 'RIFee'
GROUP BY 1, 2, 3, 4
ORDER BY ri_fee_cost DESC
