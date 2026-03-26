-- テンプレート: RDS インスタンス リソース ID 一覧（OD / RI 内訳付き）
-- 指定した instance_type prefix / engine でフィルタし、resource_id ごとに
-- OD コストと RI 実効コスト（reservation_effective_cost）を集計する
-- 変数: {{ year }}, {{ month }}, {{ instance_type_prefix }}, {{ engine }}
--
-- 例:
--   python athena_run.py rds_resource_ids \
--     -p year=2026 -p month=3 \
--     -p instance_type_prefix=db.r8g \
--     -p engine=Aurora MySQL
--
-- コスト列の意味:
--   od_cost          : OD 利用分の実コスト（line_item_unblended_cost）
--   ri_effective_cost: RI 利用分の償却済み実効コスト（reservation_effective_cost）

SELECT
    regexp_extract(line_item_resource_id, '[^:]+$')
                                    AS resource_id,
    line_item_usage_account_id      AS account_id,
    product_region                  AS region,
    product_instance_type           AS instance_type,
    product_database_engine         AS engine,
    product_deployment_option       AS deployment,
    ROUND(SUM(line_item_usage_amount), 1)
                                    AS total_hours,
    ROUND(SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_usage_amount ELSE 0.0 END
    ), 1)                           AS od_hours,
    ROUND(SUM(
        CASE WHEN line_item_line_item_type = 'DiscountedUsage'
             THEN line_item_usage_amount ELSE 0.0 END
    ), 1)                           AS ri_hours,
    ROUND(SUM(
        CASE WHEN line_item_line_item_type = 'Usage'
             THEN line_item_unblended_cost ELSE 0.0 END
    ), 2)                           AS od_cost,
    ROUND(SUM(
        CASE WHEN line_item_line_item_type = 'DiscountedUsage'
             THEN reservation_effective_cost ELSE 0.0 END
    ), 2)                           AS ri_effective_cost
FROM {{ database }}.{{ table }}
WHERE year = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_product_code = 'AmazonRDS'
  AND line_item_line_item_type IN ('Usage', 'DiscountedUsage')
  AND line_item_usage_type LIKE '%InstanceUsage%'
  AND product_instance_type LIKE '{{ instance_type_prefix }}.%'
  AND product_database_engine LIKE '%{{ engine }}%'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY total_hours DESC
