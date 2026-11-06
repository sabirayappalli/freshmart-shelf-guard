-- Gold: the business facts. Inventory snapshot is the complete store-SKU-day grid (POS only reports sold items).
-- A day is OUT OF STOCK when end-of-day on-hand is 0. Lost sales are ESTIMATED from the trailing in-stock run-rate
-- (true demand is unobservable in POS - censoring), which makes this a conservative estimate.

CREATE OR REFRESH MATERIALIZED VIEW gold.store_sku_daily
COMMENT 'One row per store x SKU x day: sales, inventory, deliveries, promo, out-of-stock flag and estimated lost sales.'
CLUSTER BY (business_date, store_id)
AS WITH base AS (
  SELECT i.snapshot_date AS business_date, i.store_id, s.store_name, s.region, s.store_format,
         i.sku_id, p.sku_name, p.category, p.subcategory, p.perishable, p.unit_price, p.case_pack, p.lead_time_days,
         coalesce(ps.units_sold, 0) AS units_sold, coalesce(ps.net_sales_usd, 0) AS net_sales_usd,
         i.on_hand_units, i.on_order_units, i.inbound_d1, i.inbound_d2, i.inbound_d3, coalesce(dl.units_received, 0) AS units_received,
         coalesce(pc.discount_pct, 0) AS promo_discount_pct, pc.promo_type, pc.discount_pct IS NOT NULL AS on_promo,
         i.on_hand_units = 0 AS is_oos
  FROM silver.inventory_snapshots i
  JOIN silver.stores s USING (store_id)
  JOIN silver.products p USING (sku_id)
  LEFT JOIN silver.pos_sales ps ON ps.store_id = i.store_id AND ps.sku_id = i.sku_id AND ps.business_date = i.snapshot_date
  LEFT JOIN silver.deliveries dl ON dl.store_id = i.store_id AND dl.sku_id = i.sku_id AND dl.received_date = i.snapshot_date
  LEFT JOIN silver.promo_calendar pc ON pc.sku_id = i.sku_id AND pc.region = s.region AND pc.promo_date = i.snapshot_date
), runrate AS (
  SELECT *, avg(CASE WHEN NOT is_oos THEN units_sold END)
              OVER (PARTITION BY store_id, sku_id ORDER BY business_date ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING) AS instock_run_rate_28d
  FROM base
)
SELECT *,
  CASE WHEN is_oos THEN greatest(0, round(coalesce(instock_run_rate_28d, 0) * (1 + 1.5 * promo_discount_pct) - units_sold, 1)) ELSE 0 END AS est_lost_units,
  CASE WHEN is_oos THEN greatest(0, round(coalesce(instock_run_rate_28d, 0) * (1 + 1.5 * promo_discount_pct) - units_sold, 1)) * unit_price * (1 - promo_discount_pct) ELSE 0 END AS est_lost_sales_usd
FROM runrate;

CREATE OR REFRESH MATERIALIZED VIEW gold.store_daily_kpis
COMMENT 'Store x day KPIs: net sales, estimated lost sales, out-of-stock rate and on-shelf availability (OSA).'
AS SELECT business_date, store_id, store_name, region, store_format,
  round(sum(net_sales_usd), 2) AS net_sales_usd,
  round(sum(est_lost_sales_usd), 2) AS est_lost_sales_usd,
  count(*) AS skus_tracked,
  sum(CAST(is_oos AS INT)) AS skus_oos,
  round(avg(CAST(is_oos AS INT)), 4) AS oos_rate,
  round(1 - avg(CAST(is_oos AS INT)), 4) AS on_shelf_availability,
  round(avg(CASE WHEN on_promo THEN CAST(is_oos AS INT) END), 4) AS promo_oos_rate
FROM gold.store_sku_daily
GROUP BY ALL;
