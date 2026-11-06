-- Silver: validated, typed, conformed. Expectations quarantine bad POS rows (void reversals, missing keys)
-- and fail the update if inventory ever goes negative (a systemic WMS defect, not a row-level one).

CREATE OR REFRESH STREAMING TABLE silver.pos_sales (
  CONSTRAINT valid_store EXPECT (store_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sku EXPECT (sku_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_units EXPECT (units_sold >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (business_date IS NOT NULL)
)
COMMENT 'Clean POS sales, one row per store-SKU-day with units > 0.'
CLUSTER BY (store_id, business_date)
AS SELECT business_date, store_id, sku_id, units_sold, net_sales_usd, CAST(promo_flag AS BOOLEAN) AS promo_flag, _source_file
FROM STREAM(bronze.pos_sales_raw);

CREATE OR REFRESH STREAMING TABLE silver.inventory_snapshots (
  CONSTRAINT keys_present EXPECT (store_id IS NOT NULL AND sku_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_on_hand EXPECT (on_hand_units >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'End-of-day inventory position per store-SKU, incl. open-PO units due to arrive in 1, 2 and 3 days.'
CLUSTER BY (store_id, snapshot_date)
AS SELECT CAST(snapshot_date AS DATE) AS snapshot_date, store_id, sku_id,
          CAST(on_hand_units AS INT) AS on_hand_units, CAST(on_order_units AS INT) AS on_order_units,
          CAST(inbound_d1 AS INT) AS inbound_d1, CAST(inbound_d2 AS INT) AS inbound_d2, CAST(inbound_d3 AS INT) AS inbound_d3, _source_file
FROM STREAM(bronze.inventory_snapshots_raw);

CREATE OR REFRESH STREAMING TABLE silver.deliveries (
  CONSTRAINT positive_receipt EXPECT (units_received > 0) ON VIOLATION DROP ROW
)
COMMENT 'Store receipts from DC.'
AS SELECT received_date, store_id, sku_id, units_received FROM STREAM(bronze.deliveries_raw);

CREATE OR REFRESH MATERIALIZED VIEW silver.stores
COMMENT 'Store dimension.'
AS SELECT store_id, store_name, region, store_format, city, state, CAST(opened_year AS INT) AS opened_year, CAST(sqft AS INT) AS sqft
FROM bronze.stores_raw;

CREATE OR REFRESH MATERIALIZED VIEW silver.products (
  sku_id STRING NOT NULL, sku_name STRING, category STRING, subcategory STRING, unit_price DOUBLE,
  unit_cost DOUBLE MASK ${catalog}.gov.mask_unit_cost COMMENT 'Supplier cost - masked for non-finance users',
  perishable BOOLEAN, shelf_life_days INT, case_pack INT, supplier_id STRING, lead_time_days INT,
  CONSTRAINT price_above_cost EXPECT (unit_price > unit_cost)
)
COMMENT 'Item dimension. unit_cost is margin-sensitive and masked for non-finance users (see gov.mask_unit_cost).'
AS SELECT sku_id, sku_name, category, subcategory, unit_price, unit_cost, perishable, shelf_life_days, case_pack, supplier_id, lead_time_days
FROM bronze.products_raw;

CREATE OR REFRESH MATERIALIZED VIEW silver.promo_calendar
COMMENT 'Promo calendar exploded to one row per SKU x region x day (region ALL expanded to every region).'
AS SELECT p.sku_id, r.region, d AS promo_date, max(p.discount_pct) AS discount_pct, max_by(p.promo_type, p.discount_pct) AS promo_type
FROM bronze.promotions_raw p
  CROSS JOIN (SELECT DISTINCT region FROM bronze.stores_raw) r
  LATERAL VIEW explode(sequence(p.start_date, p.end_date)) days AS d
WHERE p.region = 'ALL' OR p.region = r.region
GROUP BY p.sku_id, r.region, d;
