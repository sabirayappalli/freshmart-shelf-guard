-- Bronze: Auto Loader incremental ingestion of every source-system drop. Append-only, schema-on-read,
-- with file lineage (_source_file, _ingested_at) for audit.

CREATE OR REFRESH STREAMING TABLE bronze.pos_sales_raw
COMMENT 'POS daily store-SKU sales extract, as landed (CSV). Includes void reversals / bad keys - cleaned in silver.'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/pos_sales', format => 'csv', header => true,
  schemaHints => 'business_date DATE, store_id STRING, sku_id STRING, units_sold INT, net_sales_usd DOUBLE, promo_flag INT');

CREATE OR REFRESH STREAMING TABLE bronze.inventory_snapshots_raw
COMMENT 'WMS end-of-day on-hand / on-order snapshot per store-SKU (Parquet).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/inventory', format => 'parquet');

CREATE OR REFRESH STREAMING TABLE bronze.deliveries_raw
COMMENT 'DC-to-store receipts (JSON lines).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/deliveries', format => 'json',
  schemaHints => 'received_date DATE, store_id STRING, sku_id STRING, units_received INT');

CREATE OR REFRESH STREAMING TABLE bronze.promotions_raw
COMMENT 'Merchandising promo calendar (JSON lines).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/promotions', format => 'json',
  schemaHints => 'start_date DATE, end_date DATE, discount_pct DOUBLE');

CREATE OR REFRESH STREAMING TABLE bronze.stores_raw
COMMENT 'Store master (CSV).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/stores', format => 'csv', header => true);

CREATE OR REFRESH STREAMING TABLE bronze.products_raw
COMMENT 'Item master (CSV).'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/products', format => 'csv', header => true,
  schemaHints => 'unit_price DOUBLE, unit_cost DOUBLE, perishable BOOLEAN, shelf_life_days INT, case_pack INT, lead_time_days INT');
