# Lakeflow pipeline evidence — `33b3763a-8e86-4849-9c1e-141f8273552d`

Source: pipeline event log via `event_log()` (SQL). Most recent updates first.

## Update `3248fe71-146b-465b-abb0-07cb8d95bf4f` — **COMPLETED** (2026-11-06T01:09:33.765Z → 2026-11-06T01:11:01.679Z)

### Rows per flow

| flow | status | rows_written | rows_dropped |
|---|---|---|---|
| sabir_freshmart.bronze.deliveries_raw |  | 4739 | 0 |
| sabir_freshmart.bronze.inventory_snapshots_raw |  | 16000 | 0 |
| sabir_freshmart.bronze.pos_sales_raw |  | 15368 | 0 |
| sabir_freshmart.bronze.products_raw | COMPLETED | 0 | 0 |
| sabir_freshmart.bronze.promotions_raw | COMPLETED | 0 | 0 |
| sabir_freshmart.bronze.stores_raw | COMPLETED | 0 | 0 |
| sabir_freshmart.gold.store_daily_kpis |  | 29280 | 0 |
| sabir_freshmart.gold.store_sku_daily |  | 5856000 | 0 |
| sabir_freshmart.silver.deliveries |  | 4739 | 0 |
| sabir_freshmart.silver.inventory_snapshots |  | 16000 | 0 |
| sabir_freshmart.silver.pos_sales |  | 15368 | 0 |
| sabir_freshmart.silver.products | COMPLETED | 200 | 0 |
| sabir_freshmart.silver.promo_calendar | COMPLETED | 15051 | 0 |
| sabir_freshmart.silver.stores | COMPLETED | 80 | 0 |

### Expectations

| flow | expectation | passed | failed |
|---|---|---|---|
| sabir_freshmart.silver.deliveries | positive_receipt | 4739 | 0 |
| sabir_freshmart.silver.inventory_snapshots | keys_present | 16000 | 0 |
| sabir_freshmart.silver.inventory_snapshots | non_negative_on_hand | 16000 | 0 |
| sabir_freshmart.silver.pos_sales | non_negative_units | 15368 | 0 |
| sabir_freshmart.silver.pos_sales | valid_date | 15368 | 0 |
| sabir_freshmart.silver.pos_sales | valid_sku | 15368 | 0 |
| sabir_freshmart.silver.pos_sales | valid_store | 15368 | 0 |
| sabir_freshmart.silver.products | price_above_cost | 200 | 0 |

## Update `21512f61-fadf-40c2-9a6d-eb39f36ea2cb` — **COMPLETED** (2026-11-06T01:07:19.726Z → 2026-11-06T01:08:55.633Z)

### Rows per flow

| flow | status | rows_written | rows_dropped |
|---|---|---|---|
| sabir_freshmart.bronze.deliveries_raw |  | 1725562 | 0 |
| sabir_freshmart.bronze.inventory_snapshots_raw |  | 5840000 | 0 |
| sabir_freshmart.bronze.pos_sales_raw |  | 5580621 | 0 |
| sabir_freshmart.bronze.products_raw |  | 200 | 0 |
| sabir_freshmart.bronze.promotions_raw |  | 1020 | 0 |
| sabir_freshmart.bronze.stores_raw |  | 80 | 0 |
| sabir_freshmart.gold.store_daily_kpis |  | 29200 | 0 |
| sabir_freshmart.gold.store_sku_daily |  | 5840000 | 0 |
| sabir_freshmart.silver.deliveries |  | 1725562 | 0 |
| sabir_freshmart.silver.inventory_snapshots |  | 5840000 | 0 |
| sabir_freshmart.silver.pos_sales |  | 5580141 | 480 |
| sabir_freshmart.silver.products |  | 200 | 0 |
| sabir_freshmart.silver.promo_calendar |  | 15051 | 0 |
| sabir_freshmart.silver.stores |  | 80 | 0 |

### Expectations

| flow | expectation | passed | failed |
|---|---|---|---|
| sabir_freshmart.silver.deliveries | positive_receipt | 1725562 | 0 |
| sabir_freshmart.silver.inventory_snapshots | keys_present | 5840000 | 0 |
| sabir_freshmart.silver.inventory_snapshots | non_negative_on_hand | 5840000 | 0 |
| sabir_freshmart.silver.pos_sales | non_negative_units | 5580321 | 300 |
| sabir_freshmart.silver.pos_sales | valid_date | 5580621 | 0 |
| sabir_freshmart.silver.pos_sales | valid_sku | 5580621 | 0 |
| sabir_freshmart.silver.pos_sales | valid_store | 5580441 | 180 |
| sabir_freshmart.silver.products | price_above_cost | 200 | 0 |

