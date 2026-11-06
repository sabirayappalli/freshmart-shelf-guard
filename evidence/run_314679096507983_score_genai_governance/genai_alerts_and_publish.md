# genai_alerts_and_publish

- Task run id: `342977126100969`  
- Result: **SUCCESS**  
- Duration: 436s

# 05 · GenAI alert reasons + store briefings → `serving.*` (published to Lakebase)

The model says *what* is at risk; store managers need *why* and *what to do* in one line they trust.
`ai_query()` runs in SQL, in batch, governed by Unity Catalog — the LLM only sees the structured facts we pass it
(no free-text customer data), and the facts stay in the row next to the sentence so it can be audited.

Outputs (both Change Data Feed-enabled, synced to Lakebase for the app):
- `serving.stockout_alerts` — one row per store-SKU, latest scoring run; AI reason for the at-risk ones
- `serving.store_briefings` — one AI morning briefing per store

```python
dbutils.widgets.text("catalog", "sabir_freshmart")
dbutils.widgets.text("llm_endpoint", "databricks-claude-haiku-5-5")
dbutils.widgets.text("max_ai_alerts", "1500")
CATALOG = dbutils.widgets.get("catalog")
LLM = dbutils.widgets.get("llm_endpoint")
MAX_AI = int(dbutils.widgets.get("max_ai_alerts"))
spark.sql(f"USE CATALOG {CATALOG}")
```

```python
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW alerts_ai AS
WITH ranked AS (
  SELECT *, row_number() OVER (ORDER BY revenue_at_risk_usd DESC) AS rk
  FROM ml.stockout_risk_scores WHERE risk_tier IN ('HIGH', 'MEDIUM')
)
SELECT store_id, sku_id,
  ai_query('{LLM}',
    concat('You are a replenishment assistant for a grocery store manager. In ONE sentence of at most 30 words, explain why this item ',
           'is likely to run out in the next 3 days and what to do. Use only these facts, cite the key numbers, no preamble.\\n',
           'Item: ', sku_name, ' (', category, ')\\nStore: ', store_name, '\\nOn hand: ', on_hand_units, ' units; on order: ', on_order_units,
           '\\nForecast 3-day demand: ', round(forecast_units_3d, 0), ' units (', demand_uplift_vs_runrate, 'x normal)',
           '\\nStockout probability: ', round(stockout_prob_3d * 100, 0), '%\\nRevenue at risk: $', round(revenue_at_risk_usd, 0),
           '\\nDelivery due in next 3 days: ', inbound_3d_units, ' units\\nDrivers: ', risk_drivers, '\\nRecommended action: ', recommended_action,
           '\\nSuggested extra order now: ', recommended_order_units, ' units (lands in ', lead_time_days, ' days); it cuts stockout probability in days ',
           order_window_start_day, '-', order_window_end_day, ' from ', round(stockout_prob_order_window * 100, 0), '% to ', round(stockout_prob_if_approved * 100, 0), '%'),
    failOnError => false) AS ai
FROM ranked WHERE rk <= {MAX_AI}
""")
spark.sql("CREATE OR REPLACE TABLE ml.alert_ai_reasons COMMENT 'Raw ai_query outputs per at-risk store-SKU (audit trail).' AS SELECT *, current_timestamp() AS generated_at FROM alerts_ai")
# Quality gate: with a max_tokens cap (90 or 1000) this model returned "" for most rows (its budget is used before any visible text), so we
# use the endpoint default and check explicitly that outputs are non-empty.
q = spark.sql("""SELECT count(*) AS ai_calls, count_if(ai.errorMessage IS NOT NULL AND ai.errorMessage <> '') AS errors,
                        count_if(coalesce(trim(ai.result), '') = '') AS empty FROM ml.alert_ai_reasons""").first()
print(q.asDict())
assert q.ai_calls == 0 or (q.errors + q.empty) / q.ai_calls < 0.05, f"ai_query quality gate failed: {q.asDict()}"
```

**Output:**

```text
{'ai_calls': 1500, 'errors': 0, 'empty': 0}
```

```python
spark.sql("""
CREATE TABLE IF NOT EXISTS serving.stockout_alerts (
  alert_id STRING NOT NULL, as_of_date DATE, store_id STRING, store_name STRING, city STRING, region STRING, store_format STRING,
  sku_id STRING, sku_name STRING, category STRING, subcategory STRING, unit_price DOUBLE, case_pack INT, lead_time_days INT,
  on_hand_units INT, on_order_units INT, forecast_units_3d DOUBLE, days_of_cover DOUBLE, stockout_prob_3d DOUBLE, risk_tier STRING,
  revenue_at_risk_usd DOUBLE, recommended_order_units INT, recommended_action STRING, order_window_start_day INT, order_window_end_day INT,
  stockout_prob_order_window DOUBLE, stockout_prob_if_approved DOUBLE, revenue_at_risk_7d_usd DOUBLE, revenue_protected_if_approved_usd DOUBLE, inbound_3d_units INT, max_promo_discount_3d DOUBLE, promo_type STRING,
  risk_drivers STRING, ai_reason STRING, ai_model STRING, model_version INT, generated_at TIMESTAMP,
  CONSTRAINT stockout_alerts_pk PRIMARY KEY (alert_id))
COMMENT 'App-facing stockout alerts (latest run). Synced to Lakebase Postgres for sub-10ms reads.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql(f"""
MERGE INTO serving.stockout_alerts t
USING (
  SELECT concat(r.store_id, '-', r.sku_id) AS alert_id, r.as_of_date, r.store_id, r.store_name, r.city, r.region, r.store_format,
         r.sku_id, r.sku_name, r.category, r.subcategory, r.unit_price, CAST(r.case_pack AS INT) case_pack, CAST(r.lead_time_days AS INT) lead_time_days,
         CAST(r.on_hand_units AS INT) on_hand_units, CAST(r.on_order_units AS INT) on_order_units, round(r.forecast_units_3d, 1) forecast_units_3d,
         r.days_of_cover, r.stockout_prob_3d, r.risk_tier, r.revenue_at_risk_usd, CAST(r.recommended_order_units AS INT) recommended_order_units,
         r.recommended_action, CAST(r.order_window_start_day AS INT) order_window_start_day, CAST(r.order_window_end_day AS INT) order_window_end_day,
         r.stockout_prob_order_window, r.stockout_prob_if_approved, r.revenue_at_risk_7d_usd, r.revenue_protected_if_approved_usd, CAST(r.inbound_3d_units AS INT) inbound_3d_units, r.max_promo_discount_3d, r.promo_type, r.risk_drivers,
         coalesce(nullif(trim(a.ai.result), ''), CASE WHEN r.risk_tier = 'LOW' THEN NULL ELSE concat('At risk: ', r.risk_drivers) END) AS ai_reason,
         CASE WHEN nullif(trim(a.ai.result), '') IS NOT NULL THEN '{LLM}' END AS ai_model, CAST(r.model_version AS INT) model_version, current_timestamp() AS generated_at
  FROM ml.stockout_risk_scores r LEFT JOIN ml.alert_ai_reasons a USING (store_id, sku_id)
) s ON t.alert_id = s.alert_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
```

### Store morning briefings (one LLM call per store)

```python
spark.sql("""
CREATE TABLE IF NOT EXISTS serving.store_briefings (
  store_id STRING NOT NULL, store_name STRING, region STRING, as_of_date DATE, high_risk_items BIGINT, medium_risk_items BIGINT,
  revenue_at_risk_usd DOUBLE, briefing STRING, ai_model STRING, generated_at TIMESTAMP,
  CONSTRAINT store_briefings_pk PRIMARY KEY (store_id))
COMMENT 'One AI-written morning briefing per store, grounded on that store''s top at-risk items. Synced to Lakebase.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql(f"""
MERGE INTO serving.store_briefings t USING (
WITH top AS (
  SELECT store_id, store_name, region, as_of_date,
         count_if(risk_tier = 'HIGH') AS high_risk_items, count_if(risk_tier = 'MEDIUM') AS medium_risk_items,
         round(sum(revenue_at_risk_usd), 0) AS revenue_at_risk_usd,
         array_join(slice(transform(array_sort(collect_list(CASE WHEN risk_tier <> 'LOW' THEN named_struct('r', -revenue_at_risk_usd,
             't', concat(sku_name, ' (on hand ', on_hand_units, ', 3d demand ', round(forecast_units_3d,0), ', ', risk_drivers, ')')) END)),
             x -> x.t), 1, 5), '\\n- ') AS top_items
  FROM serving.stockout_alerts GROUP BY store_id, store_name, region, as_of_date
)
SELECT store_id, store_name, region, as_of_date, high_risk_items, medium_risk_items, revenue_at_risk_usd,
  ai_query('{LLM}', concat('Write a 3-bullet morning briefing (max 70 words total) for the manager of ', store_name,
     '. Prioritise by revenue at risk, say which orders to approve before the 10am DC cutoff. Facts: ', high_risk_items, ' high-risk and ',
     medium_risk_items, ' medium-risk items, $', revenue_at_risk_usd, ' revenue at risk over 3 days. Top items:\\n- ', top_items)) AS briefing,
  '{LLM}' AS ai_model, current_timestamp() AS generated_at
FROM top
) s ON t.store_id = s.store_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

### Sample AI output (verbatim)

```python
ex = spark.sql("""SELECT store_name, sku_name, risk_tier, round(stockout_prob_3d,2) p, revenue_at_risk_usd, recommended_order_units, round(stockout_prob_if_approved,2) p_after, ai_reason
                  FROM serving.stockout_alerts WHERE ai_model IS NOT NULL ORDER BY revenue_at_risk_usd DESC LIMIT 12""")
display(ex)
for r in ex.collect():
    print(f"[{r.risk_tier} p={r.p} ${r.revenue_at_risk_usd}] {r.store_name} | {r.sku_name}\n   order {r.recommended_order_units} -> risk {r.p_after} -> {r.ai_reason}\n")
b = spark.sql("SELECT store_name, revenue_at_risk_usd, briefing FROM serving.store_briefings ORDER BY revenue_at_risk_usd DESC LIMIT 3").collect()
for r in b:
    print(f"=== {r.store_name} (${r.revenue_at_risk_usd} at risk)\n{r.briefing}\n")
```

**Output:**

| store_name | sku_name | risk_tier | p | revenue_at_risk_usd | recommended_order_units | p_after | ai_reason |
|---|---|---|---|---|---|---|---|
| FreshMart Salt Lake City #1080 | Harvest Co Juice Value | HIGH | 1.0 | 428.7 | 0 | 0.04 | Harvest Co Juice Value runs out: 0 on hand vs. 143 forecast units in 3 days (100% stockout risk, $429 at risk); expedite |
| FreshMart Salt Lake City #1065 | Northstar Coffee Classic | HIGH | 1.0 | 351.48 | 48 | 0.01 | Northstar Coffee Classic will likely stock out within 3 days: 0 units on hand against 155.0 units of forecast demand (1. |
| FreshMart Portland #1079 | Northstar Coffee Classic | HIGH | 1.0 | 316.95 | 72 | 0.03 | Northstar Coffee Classic: 0 on hand vs 145 forecast demand, 100% stockout risk; expedite 264 due soon and order 72 more  |
| FreshMart Sacramento #1063 | Northstar Coffee Classic | HIGH | 1.0 | 310.24 | 48 | 0.0 | Northstar Coffee Classic will stock out: 0 on hand versus 73 demand (25% promo); expedite or transfer, and order 48 more |
| FreshMart Raleigh #1040 | Valley Farms Soda Value | HIGH | 1.0 | 223.75 | 0 | 0.01 | Valley Farms Soda Value has 0 units on hand against 106.0 units of forecast demand over 3 days (100.0% stockout risk), s |
| FreshMart Boston #1011 | Harvest Co Soda Organic | HIGH | 1.0 | 219.13 | 0 | 0.0 | Harvest Co Soda Organic: 22 units vs 103.0 forecast 3-day demand, 0.6 days cover, 100% stockout probability, and $219.0  |
| FreshMart Albany #1008 | Harvest Co Juice Value | HIGH | 1.0 | 205.07 | 0 | 0.0 | Harvest Co Juice Value faces 100% stockout risk in 3 days: 2 units on hand against 40.0 forecast demand, $205.0 at risk, |
| FreshMart Portland #1074 | FreshMart Meals Organic | HIGH | 1.0 | 200.32 | 36 | 0.0 | FreshMart Meals Organic will sell out: 0 units on hand and no delivery in 3 days against 34.0 units of forecast demand,  |
| FreshMart Salt Lake City #1075 | Northstar Coffee Classic | HIGH | 1.0 | 174.73 | 48 | 0.01 | Six units against 92.0 forecast demand (1.27x, 25% promo) make stockout 100% likely: expedite 96 units, transfer stock,  |
| FreshMart Phoenix #1062 | FreshMart Meals Organic | HIGH | 1.0 | 170.57 | 12 | 0.0 | Expect a stockout: only 1 unit on hand against 30.0 forecast units over 3 days (1.32x normal, 25% promo) with no deliver |
| FreshMart Portland #1064 | Northstar Coffee Classic | HIGH | 1.0 | 166.55 | 48 | 0.03 | Northstar Coffee Classic: 0 units vs 74.0 forecast 3-day demand (1.32x normal); expedite the 120 on order and add 48 to  |
| FreshMart Salt Lake City #1080 | Blue Ridge Cookies Organic | HIGH | 1.0 | 145.63 | 36 | 0.01 | With 0 units on hand against 69.0 units of 3-day demand (1.34x normal, 25% promo), stockout probability is 100%; expedit |

```text
[HIGH p=1.0 $428.7] FreshMart Salt Lake City #1080 | Harvest Co Juice Value
   order 0 -> risk 0.04 -> Harvest Co Juice Value runs out: 0 on hand vs. 143 forecast units in 3 days (100% stockout risk, $429 at risk); expedite or transfer stock now.

[HIGH p=1.0 $351.48] FreshMart Salt Lake City #1065 | Northstar Coffee Classic
   order 48 -> risk 0.01 -> Northstar Coffee Classic will likely stock out within 3 days: 0 units on hand against 155.0 units of forecast demand (1.21x normal, boosted by a 25% off circular), with 100.0% stockout probability and $351.0 revenue at risk, so expedite the 312 units arriving within 3 days and place an extra 48-unit order now to cut days 4-6 stockout odds from 11.0% to 1.0%.

[HIGH p=1.0 $316.95] FreshMart Portland #1079 | Northstar Coffee Classic
   order 72 -> risk 0.03 -> Northstar Coffee Classic: 0 on hand vs 145 forecast demand, 100% stockout risk; expedite 264 due soon and order 72 more to cut days 4-6 risk from 20% to 3%.

[HIGH p=1.0 $310.24] FreshMart Sacramento #1063 | Northstar Coffee Classic
   order 48 -> risk 0.0 -> Northstar Coffee Classic will stock out: 0 on hand versus 73 demand (25% promo); expedite or transfer, and order 48 more to cut days 4-6 risk from 12% to 0%.

[HIGH p=1.0 $223.75] FreshMart Raleigh #1040 | Valley Farms Soda Value
   order 0 -> risk 0.01 -> Valley Farms Soda Value has 0 units on hand against 106.0 units of forecast demand over 3 days (100.0% stockout risk), so expedite or transfer from a nearby store.

[HIGH p=1.0 $219.13] FreshMart Boston #1011 | Harvest Co Soda Organic
   order 0 -> risk 0.0 -> Harvest Co Soda Organic: 22 units vs 103.0 forecast 3-day demand, 0.6 days cover, 100% stockout probability, and $219.0 at risk; expedite or transfer now, as restock lands in 5 days.

[HIGH p=1.0 $205.07] FreshMart Albany #1008 | Harvest Co Juice Value
   order 0 -> risk 0.0 -> Harvest Co Juice Value faces 100% stockout risk in 3 days: 2 units on hand against 40.0 forecast demand, $205.0 at risk, 0.2 days of cover; expedite or transfer now.

[HIGH p=1.0 $200.32] FreshMart Portland #1074 | FreshMart Meals Organic
   order 36 -> risk 0.0 -> FreshMart Meals Organic will sell out: 0 units on hand and no delivery in 3 days against 34.0 units of forecast demand, so expedite or transfer from a nearby store now.

[HIGH p=1.0 $174.73] FreshMart Salt Lake City #1075 | Northstar Coffee Classic
   order 48 -> risk 0.01 -> Six units against 92.0 forecast demand (1.27x, 25% promo) make stockout 100% likely: expedite 96 units, transfer stock, and order 48 to cut days 4-6 risk from 17.0% to 1.0%.

[HIGH p=1.0 $170.57] FreshMart Phoenix #1062 | FreshMart Meals Organic
   order 12 -> risk 0.0 -> Expect a stockout: only 1 unit on hand against 30.0 forecast units over 3 days (1.32x normal, 25% promo) with no delivery due, risking $171.0; expedite or transfer now.

[HIGH p=1.0 $166.55] FreshMart Portland #1064 | Northstar Coffee Classic
   order 48 -> risk 0.03 -> Northstar Coffee Classic: 0 units vs 74.0 forecast 3-day demand (1.32x normal); expedite the 120 on order and add 48 to cut days 4-6 stockout odds from 31.0% to 3.0%.

[HIGH p=1.0 $145.63] FreshMart Salt Lake City #1080 | Blue Ridge Cookies Organic
   order 36 -> risk 0.01 -> With 0 units on hand against 69.0 units of 3-day demand (1.34x normal, 25% promo), stockout probability is 100%; expedite or transfer stock from a nearby store now.

=== FreshMart Salt Lake City #1065 ($1233.0 at risk)
- **$1,233 at risk over 3 days** (14 high, 13 medium). Approve expedites before the 10am DC cutoff.
- **Northstar Coffee Classic:** 0 on hand, 155 units demand, 25% promo. Expedite or transfer now, since stock empties before a standard order lands.
- **Blue Ridge Cookies Organic** (3 on hand, 69 units, promo) and **Northstar Baking Lite** (0 on hand, 65 units): approve expedites or transfers today.

=== FreshMart Salt Lake City #1080 ($1178.0 at risk)
- **Risk:** 13 high-risk and 15 medium-risk items, with $1,178 in revenue at risk over 3 days.
- **Approve before 10am DC cutoff:** Harvest Co Juice Value (143 units demand, zero stock) and Northstar Coffee Classic (136 demand, 1.2 days cover).
- **Expedite or transfer from nearby stores:** Blue Ridge Cookies Organic, FreshMart Meals Organic (both out, on promo), and Valley Farms Ice Cream Organic (out, 37 demand).

=== FreshMart Nashville #1029 ($1174.0 at risk)
Morning briefing, FreshMart #1029: $1,174 at risk over 3 days (18 high, 21 medium items).

- Approve before 10am DC cutoff: Golden Acre Chips Premium (0 on hand, 67 demand) and Yogurt Lite (0.2 days cover).
- Expedite or transfer Northstar Cookies Lite (0.3 days cover) and Paper Towels Lite (0 on hand).
- Expedite or transfer Northstar Cleaning Value (0 on hand); new orders land after shelves empty.
```

```python
import json
stats = spark.sql("""SELECT count(*) alerts, count_if(risk_tier='HIGH') high, count_if(risk_tier='MEDIUM') medium,
                     count_if(ai_model IS NOT NULL) ai_reasons FROM serving.stockout_alerts""").first().asDict()
stats["briefings"] = spark.table("serving.store_briefings").count()
print(stats)
dbutils.notebook.exit(json.dumps(stats))
```

**Output:**

```text
{"alerts": 16000, "high": 1034, "medium": 1465, "ai_reasons": 1500, "briefings": 80}
```

## Notebook exit value

```json
{
  "alerts": 16000,
  "high": 1034,
  "medium": 1465,
  "ai_reasons": 1500,
  "briefings": 80
}
```
