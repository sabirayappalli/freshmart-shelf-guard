# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Unity Catalog governance + semantic layer
# MAGIC
# MAGIC | Control | Where | Why |
# MAGIC |---|---|---|
# MAGIC | **Row filter** `gov.region_filter` | `ml.stockout_risk_scores`, `gold.store_daily_kpis` (via view) | Regional managers see only their region; HQ sees all |
# MAGIC | **Column mask** `gov.mask_unit_cost` | `silver.products.unit_cost` (declared in the Lakeflow pipeline) | Supplier cost is margin-sensitive |
# MAGIC | **Tags** | catalog / schemas / tables / columns | Domain, tier, certification, sensitivity — discoverable + policy-ready |
# MAGIC | **Comments** | ML + serving columns | Humans *and* Genie read them |
# MAGIC | **Metric view** `gold.stockout_metrics` | over `gold.store_sku_daily` | One governed definition of OOS rate, OSA, lost sales — used by Genie and the app |
# MAGIC | **Lineage** | automatic | raw files → bronze → silver → gold → ML → serving → Lakebase |

# COMMAND ----------

dbutils.widgets.text("catalog", "sabir_freshmart")
dbutils.widgets.text("app_sp", "")
CATALOG = dbutils.widgets.get("catalog")
APP_SP = dbutils.widgets.get("app_sp").strip()
spark.sql(f"USE CATALOG {CATALOG}")
results = []

def run(sql, label=None):
    try:
        spark.sql(sql)
        results.append((label or sql.strip().split("\n")[0][:110], "OK"))
    except Exception as e:
        results.append((label or sql.strip().split("\n")[0][:110], "SKIPPED: " + str(e).split("\n")[0][:160]))

# COMMAND ----------

# MAGIC %md ### Row-level security

# COMMAND ----------

run("ALTER TABLE ml.stockout_risk_scores SET ROW FILTER gov.region_filter ON (region)")
run("""MERGE INTO gov.user_region_access t USING (
      SELECT 'northeast.manager@freshmart.example' AS principal, 'Northeast' AS region, 'regional_manager' AS role
      UNION ALL SELECT 'west.manager@freshmart.example', 'West', 'regional_manager') s
    ON t.principal = s.principal WHEN NOT MATCHED THEN INSERT *""", "seed example regional-manager mappings")
if APP_SP:
    run(f"""MERGE INTO gov.user_region_access t USING (SELECT '{APP_SP}' principal, 'ALL' region, 'app_service_principal' role) s
           ON t.principal = s.principal WHEN NOT MATCHED THEN INSERT *""", "map app service principal -> ALL")

# COMMAND ----------

# MAGIC %md ### Tags + comments

# COMMAND ----------

run(f"ALTER CATALOG {CATALOG} SET TAGS ('business_area' = 'supply_chain', 'industry' = 'grocery_retail', 'data_origin' = 'synthetic')")
for schema, tier in [("bronze", "bronze"), ("silver", "silver"), ("gold", "gold"), ("ml", "ml"), ("serving", "serving")]:
    run(f"ALTER SCHEMA {schema} SET TAGS ('tier' = '{tier}')")
for t in ["gold.store_sku_daily", "gold.store_daily_kpis"]:
    run(f"ALTER MATERIALIZED VIEW {t} SET TAGS ('certified' = 'true', 'owner_team' = 'merch_analytics')")
for t in ["ml.stockout_risk_scores", "serving.stockout_alerts", "serving.store_briefings"]:
    run(f"ALTER TABLE {t} SET TAGS ('certified' = 'true', 'owner_team' = 'supply_chain_ds', 'contains_ai_output' = '{str(t != 'ml.stockout_risk_scores').lower()}')")
run("ALTER MATERIALIZED VIEW silver.products ALTER COLUMN unit_cost SET TAGS ('sensitivity' = 'confidential')")

col_comments = {
    "ml.stockout_risk_scores": {
        "stockout_prob_3d": "Probability (0-1) the shelf ends any of the next 3 days empty if we rely on the current replenishment rule (Monte Carlo over the ML demand distribution).",
        "revenue_at_risk_usd": "Expected lost net sales over the next 3 days if no action is taken (USD).",
        "recommended_order_units": "Extra units to order now on top of the system order: ML order-up-to (lead time + 1 day demand at 95% service) minus position, rounded to case pack.",
        "stockout_prob_if_approved": "Stockout probability in the order window (days lead_time..lead_time+2) re-simulated with the suggested order placed now.",
        "stockout_prob_order_window": "Stockout probability in days lead_time..lead_time+2, the window in which an order placed today can land.",
        "recommended_action": "EXPEDITE_OR_TRANSFER (shelf empties before any new order can land), EXPEDITE_AND_ORDER, APPROVE_ORDER, MONITOR or NONE.",
        "inbound_3d_units": "Open purchase-order units due to arrive in the next 3 days (WMS).",
        "risk_tier": "HIGH (prob >= 0.5), MEDIUM (0.2-0.5), LOW.",
        "forecast_units_3d": "Forecast customer demand over the next 3 days (units), includes published promos and holidays.",
        "days_of_cover": "On-hand units divided by forecast daily demand.",
        "risk_drivers": "Structured reasons the item is at risk (promo, holiday, demand spike, thin cover, recent stockouts).",
    },
    "serving.stockout_alerts": {
        "ai_reason": "One-sentence explanation + action written by an LLM (ai_query) from the structured facts in this row.",
        "alert_id": "store_id-sku_id; primary key in Lakebase.",
    },
}
for t, cols in col_comments.items():
    for c, txt in cols.items():
        run(f"ALTER TABLE {t} ALTER COLUMN {c} COMMENT '{txt}'", f"comment {t}.{c}")

# COMMAND ----------

# MAGIC %md ### Metric view — the governed KPI definitions (used by Genie + app)

# COMMAND ----------

run(f"""
CREATE OR REPLACE VIEW gold.stockout_metrics
WITH METRICS LANGUAGE YAML
COMMENT 'Governed on-shelf availability, out-of-stock and lost-sales KPIs for FreshMart store x SKU x day.'
AS $$
version: 1.1
source: {CATALOG}.gold.store_sku_daily
dimensions:
  - name: Business Date
    expr: business_date
  - name: Week
    expr: DATE_TRUNC('WEEK', business_date)
  - name: Month
    expr: DATE_TRUNC('MONTH', business_date)
  - name: Region
    expr: region
  - name: Store
    expr: store_name
  - name: Store Format
    expr: store_format
  - name: Category
    expr: category
  - name: Subcategory
    expr: subcategory
  - name: SKU
    expr: sku_name
  - name: On Promo
    expr: on_promo
  - name: Perishable
    expr: perishable
measures:
  - name: Net Sales
    expr: SUM(net_sales_usd)
    comment: Net sales after promo discount, USD.
  - name: Estimated Lost Sales
    expr: SUM(est_lost_sales_usd)
    comment: Sales lost to empty shelves, estimated from the in-stock run-rate (conservative; true demand is censored).
  - name: Units Sold
    expr: SUM(units_sold)
  - name: Out Of Stock Rate
    expr: AVG(CAST(is_oos AS INT))
    comment: Share of store-SKU-days ending with zero on hand.
  - name: On Shelf Availability
    expr: 1 - AVG(CAST(is_oos AS INT))
    comment: OSA = 1 - out-of-stock rate.
  - name: Promo Out Of Stock Rate
    expr: AVG(CASE WHEN on_promo THEN CAST(is_oos AS INT) END)
  - name: Lost Sales Pct
    expr: SUM(est_lost_sales_usd) / NULLIF(SUM(net_sales_usd) + SUM(est_lost_sales_usd), 0)
    comment: Estimated lost sales as a share of potential sales.
  - name: Stockout Days
    expr: SUM(CAST(is_oos AS INT))
$$""", "metric view gold.stockout_metrics")

# COMMAND ----------

# MAGIC %md ### Grants for the app service principal (least privilege: read gold/ml/serving, nothing in raw/bronze)

# COMMAND ----------

if APP_SP:
    run(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{APP_SP}`")
    for s in ["gold", "ml", "serving", "gov"]:
        run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {s} TO `{APP_SP}`")
    run(f"GRANT EXECUTE ON FUNCTION gov.region_filter TO `{APP_SP}`")

# COMMAND ----------

import pandas as pd
res = pd.DataFrame(results, columns=["action", "status"])
display(res)
print(res.to_string(index=False))

# COMMAND ----------

# MAGIC %md ### Evidence: policies in force, metric view query, lineage

# COMMAND ----------

for q in [f"SELECT * FROM gov.user_region_access",
          f"""SELECT table_schema, table_name, tag_name, tag_value FROM {CATALOG}.information_schema.table_tags ORDER BY 1, 2, 3""",
          f"""SELECT table_schema, table_name, column_name, tag_name, tag_value FROM {CATALOG}.information_schema.column_tags""",
          f"""SELECT `Region`, MEASURE(`Net Sales`) net_sales, MEASURE(`Estimated Lost Sales`) lost_sales,
                     MEASURE(`On Shelf Availability`) osa, MEASURE(`Promo Out Of Stock Rate`) promo_oos
              FROM gold.stockout_metrics GROUP BY ALL ORDER BY 1""",
          f"""SELECT source_table_full_name, target_table_full_name, count(*) events FROM system.access.table_lineage
              WHERE target_table_catalog = '{CATALOG}' AND source_table_full_name IS NOT NULL
              GROUP BY ALL ORDER BY 2, 1"""]:
    try:
        df = spark.sql(q).toPandas()
        print(q.split("FROM")[1].split()[0], f"({len(df)} rows)")
        print(df.to_string(index=False, max_colwidth=60), "\n")
    except Exception as e:
        print("query skipped:", str(e)[:200])
dbutils.notebook.exit(res.to_json(orient="records"))
