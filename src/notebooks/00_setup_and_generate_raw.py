# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup Unity Catalog + land raw synthetic source files
# MAGIC
# MAGIC **FreshMart** (fictional 80-store regional grocer). This notebook plays the role of the chain's source systems:
# MAGIC
# MAGIC | Source system | Landed as | Folder |
# MAGIC |---|---|---|
# MAGIC | POS (daily store-SKU sales extract) | CSV, monthly files | `pos_sales/` |
# MAGIC | WMS / store inventory (end-of-day on-hand, on-order + open-PO units due in 1/2/3 days) | Parquet, monthly files | `inventory/` |
# MAGIC | DC deliveries / receiving | JSON lines | `deliveries/` |
# MAGIC | Merchandising promo calendar | JSON lines | `promotions/` |
# MAGIC | Store + item master | CSV | `stores/`, `products/` |
# MAGIC
# MAGIC History (2025-11-01 → 2026-10-31) lands now. The **2026-11-01** files are staged separately and landed by a later job task,
# MAGIC to prove that Lakeflow (Auto Loader) only picks up the new files.

# COMMAND ----------

dbutils.widgets.text("catalog", "sabir_freshmart")
CATALOG = dbutils.widgets.get("catalog")

for s, c in [("raw", "Landing zone for source-system extracts"), ("bronze", "Raw, append-only ingested data (Lakeflow)"),
             ("silver", "Cleaned, validated, conformed data"), ("gold", "Business-level store-SKU facts and KPIs"),
             ("ml", "Features, model outputs, backtests"), ("serving", "Curated tables published to Lakebase + app"),
             ("gov", "Governance helpers: access mapping, row-filter / mask functions")]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s} COMMENT '{c}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing COMMENT 'Source-system file drops (synthetic)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.staged COMMENT 'Files staged for later arrival (simulates next-day drop)'")
# Governance primitives must exist before the pipeline runs (silver.products column mask references them).
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.gov.user_region_access (principal STRING, region STRING, role STRING)
  COMMENT 'Who may see which region (row filter) and who is finance (cost mask). region = ALL for HQ.'""")
spark.sql(f"""MERGE INTO {CATALOG}.gov.user_region_access t USING (SELECT current_user() principal, 'ALL' region, 'hq_supply_chain_admin' role) s
  ON t.principal = s.principal WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.gov.region_filter(region STRING) RETURNS BOOLEAN
  COMMENT 'Row filter: HQ sees all regions; regional managers only their own region (group freshmart_<region>_managers or mapping table).'
  RETURN is_account_group_member('freshmart_hq')
      OR is_account_group_member(concat('freshmart_', lower(replace(region, ' ', '')), '_managers'))
      OR EXISTS (SELECT 1 FROM {CATALOG}.gov.user_region_access a WHERE a.principal = current_user() AND a.region IN ('ALL', region))""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.gov.mask_unit_cost(unit_cost DOUBLE) RETURNS DOUBLE
  COMMENT 'Column mask: supplier unit cost (margin-sensitive) visible to finance / HQ only.'
  RETURN CASE WHEN is_account_group_member('freshmart_finance')
              OR EXISTS (SELECT 1 FROM {CATALOG}.gov.user_region_access a WHERE a.principal = current_user() AND a.region = 'ALL')
         THEN unit_cost ELSE NULL END""")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

import os, shutil, json, time
import numpy as np, pandas as pd
import freshmart_sim as fs

t0 = time.time()
g = fs.generate()
print(f"simulated {g['demand'].size:,} store-SKU-days in {time.time()-t0:.1f}s")

stores, products, promos = g["stores"], g["products"], g["promos"]
sim = g["sim"]
LIVE_DAY = fs.END  # 2026-11-01

# COMMAND ----------

LAND = f"/Volumes/{CATALOG}/raw/landing"
STAGE = f"/Volumes/{CATALOG}/raw/staged"
for base in (LAND, STAGE):
    for sub in ["pos_sales", "inventory", "deliveries", "promotions", "stores", "products"]:
        p = f"{base}/{sub}"
        if os.path.exists(p):
            shutil.rmtree(p)
        os.makedirs(p, exist_ok=True)

stores.to_csv(f"{LAND}/stores/stores.csv", index=False)
products.drop(columns=[c for c in products if c.startswith("_")]).to_csv(f"{LAND}/products/products.csv", index=False)
promos.to_json(f"{LAND}/promotions/promotions.json", orient="records", lines=True)

long = fs.to_long(g["dates"], stores, products, **{k: sim[k] for k in ["sales", "on_hand_end", "on_order", "arrivals", "inbound_d1", "inbound_d2", "inbound_d3"]})
long["discount"] = g["disc"].ravel()
price = dict(zip(products.sku_id, products.unit_price))
long["unit_price"] = long.sku_id.map(price)
print(f"long frame: {len(long):,} rows")

# COMMAND ----------

rng = np.random.default_rng(99)
long["month"] = long.date.dt.strftime("%Y_%m")
live_mask = long.date.dt.date == LIVE_DAY
counts = {}
for live, frame in [(False, long[~live_mask]), (True, long[live_mask])]:
    base = STAGE if live else LAND
    for m, part in frame.groupby("month"):
        tag = "2026_11_01" if live else m
        # --- POS: only rows with sales (real POS extracts don't emit zero rows)
        pos = part[part.sales > 0]
        pos = pd.DataFrame({"business_date": pos.date.dt.strftime("%Y-%m-%d"), "store_id": pos.store_id, "sku_id": pos.sku_id,
                            "units_sold": pos.sales, "net_sales_usd": (pos.sales * pos.unit_price * (1 - pos.discount)).round(2),
                            "promo_flag": (pos.discount > 0).astype(int)})
        # inject a few realistic data-quality defects (void reversals, missing store keys) for Lakeflow expectations to catch
        if not live:
            bad = pos.sample(40, random_state=int(rng.integers(1e6))).copy()
            bad.loc[bad.index[:25], "units_sold"] = -bad.loc[bad.index[:25], "units_sold"]
            bad.loc[bad.index[25:], "store_id"] = None
            pos = pd.concat([pos, bad])
        pos.to_csv(f"{base}/pos_sales/pos_{tag}.csv", index=False)
        # --- Inventory snapshot (end of day)
        inv = pd.DataFrame({"snapshot_date": part.date.dt.date, "store_id": part.store_id, "sku_id": part.sku_id,
                            "on_hand_units": part.on_hand_end.astype("int32"), "on_order_units": part.on_order.astype("int32"),
                            "inbound_d1": part.inbound_d1.astype("int32"), "inbound_d2": part.inbound_d2.astype("int32"),
                            "inbound_d3": part.inbound_d3.astype("int32")})
        inv.to_parquet(f"{base}/inventory/inv_{tag}.parquet", index=False)
        # --- Deliveries
        dl = part[part.arrivals > 0]
        dl = pd.DataFrame({"received_date": dl.date.dt.strftime("%Y-%m-%d"), "store_id": dl.store_id, "sku_id": dl.sku_id,
                           "units_received": dl.arrivals})
        dl.to_json(f"{base}/deliveries/deliveries_{tag}.json", orient="records", lines=True)
        counts[tag] = dict(pos=len(pos), inventory=len(inv), deliveries=len(dl))

print(json.dumps({"history_months": len([k for k in counts if k != "2026_11_01"]),
                  "history_rows": {k: sum(v[k] for t, v in counts.items() if t != "2026_11_01") for k in ["pos", "inventory", "deliveries"]},
                  "staged_live_day": counts["2026_11_01"]}, indent=2))

# COMMAND ----------

# MAGIC %md ### What landed

# COMMAND ----------

files = []
for base in (LAND, STAGE):
    for root, _, fnames in os.walk(base):
        for f in fnames:
            files.append(dict(zone=base.split("/")[-1], folder=root.split("/")[-1], file=f,
                              mb=round(os.path.getsize(os.path.join(root, f)) / 1e6, 2)))
fdf = pd.DataFrame(files)
display(fdf.groupby(["zone", "folder"]).agg(files=("file", "count"), total_mb=("mb", "sum")).reset_index())

# COMMAND ----------

# MAGIC %md ### Ground-truth summary of the simulated year (legacy replenishment policy)
# MAGIC Simulator truth (true demand incl. lost sales) is **not** landed — the business only sees censored POS, exactly like reality.

# COMMAND ----------

d, s = g["demand"], sim
p_arr, c_arr = products.unit_price.values, products.unit_cost.values
rev = float((s["sales"] * p_arr).sum()); lost = float((s["lost"] * p_arr).sum())
pm = g["disc"] > 0
truth = {
    "stores": len(stores), "skus": len(products), "days": len(g["dates"]),
    "revenue_usd": round(rev), "true_lost_sales_usd": round(lost), "lost_sales_pct_of_demand": round(100 * lost / (rev + lost), 2),
    "oos_store_sku_day_rate_pct": round(100 * float((s["on_hand_end"] == 0).mean()), 2),
    "oos_rate_on_promo_pct": round(100 * float((s["on_hand_end"][pm] == 0).mean()), 2),
    "avg_inventory_at_cost_usd": round(float((s["on_hand_end"] * c_arr).sum(axis=(1, 2)).mean())),
    "shrink_at_cost_usd": round(float((s["shrink"] * c_arr).sum())),
}
print(json.dumps(truth, indent=2))
dbutils.notebook.exit(json.dumps(truth))
