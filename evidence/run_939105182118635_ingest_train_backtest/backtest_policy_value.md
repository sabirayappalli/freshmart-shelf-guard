# backtest_policy_value

- Task run id: `443139840938318`  
- Result: **SUCCESS**  
- Duration: 164s

# 04 · Backtest: what would the ML policy have saved? → `ml.backtest_summary`, `ml.backtest_daily`

Replays the **last 6 weeks** (2026-09-21 → 2026-11-01, never seen in training) through the store-inventory simulator twice,
on the **same true customer demand**, starting from the same on-hand / on-order state:

| Policy | Order rule |
|---|---|
| **Legacy** (today) | min/max off a trailing 28-day average of observed sales |
| **ML** | daily order-up-to = forecast(lead time + 1 day) + 1.65·σ (95% service), using `demand_forecaster@prod` |

Conservative by construction: the ML policy's features come from the *legacy* (censored) sales history, so it never benefits
from the cleaner history it would itself create.

```python
%pip install -q lightgbm==4.6.0 mlflow>=3.1
%restart_python
```

```python
dbutils.widgets.text("catalog", "sabir_freshmart")
CATALOG = dbutils.widgets.get("catalog")
MODEL_NAME = f"{CATALOG}.ml.demand_forecaster"

import json, datetime as dt
import numpy as np, pandas as pd
from pyspark.sql import functions as F
import mlflow
import freshmart_sim as fs, freshmart_features as ff

g = fs.generate()
dates, products, stores = g["dates"], g["products"], g["stores"]
HOLD = 42
t0 = len(dates) - HOLD
hold_dates = dates[t0:]
print("holdout:", hold_dates[0], "->", hold_dates[-1])
```

**Output:**

```text
color_warning(
```
```text
holdout: 2026-09-21 -> 2026-11-01
```

### Forecasts for every holdout decision day (Spark-distributed scoring with the registered model)

```python
mlflow.set_registry_uri("databricks-uc")
udf = mlflow.pyfunc.spark_udf(spark, f"models:/{MODEL_NAME}@prod", result_type="mean_units double, std_units double", env_manager="local")
daily = spark.table(f"{CATALOG}.gold.store_sku_daily")
cal = ff.calendar_features(spark, daily)
promo = spark.table(f"{CATALOG}.silver.promo_calendar").select("sku_id", "region", F.col("promo_date").alias("target_date"), F.col("discount_pct").alias("tgt_discount"))
feats = spark.table(f"{CATALOG}.ml.store_sku_features").where(F.col("feature_date") >= F.lit(hold_dates[0]))
grid = (feats.crossJoin(spark.range(1, 8).withColumnRenamed("id", "h"))
        .where(F.col("h") <= F.col("lead_time_days") + 1)
        .withColumn("target_date", F.date_add("feature_date", F.col("h").cast("int")))
        .join(cal, "target_date").join(promo, ["sku_id", "region", "target_date"], "left").fillna({"tgt_discount": 0.0})
        .withColumn("dow_avg_4w", F.expr("element_at(array(dowavg_0,dowavg_1,dowavg_2,dowavg_3,dowavg_4,dowavg_5,dowavg_6), CAST(((7 - h) % 7) + 1 AS INT))")))
scored = grid.withColumn("p", udf(*[F.col(c) if c in ff.CATEGORICAL else F.col(c).cast("double").alias(c) for c in ff.FEATURES]))
agg = (scored.groupBy("feature_date", "store_id", "sku_id")
       .agg(F.sum("p.mean_units").alias("mu"), F.sqrt(F.sum(F.col("p.std_units") ** 2)).alias("sd"))).toPandas()
print(f"{len(agg):,} store-SKU-day order decisions forecast")

S, K = len(stores), len(products)
s_idx = {s: i for i, s in enumerate(stores.store_id)}
k_idx = {k: i for i, k in enumerate(products.sku_id)}
d_idx = {d: i for i, d in enumerate(hold_dates)}
MU = np.zeros((HOLD, S, K)); SD = np.zeros((HOLD, S, K))
ti = agg.feature_date.map(d_idx).values; si = agg.store_id.map(s_idx).values; ki = agg.sku_id.map(k_idx).values
MU[ti, si, ki] = agg.mu.values; SD[ti, si, ki] = agg.sd.values
print("coverage of decision grid:", round(float((MU > 0).mean()), 4))
```

**Output:**

```text
2026/11/06 01:18:50 WARNING mlflow.pyfunc: Calling `spark_udf()` with `env_manager="local"` does not recreate the same environment that was used during training, which may lead to errors or inaccurate predictions. We recommend specifying `env_manager="conda"`, which automatically recreates the environment that was used to train the model and performs inference in the recreated environment.
```
```text
672,000 store-SKU-day order decisions forecast
coverage of decision grid: 1.0
```

### Replay both policies on identical demand

```python
w = fs.WARMUP_DAYS
prefix = fs.replay(g["demand_all"][: w + t0], products, g["base"], "legacy")       # identical to production history up to t0
st = prefix["_state"]
dem = g["demand"][t0:]
runs = {}
for policy in ["legacy", "ml"]:
    runs[policy] = fs.replay(dem, products, g["base"], policy, init_on_hand=st["on_hand"], init_avg=st["avg"], pipeline_in=st["pipe"],
                             forecast_fn=(lambda t: (MU[t], SD[t])) if policy == "ml" else None, rng=np.random.default_rng(2026))
# sanity: the legacy replay must reproduce the landed history exactly in its first day's sales
print("legacy replay matches landed POS on day 1:", bool((runs["legacy"]["sales"][0] == g["sim"]["sales"][t0]).all()))
```

**Output:**

```text
legacy replay matches landed POS on day 1: True
```

```python
price, cost = products.unit_price.values, products.unit_cost.values
disc = g["disc"][t0:]
promo_mask = disc > 0
ann = 365 / HOLD

def kpis(r):
    sales_usd = (r["sales"] * price * (1 - disc)).sum()
    lost_usd = (r["lost"] * price * (1 - disc)).sum()
    gm_usd = (r["sales"] * (price * (1 - disc) - cost)).sum()
    return {
        "net_sales_usd": sales_usd, "lost_sales_usd": lost_usd,
        "lost_sales_pct": 100 * lost_usd / (sales_usd + lost_usd),
        "oos_rate_pct": 100 * (r["on_hand_end"] == 0).mean(),
        "promo_oos_rate_pct": 100 * (r["on_hand_end"][promo_mask] == 0).mean(),
        "on_shelf_availability_pct": 100 * (1 - (r["on_hand_end"] == 0).mean()),
        "avg_inventory_at_cost_usd": (r["on_hand_end"] * cost).sum(axis=(1, 2)).mean(),
        "shrink_at_cost_usd": (r["shrink"] * cost).sum(),
        "gross_margin_usd": gm_usd - (r["shrink"] * cost).sum(),
        "fill_rate_pct": 100 * r["sales"].sum() / (r["sales"].sum() + r["lost"].sum()),
    }

L, M = kpis(runs["legacy"]), kpis(runs["ml"])
rows = []
for k in L:
    rows.append(dict(metric=k, legacy=round(float(L[k]), 2), ml_policy=round(float(M[k]), 2), delta=round(float(M[k] - L[k]), 2),
                     delta_pct=round(float(100 * (M[k] - L[k]) / L[k]), 1) if L[k] else None))
summary = pd.DataFrame(rows)
display(summary)
print(summary.to_string(index=False))
```

**Output:**

| metric | legacy | ml_policy | delta | delta_pct |
|---|---|---|---|---|
| net_sales_usd | 37851916.62 | 38488700.17 | 636783.55 | 1.7 |
| lost_sales_usd | 771825.58 | 135042.03 | -636783.55 | -82.5 |
| lost_sales_pct | 2.0 | 0.35 | -1.65 | -82.5 |
| oos_rate_pct | 4.41 | 0.84 | -3.57 | -80.9 |
| promo_oos_rate_pct | 12.88 | 1.57 | -11.3 | -87.8 |
| on_shelf_availability_pct | 95.59 | 99.16 | 3.57 | 3.7 |
| avg_inventory_at_cost_usd | 1645794.67 | 1881801.41 | 236006.74 | 14.3 |
| shrink_at_cost_usd | 644422.13 | 667637.8 | 23215.67 | 3.6 |
| gross_margin_usd | 11307215.67 | 11461970.93 | 154755.26 | 1.4 |
| fill_rate_pct | 98.06 | 99.68 | 1.61 | 1.6 |

```text
metric      legacy   ml_policy      delta  delta_pct
            net_sales_usd 37851916.62 38488700.17  636783.55        1.7
           lost_sales_usd   771825.58   135042.03 -636783.55      -82.5
           lost_sales_pct        2.00        0.35      -1.65      -82.5
             oos_rate_pct        4.41        0.84      -3.57      -80.9
       promo_oos_rate_pct       12.88        1.57     -11.30      -87.8
on_shelf_availability_pct       95.59       99.16       3.57        3.7
avg_inventory_at_cost_usd  1645794.67  1881801.41  236006.74       14.3
       shrink_at_cost_usd   644422.13   667637.80   23215.67        3.6
         gross_margin_usd 11307215.67 11461970.93  154755.26        1.4
            fill_rate_pct       98.06       99.68       1.61        1.6
```

### Value, annualised (modelled scope: 80 stores × 200 high-velocity SKUs)

```python
value = {
    "window_days": HOLD,
    "lost_sales_recovered_usd_annual": round(float((L["lost_sales_usd"] - M["lost_sales_usd"]) * ann)),
    "net_sales_uplift_usd_annual": round(float((M["net_sales_usd"] - L["net_sales_usd"]) * ann)),
    "gross_margin_uplift_usd_annual_after_shrink": round(float((M["gross_margin_usd"] - L["gross_margin_usd"]) * ann)),
    "shrink_change_usd_annual": round(float((M["shrink_at_cost_usd"] - L["shrink_at_cost_usd"]) * ann)),
    "inventory_change_usd": round(float(M["avg_inventory_at_cost_usd"] - L["avg_inventory_at_cost_usd"])),
    "oos_rate_legacy_pct": round(float(L["oos_rate_pct"]), 2), "oos_rate_ml_pct": round(float(M["oos_rate_pct"]), 2),
    "promo_oos_legacy_pct": round(float(L["promo_oos_rate_pct"]), 2), "promo_oos_ml_pct": round(float(M["promo_oos_rate_pct"]), 2),
    "osa_legacy_pct": round(float(L["on_shelf_availability_pct"]), 2), "osa_ml_pct": round(float(M["on_shelf_availability_pct"]), 2),
    "lost_sales_reduction_pct": round(float(100 * (1 - M["lost_sales_usd"] / L["lost_sales_usd"])), 1),
}
print(json.dumps(value, indent=2))

spark.createDataFrame(summary).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.backtest_summary")
spark.sql(f"COMMENT ON TABLE {CATALOG}.ml.backtest_summary IS 'Legacy vs ML replenishment policy, 42-day holdout replay on identical simulated demand.'")
daily_rows = []
for policy, r in runs.items():
    for i, d in enumerate(hold_dates):
        daily_rows.append(dict(business_date=d, policy=policy,
                               lost_sales_usd=float((r["lost"][i] * price * (1 - disc[i])).sum()),
                               oos_rate=float((r["on_hand_end"][i] == 0).mean()),
                               inventory_at_cost_usd=float((r["on_hand_end"][i] * cost).sum())))
spark.createDataFrame(pd.DataFrame(daily_rows)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.backtest_daily")
spark.createDataFrame(pd.DataFrame([{"k": k, "v": float(v)} for k, v in value.items()])).write.mode("overwrite").saveAsTable(f"{CATALOG}.ml.value_summary")
dbutils.notebook.exit(json.dumps(value))
```

**Output:**

```text
{"window_days": 42, "lost_sales_recovered_usd_annual": 5533952, "net_sales_uplift_usd_annual": 5533952, "gross_margin_uplift_usd_annual_after_shrink": 1344897, "shrink_change_usd_annual": 201755, "inventory_change_usd": 236007, "oos_rate_legacy_pct": 4.41, "oos_rate_ml_pct": 0.84, "promo_oos_legacy_pct": 12.88, "promo_oos_ml_pct": 1.57, "osa_legacy_pct": 95.59, "osa_ml_pct": 99.16, "lost_sales_reduction_pct": 82.5}
```

## Notebook exit value

```json
{
  "window_days": 42,
  "lost_sales_recovered_usd_annual": 5533952,
  "net_sales_uplift_usd_annual": 5533952,
  "gross_margin_uplift_usd_annual_after_shrink": 1344897,
  "shrink_change_usd_annual": 201755,
  "inventory_change_usd": 236007,
  "oos_rate_legacy_pct": 4.41,
  "oos_rate_ml_pct": 0.84,
  "promo_oos_legacy_pct": 12.88,
  "promo_oos_ml_pct": 1.57,
  "osa_legacy_pct": 95.59,
  "osa_ml_pct": 99.16,
  "lost_sales_reduction_pct": 82.5
}
```
