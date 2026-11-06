# score_stockout_risk

- Task run id: `760984043645898`  
- Result: **FAILED**  
- Duration: 2265s

# 03 · Score tomorrow's stockout risk for every store-SKU → `ml.stockout_risk_scores`

As of the latest business date, using the `@prod` forecaster:
- **3-day demand** (mean + std) using the *published* promo calendar and holiday proximity for the coming days
- **Stockout probability** = Monte Carlo of the next 3 days *under the current replenishment rule*: real on-hand + open POs by due day,
  demand paths from the forecast distribution, legacy min/max keeps ordering as it does today → P(shelf ends a day empty)
- **Revenue at risk** = expected lost units from the same simulation × net price; re-simulated *with* the suggested order to show the risk after approval
- **Suggested order** = order-up-to (lead-time + 1 day demand at 95% service) − position, rounded to case pack
- **Drivers**: promo uplift, holiday, demand trend, thin cover, recent stockouts — structured, so GenAI can explain them

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
from scipy.stats import norm
from pyspark.sql import functions as F
import mlflow
import freshmart_features as ff

mlflow.set_registry_uri("databricks-uc")
mv = mlflow.MlflowClient().get_model_version_by_alias(MODEL_NAME, "prod")
model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@prod")
feats = spark.table(f"{CATALOG}.ml.store_sku_features")
as_of = feats.agg(F.max("feature_date")).first()[0]
print(f"scoring as of {as_of} with {MODEL_NAME} v{mv.version}")
```

**Output:**

```text
color_warning(
```
```text
scoring as of 2026-11-01 with sabir_freshmart.ml.demand_forecaster v3
```

```python
daily = spark.table(f"{CATALOG}.gold.store_sku_daily")
cal = ff.calendar_features(spark, daily)
promo = (spark.table(f"{CATALOG}.silver.promo_calendar")
         .select("sku_id", "region", F.col("promo_date").alias("target_date"), F.col("discount_pct").alias("tgt_discount"), "promo_type"))
DISP = model.unwrap_python_model().dispersion       # negative-binomial dispersion by category, learned in training
N_PATHS = 300


def simulate_next_3_days(s, mu, phi, extra_order=None, seed=11):
    """Monte Carlo the next 3 days for every store-SKU *under the current replenishment rule*.

    Starts from the real end-of-day position (on hand + open POs by due day from the WMS), draws demand paths from the
    forecast distribution, lets the legacy min/max rule keep ordering as it does today, and returns
    P(shelf ends any of the next 3 days empty) and expected lost units. extra_order = units ordered now (arrive in L days).
    """
    rng = np.random.default_rng(seed)
    n = len(s)
    L = s.lead_time_days.to_numpy().astype(int); case = s.case_pack.to_numpy(); avg0 = s.r28.to_numpy()
    D = 3 + int(L.max()) + 1
    pipe = np.zeros((D, N_PATHS, n), dtype=np.float32)
    for k, c in enumerate(["inbound_d1", "inbound_d2", "inbound_d3"]):
        pipe[k] += s[c].to_numpy()
    pipe[3] += (s.on_order_units - s.inbound_d1 - s.inbound_d2 - s.inbound_d3).clip(lower=0).to_numpy()
    if extra_order is not None:
        for l in np.unique(L):
            m = L == l
            pipe[l - 1][:, m] += extra_order[m]
    oh = np.broadcast_to(s.on_hand_units.to_numpy().astype(float), (N_PATHS, n)).copy()
    avg = np.broadcast_to(avg0, (N_PATHS, n)).copy()
    hit = np.zeros((N_PATHS, n), bool); lost = np.zeros((N_PATHS, n))
    shape = 1.0 / np.maximum(phi, 1e-3)
    for k in range(3):
        oh += pipe[k]
        d = rng.poisson(rng.gamma(shape, np.maximum(mu[k], 1e-6) / shape, size=(N_PATHS, n)))
        sold = np.minimum(d, oh); lost += d - sold; oh -= sold; hit |= oh <= 0
        avg = avg + (sold - avg) / 28.0
        position = oh + pipe[k + 1:].sum(axis=0)
        reorder_pt = avg * (L + 1)
        q = np.where(position < reorder_pt, np.ceil((reorder_pt + 2 * avg - position) / case) * case, 0)
        for l in np.unique(L):
            m = L == l
            pipe[k + l][:, m] += q[:, m]
    return hit.mean(axis=0), lost.mean(axis=0)


def score(as_of_date):
    H = 7
    grid = (feats.where(F.col("feature_date") == F.lit(as_of_date))
            .crossJoin(spark.range(1, H + 1).withColumnRenamed("id", "h"))
            .withColumn("target_date", F.date_add("feature_date", F.col("h").cast("int")))
            .join(cal, "target_date")
            .join(promo, ["sku_id", "region", "target_date"], "left")
            .fillna({"tgt_discount": 0.0}))
    pdf = ff.to_model_frame(grid.toPandas())
    pred = model.predict(pdf[ff.FEATURES].astype({c: str for c in ff.CATEGORICAL}))
    pdf["mu"], pdf["sd"] = pred["mean_units"].values, pred["std_units"].values
    print(f"{as_of_date}: {len(pdf):,} horizon-rows scored")

    key = ["store_id", "sku_id"]
    win3 = pdf[pdf.h <= 3]
    agg3 = win3.groupby(key).agg(forecast_units_3d=("mu", "sum"), var3=("sd", lambda s: float((s ** 2).sum())),
                                 max_promo_discount_3d=("tgt_discount", "max"),
                                 promo_type=("promo_type", lambda s: next((x for x in s if isinstance(x, str)), None)),
                                 holiday_within_3d=("days_to_holiday", lambda s: int((s <= 3).any()))).reset_index()
    lt = pdf[pdf.h <= pdf.lead_time_days + 1].groupby(key).agg(mu_lt=("mu", "sum"), var_lt=("sd", lambda s: float((s ** 2).sum()))).reset_index()
    base = pdf[pdf.h == 1][key + ["feature_date", "category", "store_format", "region", "perishable", "unit_price", "lead_time_days",
                                  "r28", "instock_r28", "oos_days_7", "on_hand_units", "on_order_units", "inbound_d1", "inbound_d2", "inbound_d3",
                              "trend_7_28"]]
    s = base.merge(agg3, on=key).merge(lt, on=key)

    case = spark.table(f"{CATALOG}.silver.products").select("sku_id", "sku_name", "subcategory", "case_pack").toPandas()
    stores = spark.table(f"{CATALOG}.silver.stores").select("store_id", "store_name", "city", "state").toPandas()
    s = s.merge(case, on="sku_id").merge(stores, on="store_id")

    s["sd3"] = np.sqrt(s.var3)
    mu_d = win3.pivot_table(index=key, columns="h", values="mu").reset_index()
    mu_d.columns = key + ["mu_d1", "mu_d2", "mu_d3"]
    s = s.merge(mu_d, on=key)
    s["inbound_3d_units"] = s.inbound_d1 + s.inbound_d2 + s.inbound_d3
    up_to = s.mu_lt + 1.65 * np.sqrt(s.var_lt)                                 # ML order-up-to: lead time + 1 day at ~95% service
    need = (up_to - s.on_hand_units - s.on_order_units).clip(lower=0)
    s["recommended_order_units"] = (np.ceil(need / s.case_pack) * s.case_pack).astype(int)
    mu = s[["mu_d1", "mu_d2", "mu_d3"]].to_numpy().T
    phi = s.category.map(DISP).fillna(0.2).to_numpy()
    p_now, lost_now = simulate_next_3_days(s, mu, phi)
    p_after, lost_after = simulate_next_3_days(s, mu, phi, extra_order=s.recommended_order_units.to_numpy())
    s["stockout_prob_3d"] = p_now.round(4)
    s["stockout_prob_if_approved"] = p_after.round(4)
    s["expected_lost_units_3d"] = lost_now.round(2)
    net_price = s.unit_price * (1 - s.max_promo_discount_3d)
    s["revenue_at_risk_usd"] = (s.expected_lost_units_3d * net_price).round(2)
    s["revenue_protected_if_approved_usd"] = ((lost_now - lost_after).clip(min=0) * net_price).round(2)
    s["days_of_cover"] = (s.on_hand_units / (s.forecast_units_3d / 3).clip(lower=0.05)).round(1)
    s["demand_uplift_vs_runrate"] = (s.forecast_units_3d / (3 * s.instock_r28.clip(lower=0.1))).round(2)
    s["risk_tier"] = np.select([s.stockout_prob_3d >= 0.5, s.stockout_prob_3d >= 0.2], ["HIGH", "MEDIUM"], "LOW")

    def drivers(r):
        d = []
        if r.max_promo_discount_3d > 0: d.append(f"promo {int(r.max_promo_discount_3d*100)}% off ({r.promo_type})")
        if r.holiday_within_3d: d.append("holiday within 3 days")
        if r.demand_uplift_vs_runrate >= 1.3: d.append(f"demand {r.demand_uplift_vs_runrate:.1f}x normal run-rate")
        if r.days_of_cover < 2: d.append(f"only {r.days_of_cover} days of cover")
        if r.oos_days_7 >= 1: d.append(f"out of stock {int(r.oos_days_7)} of last 7 days")
        if r.inbound_3d_units == 0: d.append("no delivery due in 3 days")
        return "; ".join(d) or "steady demand"
    s["risk_drivers"] = s.apply(drivers, axis=1)
    s["model_version"] = int(mv.version)
    s["scored_at"] = pd.Timestamp.utcnow().tz_localize(None)
    s = s.rename(columns={"feature_date": "as_of_date"})
    return s

s = score(as_of)

cols = ["as_of_date", "store_id", "store_name", "city", "state", "region", "store_format", "sku_id", "sku_name", "category", "subcategory",
        "perishable", "unit_price", "case_pack", "lead_time_days", "on_hand_units", "on_order_units", "instock_r28", "forecast_units_3d", "sd3",
        "available_3d", "days_of_cover", "stockout_prob_3d", "risk_tier", "expected_lost_units_3d", "revenue_at_risk_usd",
        "recommended_order_units", "stockout_prob_if_approved", "revenue_protected_if_approved_usd", "inbound_3d_units", "max_promo_discount_3d", "promo_type", "holiday_within_3d",
        "demand_uplift_vs_runrate", "oos_days_7", "risk_drivers", "model_version", "scored_at"]
out = spark.createDataFrame(s[cols].rename(columns={"sd3": "forecast_std_3d", "instock_r28": "instock_run_rate_28d"}))
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.stockout_risk_scores")
spark.sql(f"COMMENT ON TABLE {CATALOG}.ml.stockout_risk_scores IS 'Daily 3-day stockout risk per store-SKU from ml.demand_forecaster@prod. Region row filter applied (gov.region_filter).'")
```

**Output:**

```text
2026-11-01: 112,000 horizon-rows scored
```

### Calibration check: score as of 3 days before the latest date, compare with what actually happened
Predicted P(stockout within 3 days) vs realised: did the store-SKU end any of the next 3 days with zero on hand?

```python
from sklearn.metrics import roc_auc_score
hist_date = as_of - dt.timedelta(days=3)
h = score(hist_date)
actual = (daily.where((F.col("business_date") > F.lit(hist_date)) & (F.col("business_date") <= F.lit(hist_date + dt.timedelta(days=3))))
          .groupBy("store_id", "sku_id").agg(F.max(F.col("is_oos").cast("int")).alias("oos_next_3d"),
                                             F.sum("est_lost_sales_usd").alias("est_lost_sales_next_3d")).toPandas())
h = h.merge(actual, on=["store_id", "sku_id"])
h["bucket"] = pd.cut(h.stockout_prob_3d, [-0.01, 0.05, 0.2, 0.5, 0.8, 1.0], labels=["<5%", "5-20%", "20-50%", "50-80%", ">80%"])
calib = (h.groupby("bucket", observed=True).agg(store_skus=("sku_id", "count"), mean_predicted=("stockout_prob_3d", "mean"),
                                                 actual_oos_rate=("oos_next_3d", "mean")).round(3).reset_index())
auc = float(roc_auc_score(h.oos_next_3d, h.stockout_prob_3d))
naive_p, _ = simulate_next_3_days(h, np.repeat(h.r28.to_numpy()[None], 3, axis=0), h.category.map(DISP).fillna(0.2).to_numpy())
calib_summary = {"calibration_as_of": str(hist_date), "auc": round(auc, 3), "auc_same_simulation_with_28d_avg_forecast": round(float(roc_auc_score(h.oos_next_3d, naive_p)), 3),
                 "predicted_oos_items_28d_avg_forecast": round(float(naive_p.sum())),
                 "predicted_oos_items": round(float(h.stockout_prob_3d.sum())), "actual_oos_items": int(h.oos_next_3d.sum()),
                 "high_tier_precision": round(float(h[h.stockout_prob_3d >= 0.5].oos_next_3d.mean()), 3),
                 "share_of_actual_oos_caught_by_high_medium": round(float(h[h.stockout_prob_3d >= 0.2].oos_next_3d.sum() / h.oos_next_3d.sum()), 3)}
display(calib)
print(calib.to_string(index=False)); print(json.dumps(calib_summary, indent=2))
spark.createDataFrame(calib.astype({"bucket": str})).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.risk_calibration")
```

### Risk summary

```python
r = spark.table(f"{CATALOG}.ml.stockout_risk_scores")
summary_df = (r.groupBy("risk_tier").agg(F.count("*").alias("store_skus"), F.round(F.sum("revenue_at_risk_usd"), 0).alias("revenue_at_risk_usd"),
                                         F.round(F.avg("stockout_prob_3d"), 3).alias("avg_prob"),
                                         F.sum(F.when(F.col("max_promo_discount_3d") > 0, 1).otherwise(0)).alias("on_promo"))
              .orderBy("risk_tier"))
display(summary_df)
print(summary_df.toPandas().to_string(index=False))
top = r.orderBy(F.desc("revenue_at_risk_usd")).select("store_name", "sku_name", "on_hand_units", "forecast_units_3d", "stockout_prob_3d",
                                                       "revenue_at_risk_usd", "recommended_order_units", "stockout_prob_if_approved", "risk_drivers").limit(15)
display(top)
print(top.toPandas().to_string(index=False))
res = {"as_of": str(as_of), "model_version": int(mv.version), "scored": r.count(),
       "by_tier": {row.risk_tier: row.store_skus for row in summary_df.collect()},
       "revenue_at_risk_3d_usd": float(r.agg(F.sum("revenue_at_risk_usd")).first()[0]), **calib_summary}
dbutils.notebook.exit(json.dumps(res))
```
