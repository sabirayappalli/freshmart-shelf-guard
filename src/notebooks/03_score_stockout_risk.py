# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Score tomorrow's stockout risk for every store-SKU → `ml.stockout_risk_scores`
# MAGIC
# MAGIC As of the latest business date, using the `@prod` forecaster:
# MAGIC - **3-day demand** (mean + std) using the *published* promo calendar and holiday proximity for the coming days
# MAGIC - **Stockout probability** = Monte Carlo of the next 3 days *under the current replenishment rule*: real on-hand + open POs by due day,
# MAGIC   demand paths from the forecast distribution, legacy min/max keeps ordering as it does today → P(shelf ends a day empty)
# MAGIC - **Two decisions, two windows**: (a) will the shelf survive the next 3 days? if not and a new order can't land in time → *expedite / transfer*;
# MAGIC   (b) in the window where today's order can land (days L..L+2), does approving the suggested order prevent the stockout? → *approve order*
# MAGIC - **Revenue at risk** = expected lost units × net price; **revenue protected** = 7-day lost sales avoided by approving
# MAGIC - **Suggested order** = order-up-to (lead-time + 1 day demand at 95% service) − position, rounded to case pack
# MAGIC - **Drivers**: promo uplift, holiday, demand trend, thin cover, recent stockouts — structured, so GenAI can explain them

# COMMAND ----------

# MAGIC %pip install -q lightgbm==4.6.0 mlflow>=3.1
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "sabir_freshmart")
CATALOG = dbutils.widgets.get("catalog")
MODEL_NAME = f"{CATALOG}.ml.demand_forecaster"

import json, time, datetime as dt
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

# COMMAND ----------

daily = spark.table(f"{CATALOG}.gold.store_sku_daily")
cal = ff.calendar_features(spark, daily)
promo = (spark.table(f"{CATALOG}.silver.promo_calendar")
         .select("sku_id", "region", F.col("promo_date").alias("target_date"), F.col("discount_pct").alias("tgt_discount"), "promo_type"))
UDF = mlflow.pyfunc.spark_udf(spark, f"models:/{MODEL_NAME}@prod", result_type="mean_units double, std_units double", env_manager="local")
DISP = model.unwrap_python_model().dispersion       # negative-binomial dispersion by category, learned in training
N_PATHS = 200


def simulate_days(s, mu, phi, extra_order=None, seed=11):
    """Monte Carlo the next D days (D = mu.shape[0]) for every store-SKU *under the current replenishment rule*.

    Starts from the real end-of-day position (on hand + open POs by due day from the WMS), draws demand paths from the
    forecast distribution and lets the legacy min/max rule keep ordering as it does today. extra_order = units ordered
    now on top of that (they land in lead_time days). Returns per-day P(shelf empty) paths (D, N, n) and lost units (D, n).
    """
    rng = np.random.default_rng(seed)
    n, D = len(s), mu.shape[0]
    L = s.lead_time_days.to_numpy().astype(int); case = s.case_pack.to_numpy(); avg0 = s.r28.to_numpy()
    pipe = np.zeros((D + int(L.max()) + 2, N_PATHS, n), dtype=np.float32)
    for k, c in enumerate(["inbound_d1", "inbound_d2", "inbound_d3"]):
        pipe[k] += s[c].to_numpy()
    pipe[3] += (s.on_order_units - s.inbound_d1 - s.inbound_d2 - s.inbound_d3).clip(lower=0).to_numpy()
    if extra_order is not None:
        for l in np.unique(L):
            m = L == l
            pipe[l - 1][:, m] += extra_order[m]
    oh = np.broadcast_to(s.on_hand_units.to_numpy().astype(np.float32), (N_PATHS, n)).copy()
    avg = np.broadcast_to(avg0, (N_PATHS, n)).copy()
    hit = np.zeros((D, N_PATHS, n), bool); lost = np.zeros((D, n))
    shape = 1.0 / np.maximum(phi, 1e-3)
    for k in range(D):
        oh += pipe[k]
        d = rng.poisson(rng.gamma(shape, np.maximum(mu[k], 1e-6) / shape, size=(N_PATHS, n)))
        sold = np.minimum(d, oh); lost[k] = (d - sold).mean(axis=0); oh -= sold; hit[k] = oh <= 0
        avg = avg + (sold - avg) / 28.0
        position = oh + pipe[k + 1:].sum(axis=0)
        reorder_pt = avg * (L + 1)
        q = np.where(position < reorder_pt, np.ceil((reorder_pt + 2 * avg - position) / case) * case, 0)
        for l in np.unique(L):
            m = L == l
            pipe[k + l][:, m] += q[:, m]
    return hit, lost


def window_prob(hit, start, end):
    """P(shelf empty on any day in [start, end]) per item; start/end are 1-based day arrays."""
    days = np.arange(1, hit.shape[0] + 1)[:, None]
    win = (days >= start[None]) & (days <= end[None])
    return (hit & win[:, None, :]).any(axis=0).mean(axis=0)


def score(as_of_date):
    H = 7
    grid = (feats.where(F.col("feature_date") == F.lit(as_of_date))
            .crossJoin(spark.range(1, H + 1).withColumnRenamed("id", "h"))
            .withColumn("target_date", F.date_add("feature_date", F.col("h").cast("int")))
            .join(cal, "target_date")
            .join(promo, ["sku_id", "region", "target_date"], "left")
            .fillna({"tgt_discount": 0.0})
            .withColumn("dow_avg_4w", F.expr("element_at(array(dowavg_0,dowavg_1,dowavg_2,dowavg_3,dowavg_4,dowavg_5,dowavg_6), CAST(((7 - h) % 7) + 1 AS INT))")))
    # distributed scoring on the executors (single-node LightGBM predict on the serverless driver was the bottleneck)
    scored = grid.withColumn("p", UDF(*[F.col(c) if c in ff.CATEGORICAL else F.col(c).cast("double").alias(c) for c in ff.FEATURES]))
    t0 = time.time()
    sc = scored.withColumn("mu", F.col("p.mean_units")).withColumn("sd", F.col("p.std_units"))
    in3, in_lt = F.col("h") <= 3, F.col("h") <= F.col("lead_time_days") + 1
    per_sku = (sc.groupBy("store_id", "sku_id").agg(
        F.first("feature_date").alias("feature_date"), F.first("category").alias("category"), F.first("store_format").alias("store_format"),
        F.first("region").alias("region"), F.first("perishable").alias("perishable"), F.first("unit_price").alias("unit_price"),
        F.first("lead_time_days").alias("lead_time_days"), F.first("r28").alias("r28"), F.first("instock_r28").alias("instock_r28"),
        F.first("oos_days_7").alias("oos_days_7"), F.first("on_hand_units").alias("on_hand_units"), F.first("on_order_units").alias("on_order_units"),
        F.first("inbound_d1").alias("inbound_d1"), F.first("inbound_d2").alias("inbound_d2"), F.first("inbound_d3").alias("inbound_d3"),
        F.first("trend_7_28").alias("trend_7_28"),
        F.sum(F.when(in3, F.col("mu"))).alias("forecast_units_3d"), F.sum(F.when(in3, F.col("sd") ** 2)).alias("var3"),
        F.max(F.when(in3, F.col("tgt_discount"))).alias("max_promo_discount_3d"),
        F.first(F.when(in3, F.col("promo_type")), ignorenulls=True).alias("promo_type"),
        F.max(F.when(in3, (F.col("days_to_holiday") <= 3).cast("int"))).alias("holiday_within_3d"),
        F.sum(F.when(in_lt, F.col("mu"))).alias("mu_lt"), F.sum(F.when(in_lt, F.col("sd") ** 2)).alias("var_lt"),
        *[F.sum(F.when(F.col("h") == k, F.col("mu"))).alias(f"mu_d{k}") for k in range(1, 8)])
        .join(spark.table(f"{CATALOG}.silver.products").select("sku_id", "sku_name", "subcategory", "case_pack"), "sku_id")
        .join(spark.table(f"{CATALOG}.silver.stores").select("store_id", "store_name", "city", "state"), "store_id"))
    s = per_sku.toPandas()
    print(f"{as_of_date}: scored {len(s):,} store-SKUs x 7 horizons on Spark in {time.time()-t0:.1f}s")
    t0 = time.time()

    s["sd3"] = np.sqrt(s.var3)
    s["inbound_3d_units"] = s.inbound_d1 + s.inbound_d2 + s.inbound_d3
    up_to = s.mu_lt + 1.65 * np.sqrt(s.var_lt)                                 # ML order-up-to: lead time + 1 day at ~95% service
    need = (up_to - s.on_hand_units - s.on_order_units).clip(lower=0)
    s["recommended_order_units"] = (np.ceil(need / s.case_pack) * s.case_pack).astype(int)
    mu = s[[f"mu_d{k}" for k in range(1, 8)]].to_numpy(dtype=float).T          # (7 days, n)
    phi = s.category.astype(str).map(DISP).fillna(0.2).to_numpy()
    rec = s.recommended_order_units.to_numpy()
    hit0, lost0 = simulate_days(s, mu, phi)
    hit1, lost1 = simulate_days(s, mu, phi, extra_order=rec)
    print(f"{as_of_date}: Monte Carlo ({N_PATHS} paths x 7 days x 2 scenarios) in {time.time()-t0:.1f}s")
    L = s.lead_time_days.to_numpy().astype(int)
    # two decisions, two windows: (a) can the shelf survive the next 3 days? (b) does today's order prevent a stockout
    # in the window it can actually land in (days L .. L+2)?
    s["order_window_start_day"], s["order_window_end_day"] = L, np.minimum(L + 2, 7)
    s["stockout_prob_3d"] = window_prob(hit0, np.ones_like(L), np.full_like(L, 3)).round(4)
    s["stockout_prob_order_window"] = window_prob(hit0, L, np.minimum(L + 2, 7)).round(4)
    s["stockout_prob_if_approved"] = window_prob(hit1, L, np.minimum(L + 2, 7)).round(4)
    s["expected_lost_units_3d"] = lost0[:3].sum(axis=0).round(2)
    net_price = s.unit_price * (1 - s.max_promo_discount_3d)
    s["revenue_at_risk_usd"] = (s.expected_lost_units_3d * net_price).round(2)
    s["revenue_at_risk_7d_usd"] = (lost0.sum(axis=0) * net_price).round(2)
    s["revenue_protected_if_approved_usd"] = (np.clip((lost0 - lost1).sum(axis=0), 0, None) * net_price).round(2)
    needs_expedite = (s.stockout_prob_3d >= 0.5) & (L >= 3)          # a new order can't land in time
    needs_order = (rec > 0) & (s.stockout_prob_order_window - s.stockout_prob_if_approved >= 0.1)
    s["recommended_action"] = np.select([needs_expedite & needs_order, needs_expedite, needs_order, s.stockout_prob_3d >= 0.2],
                                        ["EXPEDITE_AND_ORDER", "EXPEDITE_OR_TRANSFER", "APPROVE_ORDER", "MONITOR"], "NONE")
    s["days_of_cover"] = (s.on_hand_units / (s.forecast_units_3d / 3).clip(lower=0.05)).round(1)
    s["demand_uplift_vs_runrate"] = (s.forecast_units_3d / (3 * s.instock_r28.clip(lower=0.1))).round(2)
    urgency = np.maximum(s.stockout_prob_3d, s.stockout_prob_order_window)
    s["risk_tier"] = np.select([urgency >= 0.5, urgency >= 0.2], ["HIGH", "MEDIUM"], "LOW")

    parts = [
        np.where(s.max_promo_discount_3d > 0, "promo " + (s.max_promo_discount_3d * 100).round().astype(int).astype(str) + "% off (" + s.promo_type.fillna("promo") + ")", ""),
        np.where(s.holiday_within_3d == 1, "holiday within 3 days", ""),
        np.where(s.demand_uplift_vs_runrate >= 1.3, "demand " + s.demand_uplift_vs_runrate.round(1).astype(str) + "x normal run-rate", ""),
        np.where(s.days_of_cover < 2, "only " + s.days_of_cover.astype(str) + " days of cover", ""),
        np.where(s.oos_days_7 >= 1, "out of stock " + s.oos_days_7.astype(int).astype(str) + " of last 7 days", ""),
        np.where(s.inbound_3d_units == 0, "no delivery due in 3 days", ""),
        np.where(needs_expedite, "a new order would land after the shelf empties: expedite or transfer from a nearby store", ""),
    ]
    s["risk_drivers"] = ["; ".join(x for x in row if x) or "steady demand" for row in zip(*parts)]
    s["model_version"] = int(mv.version)
    s["scored_at"] = pd.Timestamp.utcnow().tz_localize(None)
    s = s.rename(columns={"feature_date": "as_of_date"})
    return s

s = score(as_of)

cols = ["as_of_date", "store_id", "store_name", "city", "state", "region", "store_format", "sku_id", "sku_name", "category", "subcategory",
        "perishable", "unit_price", "case_pack", "lead_time_days", "on_hand_units", "on_order_units", "instock_r28", "forecast_units_3d", "sd3",
        "inbound_d1", "inbound_d2", "inbound_d3", "days_of_cover", "stockout_prob_3d", "risk_tier", "expected_lost_units_3d", "revenue_at_risk_usd",
        "recommended_order_units", "recommended_action", "order_window_start_day", "order_window_end_day", "stockout_prob_order_window",
        "stockout_prob_if_approved", "revenue_at_risk_7d_usd", "revenue_protected_if_approved_usd", "inbound_3d_units", "max_promo_discount_3d", "promo_type", "holiday_within_3d",
        "demand_uplift_vs_runrate", "oos_days_7", "risk_drivers", "model_version", "scored_at"]
out = spark.createDataFrame(s[cols].rename(columns={"sd3": "forecast_std_3d", "instock_r28": "instock_run_rate_28d"}))
out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.stockout_risk_scores")
spark.sql(f"COMMENT ON TABLE {CATALOG}.ml.stockout_risk_scores IS 'Daily 3-day stockout risk per store-SKU from ml.demand_forecaster@prod. Region row filter applied (gov.region_filter).'")

# COMMAND ----------

# MAGIC %md ### Calibration check: score as of 3 days before the latest date, compare with what actually happened
# MAGIC Predicted P(stockout within 3 days) vs realised: did the store-SKU end any of the next 3 days with zero on hand?

# COMMAND ----------

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
naive_hit, _ = simulate_days(h, np.repeat(h.r28.to_numpy()[None], 7, axis=0), h.category.astype(str).map(DISP).fillna(0.2).to_numpy())
naive_p = window_prob(naive_hit, np.ones(len(h), int), np.full(len(h), 3))
calib_summary = {"calibration_as_of": str(hist_date), "auc": round(auc, 3), "auc_same_simulation_with_28d_avg_forecast": round(float(roc_auc_score(h.oos_next_3d, naive_p)), 3),
                 "predicted_oos_items_28d_avg_forecast": round(float(naive_p.sum())),
                 "predicted_oos_items": round(float(h.stockout_prob_3d.sum())), "actual_oos_items": int(h.oos_next_3d.sum()),
                 "high_tier_precision": round(float(h[h.stockout_prob_3d >= 0.5].oos_next_3d.mean()), 3),
                 "share_of_actual_oos_caught_by_high_medium": round(float(h[h.stockout_prob_3d >= 0.2].oos_next_3d.sum() / h.oos_next_3d.sum()), 3)}
# order-window check: score as of 7 days back; did the shelf go empty in that item's own window (days L..L+2)?
h7_date = as_of - dt.timedelta(days=7)
h7 = score(h7_date)
oos7 = (daily.where((F.col("business_date") > F.lit(h7_date)) & (F.col("business_date") <= F.lit(as_of)))
        .select("store_id", "sku_id", F.datediff("business_date", F.lit(h7_date)).alias("day"), F.col("is_oos").cast("int").alias("oos")).toPandas())
h7 = h7.merge(oos7.pivot_table(index=["store_id", "sku_id"], columns="day", values="oos").add_prefix("oos_d").reset_index(), on=["store_id", "sku_id"])
dmat = h7[[f"oos_d{k}" for k in range(1, 8)]].to_numpy()
days = np.arange(1, 8)[None]
h7["oos_in_window"] = ((dmat == 1) & (days >= h7.order_window_start_day.to_numpy()[:, None]) & (days <= h7.order_window_end_day.to_numpy()[:, None])).any(axis=1).astype(int)
calib_summary.update({"order_window_calibration_as_of": str(h7_date),
                      "order_window_auc": round(float(roc_auc_score(h7.oos_in_window, h7.stockout_prob_order_window)), 3),
                      "order_window_predicted_oos_items": round(float(h7.stockout_prob_order_window.sum())),
                      "order_window_actual_oos_items": int(h7.oos_in_window.sum())})
display(calib)
print(calib.to_string(index=False)); print(json.dumps(calib_summary, indent=2))
spark.createDataFrame(calib.astype({"bucket": str})).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.risk_calibration")

# COMMAND ----------

# MAGIC %md ### Risk summary

# COMMAND ----------

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
