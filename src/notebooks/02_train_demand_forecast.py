# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Train the store-SKU demand forecaster (LightGBM + MLflow + Unity Catalog)
# MAGIC
# MAGIC **Why a forecast and not just a rule:** FreshMart's legacy replenishment orders off a trailing 28-day average. It is blind to
# MAGIC promotions, holidays and day-of-week — so it under-orders precisely when demand spikes (promo OOS rate ~3x non-promo).
# MAGIC
# MAGIC **Design choices**
# MAGIC - One *global* model across 16,000 store-SKU series (vs 16k local ARIMA models): shares signal across sparse series, trains in minutes.
# MAGIC - Direct multi-horizon: features as of day *t*, predict units on day *t+h* for h = 1..7 (h is a feature).
# MAGIC - **Censoring:** a stocked-out day's POS sales understate demand, so training targets exclude OOS days.
# MAGIC - Known-future covariates: promo discount and holiday proximity on the *target* day (promo calendar is published weeks ahead).

# COMMAND ----------

# MAGIC %pip install -q lightgbm==4.6.0 mlflow>=3.1
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "sabir_freshmart")
CATALOG = dbutils.widgets.get("catalog")
MODEL_NAME = f"{CATALOG}.ml.demand_forecaster"

import json, datetime as dt
import numpy as np, pandas as pd
from pyspark.sql import functions as F, Window as W
import freshmart_features as ff

daily = spark.table(f"{CATALOG}.gold.store_sku_daily")
max_date = daily.agg(F.max("business_date")).first()[0]
print("latest business_date:", max_date, "| rows:", daily.count())

# COMMAND ----------

# MAGIC %md ### Feature table (as of day *t*) → `ml.store_sku_features`

# COMMAND ----------

feats = ff.build_features(spark, daily)
feats.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.store_sku_features")
spark.sql(f"COMMENT ON TABLE {CATALOG}.ml.store_sku_features IS 'Point-in-time features per store-SKU as of feature_date (no leakage: only data <= feature_date).'")
feats = spark.table(f"{CATALOG}.ml.store_sku_features")
display(feats.limit(5))

# COMMAND ----------

# MAGIC %md ### Training set: (features at *t*) × (target on *t+h*), h = 1..7, in-stock target days only

# COMMAND ----------

HOLDOUT_START = max_date - dt.timedelta(days=41)          # last 6 weeks = validation (and backtest window)
targets = (daily.select("store_id", "sku_id", F.col("business_date").alias("target_date"), F.col("units_sold").alias("y"),
                        "is_oos", F.col("promo_discount_pct").alias("tgt_discount"))
           .where(~F.col("is_oos")))
cal = ff.calendar_features(spark, daily)
sampled = feats.where(F.col("feature_date") < F.lit(max_date)).sample(fraction=0.09, seed=7)
train_df = (sampled.crossJoin(spark.range(1, 8).withColumnRenamed("id", "h"))
            .withColumn("target_date", F.date_add("feature_date", F.col("h").cast("int")))
            .join(targets, ["store_id", "sku_id", "target_date"])
            .join(cal, "target_date"))
pdf = train_df.select(*[c for c in ff.FEATURES if c != "dow_avg_4w"], *[f"dowavg_{k}" for k in range(7)], "y", "target_date").toPandas()
pdf = ff.to_model_frame(pdf)
tr = pdf[pdf.target_date < HOLDOUT_START]
va = pdf[pdf.target_date >= HOLDOUT_START]
print(f"train rows {len(tr):,} | validation rows {len(va):,} | holdout starts {HOLDOUT_START}")

# COMMAND ----------

import lightgbm as lgb, mlflow
from mlflow.models import infer_signature
mlflow.set_registry_uri("databricks-uc")
user = spark.sql("select current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/freshmart_demand_forecaster")

params = dict(objective="tweedie", tweedie_variance_power=1.2, learning_rate=0.05, num_leaves=127, min_data_in_leaf=200,
              feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, lambda_l2=1.0, verbose=-1, seed=7)

def wape(y, p):
    return float(np.abs(y - p).sum() / y.sum())

with mlflow.start_run(run_name="lgbm_global_direct_h1_7") as run:
    dtr = lgb.Dataset(tr[ff.FEATURES], tr.y, categorical_feature=ff.CATEGORICAL)
    dva = lgb.Dataset(va[ff.FEATURES], va.y, categorical_feature=ff.CATEGORICAL, reference=dtr)
    booster = lgb.train(params, dtr, num_boost_round=1200, valid_sets=[dva],
                        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(100)])
    va = va.assign(pred=booster.predict(va[ff.FEATURES], num_iteration=booster.best_iteration))
    # baselines: what the legacy policy implicitly forecasts (trailing 28d avg) and a seasonal-naive same-weekday avg
    metrics = {
        "wape_model": wape(va.y, va.pred),
        "wape_legacy_trailing28": wape(va.y, va.r28),
        "wape_same_dow_4w": wape(va.y, va.dow_avg_4w.fillna(va.r28)),
        "bias_model": float(va.pred.sum() / va.y.sum() - 1),
        "bias_legacy_trailing28": float(va.r28.sum() / va.y.sum() - 1),
    }
    promo = va.tgt_discount > 0
    metrics["wape_model_promo_days"] = wape(va.y[promo], va.pred[promo])
    metrics["wape_legacy_promo_days"] = wape(va.y[promo], va.r28[promo])
    metrics["bias_legacy_promo_days"] = float(va.r28[promo].sum() / va.y[promo].sum() - 1)
    metrics["best_iteration"] = booster.best_iteration

    # dispersion per category for the uncertainty model: Var(y|mu) ~= mu + phi * mu^2  (negative-binomial form)
    disp = {}
    for c, g in va.groupby("category"):
        resid2 = (g.y - g.pred) ** 2 - g.pred
        disp[c] = float(max(0.01, (resid2 * g.pred ** 2).sum() / (g.pred ** 4).sum()))
    mlflow.log_params({**params, "horizons": "1-7", "train_rows": len(tr), "holdout_start": str(HOLDOUT_START)})
    mlflow.log_metrics(metrics)
    mlflow.log_dict(disp, "dispersion_by_category.json")
    imp = pd.DataFrame({"feature": ff.FEATURES, "gain": booster.feature_importance("gain")}).sort_values("gain", ascending=False)
    mlflow.log_table(imp, "feature_importance.json")

    model = ff.ForecastModel(booster, disp)
    sample_in = va[ff.FEATURES].head(50).astype({c: str for c in ff.CATEGORICAL})
    info = mlflow.pyfunc.log_model(name="model", python_model=model, input_example=sample_in,
                                   signature=infer_signature(sample_in, model.predict(None, sample_in)),
                                   pip_requirements=["lightgbm==4.6.0", "pandas", "numpy"],
                                   code_paths=["freshmart_features.py"], registered_model_name=MODEL_NAME)
    run_id = run.info.run_id

print(json.dumps(metrics, indent=2))
display(imp.head(15))

# COMMAND ----------

# MAGIC %md ### Accuracy by category (validation = last 6 weeks, in-stock days)

# COMMAND ----------

by_cat = (va.groupby("category").apply(lambda g: pd.Series({
    "actual_units": g.y.sum(), "wape_model": wape(g.y, g.pred), "wape_legacy_28d": wape(g.y, g.r28),
    "legacy_bias_on_promo": (g.r28[g.tgt_discount > 0].sum() / max(1, g.y[g.tgt_discount > 0].sum()) - 1)}))
    .round(3).reset_index())
display(by_cat)
print(by_cat.to_string(index=False))

# COMMAND ----------

# MAGIC %md ### Promote to `@prod`
# MAGIC Gate = what actually causes stockouts, not a vanity accuracy number. Daily store-SKU demand at 5-30 units/day is mostly
# MAGIC Poisson noise (irreducible WAPE ~0.35-0.40), so the model must: (1) beat the legacy forecast on WAPE overall **and** on promo
# MAGIC days, and (2) be unbiased (|bias| <= 5%) — the legacy forecast's real failure is a large *negative* bias on promo days.

# COMMAND ----------

from mlflow import MlflowClient
client = MlflowClient()
version = info.registered_model_version
gate = (metrics["wape_model"] < metrics["wape_legacy_trailing28"]
        and metrics["wape_model_promo_days"] < metrics["wape_legacy_promo_days"]
        and abs(metrics["bias_model"]) <= 0.05)
print("promotion gate passed:", gate)
client.set_registered_model_alias(MODEL_NAME, "challenger", version)
if gate:
    client.set_registered_model_alias(MODEL_NAME, "prod", version)
client.update_registered_model(MODEL_NAME, description="Global LightGBM direct multi-horizon (h=1..7) store-SKU daily demand forecaster for FreshMart. "
                               "Outputs mean units and std (neg-binomial dispersion by category). Trained on in-stock days only.")
summary = {"model": MODEL_NAME, "version": version, "run_id": run_id, "promoted_to_prod": bool(gate), **{k: round(v, 4) for k, v in metrics.items()}}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
