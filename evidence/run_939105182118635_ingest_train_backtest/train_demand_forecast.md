# train_demand_forecast

- Task run id: `941117783362155`  
- Result: **SUCCESS**  
- Duration: 413s

# 02 · Train the store-SKU demand forecaster (LightGBM + MLflow + Unity Catalog)

**Why a forecast and not just a rule:** FreshMart's legacy replenishment orders off a trailing 28-day average. It is blind to
promotions, holidays and day-of-week — so it under-orders precisely when demand spikes (promo OOS rate ~3x non-promo).

**Design choices**
- One *global* model across 16,000 store-SKU series (vs 16k local ARIMA models): shares signal across sparse series, trains in minutes.
- Direct multi-horizon: features as of day *t*, predict units on day *t+h* for h = 1..7 (h is a feature).
- **Censoring:** a stocked-out day's POS sales understate demand, so training targets exclude OOS days.
- Known-future covariates: promo discount and holiday proximity on the *target* day (promo calendar is published weeks ahead).

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
from pyspark.sql import functions as F, Window as W
import freshmart_features as ff

daily = spark.table(f"{CATALOG}.gold.store_sku_daily")
max_date = daily.agg(F.max("business_date")).first()[0]
print("latest business_date:", max_date, "| rows:", daily.count())
```

**Output:**

```text
color_warning(
```
```text
latest business_date: 2026-11-01 | rows: 5856000
```

### Feature table (as of day *t*) → `ml.store_sku_features`

```python
feats = ff.build_features(spark, daily)
feats.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.ml.store_sku_features")
spark.sql(f"COMMENT ON TABLE {CATALOG}.ml.store_sku_features IS 'Point-in-time features per store-SKU as of feature_date (no leakage: only data <= feature_date).'")
feats = spark.table(f"{CATALOG}.ml.store_sku_features")
display(feats.limit(5))
```

**Output:**

| feature_date | store_id | sku_id | category | store_format | region | perishable | unit_price | lead_time_days | last_units | on_hand_units | on_order_units | inbound_d1 | inbound_d2 | inbound_d3 | promo_today | r7 | r28 | instock_r28 | oos_days_7 | dowavg_0 | dowavg_1 | dowavg_2 | dowavg_3 | dowavg_4 | dowavg_5 | dowavg_6 | trend_7_28 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2025-11-28 | S1001 | SKU00001 | Dairy | Neighborhood | Northeast | True | 5.49 | 1 | 9 | 18 | 0 | 0 | 0 | 0 | 0.0 | 6.428571428571429 | 6.214285714285714 | 6.214285714285714 | 0 | 7.75 | 5.25 | 3.5 | 4.25 | 7.25 | 7.5 | 8.0 | 1.0344827586206897 |
| 2025-11-29 | S1001 | SKU00001 | Dairy | Neighborhood | Northeast | True | 5.49 | 1 | 4 | 14 | 0 | 0 | 0 | 0 | 0.0 | 6.0 | 6.214285714285714 | 6.214285714285714 | 0 | 8.0 | 7.75 | 5.25 | 3.5 | 4.25 | 7.25 | 7.5 | 0.9655172413793103 |
| 2025-11-30 | S1001 | SKU00001 | Dairy | Neighborhood | Northeast | True | 5.49 | 1 | 4 | 10 | 24 | 24 | 0 | 0 | 0.0 | 5.142857142857143 | 6.178571428571429 | 6.178571428571429 | 0 | 7.25 | 8.0 | 7.75 | 5.25 | 3.5 | 4.25 | 7.25 | 0.8323699421965318 |
| 2025-12-01 | S1001 | SKU00001 | Dairy | Neighborhood | Northeast | True | 5.49 | 1 | 13 | 21 | 0 | 0 | 0 | 0 | 0.0 | 6.0 | 6.428571428571429 | 6.428571428571429 | 0 | 9.0 | 7.25 | 8.0 | 7.75 | 5.25 | 3.5 | 4.25 | 0.9333333333333333 |
| 2025-12-02 | S1001 | SKU00001 | Dairy | Neighborhood | Northeast | True | 5.49 | 1 | 3 | 17 | 0 | 0 | 0 | 0 | 0.0 | 5.285714285714286 | 6.5 | 6.5 | 0 | 4.75 | 9.0 | 7.25 | 8.0 | 7.75 | 5.25 | 3.5 | 0.8131868131868132 |


### Training set: (features at *t*) × (target on *t+h*), h = 1..7, in-stock target days only

```python
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
```

**Output:**

```text
train rows 2,804,309 | validation rows 402,911 | holdout starts 2026-09-21
```

```python
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
```

**Output:**

```text
If you are using MLflow Tracing, you can migrate your traces to Unity Catalog for unlimited storage, fine-grained access controls, and queryability from notebooks, SQL, and dashboards. Learn more: https://docs.databricks.com/aws/en/mlflow3/genai/tracing/migrate-traces-to-uc
```
```text
Training until validation scores don't improve for 50 rounds
[100]	valid_0's tweedie: 42.9263
[200]	valid_0's tweedie: 42.9235
[300]	valid_0's tweedie: 42.9227
[400]	valid_0's tweedie: 42.9224
[500]	valid_0's tweedie: 42.9222
Early stopping, best iteration is:
[492]	valid_0's tweedie: 42.9221
```
```text
for c, g in va.groupby("category"):
🔗 View Logged Model at: https://e2-demo-field-eng.cloud.databricks.com/ml/experiments/3113677775769704/models/m-c0573028358242088051e7b555440063?o=1444828305810485
Registered model 'sabir_freshmart.ml.demand_forecaster' already exists. Creating a new version of this model...
```
```text
🔗 Created version '3' of model 'sabir_freshmart.ml.demand_forecaster': https://e2-demo-field-eng.cloud.databricks.com/explore/data/models/sabir_freshmart/ml/demand_forecaster/version/3?o=1444828305810485
```
```text
{
  "wape_model": 0.40199434458499445,
  "wape_legacy_trailing28": 0.4315406971151129,
  "wape_same_dow_4w": 0.4550617995545432,
  "bias_model": 0.00020309722053557522,
  "bias_legacy_trailing28": 0.027952935488610997,
  "wape_model_promo_days": 0.36687622929986186,
  "wape_legacy_promo_days": 0.41712214840824813,
  "bias_legacy_promo_days": -0.2548055205892804,
  "best_iteration": 492
}
```
| feature | gain |
|---|---|
| r28 | 77217665.28550386 |
| instock_r28 | 14667106.818288803 |
| tgt_dow | 3121806.2733888626 |
| dow_avg_4w | 3069439.248114109 |
| tgt_discount | 909903.3922023773 |
| days_to_holiday | 517385.5262079239 |
| category | 285413.69365787506 |
| tgt_month | 282362.1305999756 |
| r7 | 253236.96495342255 |
| on_hand_units | 219855.01999425888 |
| tgt_dom | 208883.32636499405 |
| unit_price | 140832.15524816513 |
| on_order_units | 133237.41793823242 |
| trend_7_28 | 89401.0133023262 |
| last_units | 83457.14524364471 |


### Accuracy by category (validation = last 6 weeks, in-stock days)

```python
by_cat = (va.groupby("category").apply(lambda g: pd.Series({
    "actual_units": g.y.sum(), "wape_model": wape(g.y, g.pred), "wape_legacy_28d": wape(g.y, g.r28),
    "legacy_bias_on_promo": (g.r28[g.tgt_discount > 0].sum() / max(1, g.y[g.tgt_discount > 0].sum()) - 1)}))
    .round(3).reset_index())
display(by_cat)
print(by_cat.to_string(index=False))
```

**Output:**

```text
by_cat = (va.groupby("category").apply(lambda g: pd.Series({
  by_cat = (va.groupby("category").apply(lambda g: pd.Series({
```
| category | actual_units | wape_model | wape_legacy_28d | legacy_bias_on_promo |
|---|---|---|---|---|
| Dairy | 792377.0 | 0.389 | 0.415 | -0.297 |
| Produce | 1206187.0 | 0.381 | 0.419 | -0.212 |
| Bakery | 367281.0 | 0.408 | 0.43 | -0.229 |
| Meat | 226446.0 | 0.451 | 0.479 | -0.302 |
| Frozen | 343242.0 | 0.428 | 0.447 | -0.225 |
| Beverages | 832713.0 | 0.4 | 0.436 | -0.241 |
| Snacks | 649819.0 | 0.398 | 0.424 | -0.342 |
| Pantry | 253035.0 | 0.435 | 0.453 | -0.192 |
| Household | 80876.0 | 0.518 | 0.533 | -0.252 |

```text
category  actual_units  wape_model  wape_legacy_28d  legacy_bias_on_promo
    Dairy      792377.0       0.389            0.415                -0.297
  Produce     1206187.0       0.381            0.419                -0.212
   Bakery      367281.0       0.408            0.430                -0.229
     Meat      226446.0       0.451            0.479                -0.302
   Frozen      343242.0       0.428            0.447                -0.225
Beverages      832713.0       0.400            0.436                -0.241
   Snacks      649819.0       0.398            0.424                -0.342
   Pantry      253035.0       0.435            0.453                -0.192
Household       80876.0       0.518            0.533                -0.252
```

### Promote to `@prod`
Gate = what actually causes stockouts, not a vanity accuracy number. Daily store-SKU demand at 5-30 units/day is mostly
Poisson noise (irreducible WAPE ~0.35-0.40), so the model must: (1) beat the legacy forecast on WAPE overall **and** on promo
days, and (2) be unbiased (|bias| <= 5%) — the legacy forecast's real failure is a large *negative* bias on promo days.

```python
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
```

**Output:**

```text
{"model": "sabir_freshmart.ml.demand_forecaster", "version": "3", "run_id": "21ea8e3fee5544f380d4d0f4e3e6650e", "promoted_to_prod": true, "wape_model": 0.402, "wape_legacy_trailing28": 0.4315, "wape_same_dow_4w": 0.4551, "bias_model": 0.0002, "bias_legacy_trailing28": 0.028, "wape_model_promo_days": 0.3669, "wape_legacy_promo_days": 0.4171, "bias_legacy_promo_days": -0.2548, "best_iteration": 492}
```

## Notebook exit value

```json
{
  "model": "sabir_freshmart.ml.demand_forecaster",
  "version": "3",
  "run_id": "21ea8e3fee5544f380d4d0f4e3e6650e",
  "promoted_to_prod": true,
  "wape_model": 0.402,
  "wape_legacy_trailing28": 0.4315,
  "wape_same_dow_4w": 0.4551,
  "bias_model": 0.0002,
  "bias_legacy_trailing28": 0.028,
  "wape_model_promo_days": 0.3669,
  "wape_legacy_promo_days": 0.4171,
  "bias_legacy_promo_days": -0.2548,
  "best_iteration": 492
}
```
