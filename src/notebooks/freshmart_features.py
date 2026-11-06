"""Shared feature engineering + model wrapper for the FreshMart demand forecaster.

Used by training (02), scoring (03) and the backtest (04) so features are computed identically everywhere.
All features for a row with feature_date t use only data <= t (point-in-time correct).
"""
from __future__ import annotations

import datetime as dt
import numpy as np
import pandas as pd

try:
    import mlflow.pyfunc
    _Base = mlflow.pyfunc.PythonModel
except Exception:  # pragma: no cover - lets the module import where mlflow is absent
    _Base = object

CATEGORIES = ["Dairy", "Produce", "Bakery", "Meat", "Frozen", "Beverages", "Snacks", "Pantry", "Household"]
FORMATS = ["Supercenter", "Neighborhood", "Express"]
REGIONS = ["Northeast", "Southeast", "Midwest", "West"]
CATEGORICAL = ["category", "store_format", "region"]
# US retail calendar (public knowledge, published years ahead)
HOLIDAYS = [dt.date(2025, 11, 27), dt.date(2025, 12, 25), dt.date(2026, 1, 1), dt.date(2026, 2, 8), dt.date(2026, 5, 25),
            dt.date(2026, 7, 4), dt.date(2026, 9, 7), dt.date(2026, 11, 26), dt.date(2026, 12, 25)]

FEATURES = ["h", "tgt_dow", "tgt_dom", "tgt_month", "tgt_discount", "days_to_holiday", "is_holiday",
            "category", "store_format", "region", "perishable", "unit_price", "lead_time_days",
            "r7", "r28", "instock_r28", "dow_avg_4w", "last_units", "oos_days_7", "trend_7_28",
            "on_hand_units", "on_order_units", "promo_today"]


def build_features(spark, daily):
    """daily = gold.store_sku_daily. Returns one row per (store_id, sku_id, feature_date)."""
    from pyspark.sql import functions as F, Window as W
    w = W.partitionBy("store_id", "sku_id").orderBy("business_date")
    d = daily.select("business_date", "store_id", "sku_id", "category", "store_format", "region", "perishable", "unit_price",
                     "lead_time_days", "units_sold", "is_oos", "on_hand_units", "on_order_units", "inbound_d1", "inbound_d2", "inbound_d3",
                     "promo_discount_pct")
    out = (d
           .withColumn("r7", F.avg("units_sold").over(w.rowsBetween(-6, 0)))
           .withColumn("r28", F.avg("units_sold").over(w.rowsBetween(-27, 0)))
           .withColumn("instock_r28", F.avg(F.when(~F.col("is_oos"), F.col("units_sold"))).over(w.rowsBetween(-27, 0)))
           .withColumn("oos_days_7", F.sum(F.col("is_oos").cast("int")).over(w.rowsBetween(-6, 0)))
           .withColumn("_n", F.count("*").over(w.rowsBetween(-27, 0))))
    # same-weekday averages for each weekday offset k (k days ago, k+7, k+14, k+21) -> picked per horizon later
    for k in range(7):
        lags = [F.lag("units_sold", k + 7 * j).over(w) for j in range(4)]
        out = out.withColumn(f"dowavg_{k}", (lags[0] + lags[1] + lags[2] + lags[3]) / 4.0)
    return (out.where(F.col("_n") >= 28)
            .withColumnRenamed("business_date", "feature_date")
            .withColumnRenamed("units_sold", "last_units")
            .withColumnRenamed("promo_discount_pct", "promo_today")
            .withColumn("trend_7_28", F.col("r7") / F.greatest(F.col("r28"), F.lit(0.1)))
            .drop("_n", "is_oos"))


def calendar_features(spark, daily, extra_days: int = 21):
    from pyspark.sql import functions as F
    lo, hi = daily.agg(F.min("business_date"), F.max("business_date")).first()
    days = pd.date_range(lo, hi + dt.timedelta(days=extra_days)).date
    rows = []
    for d in days:
        nxt = min([(h - d).days for h in HOLIDAYS if (h - d).days >= 0] or [99])
        rows.append(dict(target_date=d, tgt_dow=d.weekday(), tgt_dom=d.day, tgt_month=d.month,
                         days_to_holiday=min(nxt, 14), is_holiday=int(nxt == 0)))
    return spark.createDataFrame(pd.DataFrame(rows))


def to_model_frame(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    if "dow_avg_4w" not in pdf:
        offs = ((7 - pdf["h"].astype(int)) % 7).values
        mat = pdf[[f"dowavg_{k}" for k in range(7)]].to_numpy(dtype=float)
        pdf["dow_avg_4w"] = mat[np.arange(len(pdf)), offs]
    pdf["category"] = pd.Categorical(pdf["category"], categories=CATEGORIES)
    pdf["store_format"] = pd.Categorical(pdf["store_format"], categories=FORMATS)
    pdf["region"] = pd.Categorical(pdf["region"], categories=REGIONS)
    pdf["perishable"] = pdf["perishable"].astype(int)
    for c in FEATURES:
        if c not in CATEGORICAL:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype(float)
    return pdf


class ForecastModel(_Base):
    """pyfunc: returns mean daily units and std (negative-binomial: var = mu + phi_cat * mu^2)."""

    def __init__(self, booster, dispersion: dict):
        self.booster = booster
        self.dispersion = dispersion

    def predict(self, context, model_input, params=None):
        X = to_model_frame(model_input)[FEATURES]
        mu = np.clip(self.booster.predict(X, num_iteration=self.booster.best_iteration), 0, None)
        phi = X["category"].astype(str).map(self.dispersion).fillna(0.2).to_numpy()
        return pd.DataFrame({"mean_units": mu, "std_units": np.sqrt(mu + phi * mu ** 2)})
