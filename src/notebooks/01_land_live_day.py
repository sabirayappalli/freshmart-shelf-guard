# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Land the next day's source files (2026-11-01)
# MAGIC Simulates the overnight drop from POS / WMS / DC. The pipeline update that follows should ingest **only these files**
# MAGIC (Auto Loader checkpoints) — no reprocessing of the year of history.

# COMMAND ----------

dbutils.widgets.text("catalog", "sabir_freshmart")
CATALOG = dbutils.widgets.get("catalog")
import os, shutil
landed = []
for sub in ["pos_sales", "inventory", "deliveries"]:
    src = f"/Volumes/{CATALOG}/raw/staged/{sub}"
    for f in sorted(os.listdir(src)):
        dst = f"/Volumes/{CATALOG}/raw/landing/{sub}/{f}"
        shutil.copy(os.path.join(src, f), dst)
        landed.append(dst)
print("\n".join(landed))
dbutils.notebook.exit(",".join(landed))
