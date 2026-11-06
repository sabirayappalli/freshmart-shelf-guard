# land_live_day

- Task run id: `855472575981935`  
- Result: **SUCCESS**  
- Duration: 19s

# 01 · Land the next day's source files (2026-11-01)
Simulates the overnight drop from POS / WMS / DC. The pipeline update that follows should ingest **only these files**
(Auto Loader checkpoints) — no reprocessing of the year of history.

```python
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
```

**Output:**

```text
/Volumes/sabir_freshmart/raw/landing/pos_sales/pos_2026_11_01.csv,/Volumes/sabir_freshmart/raw/landing/inventory/inv_2026_11_01.parquet,/Volumes/sabir_freshmart/raw/landing/deliveries/deliveries_2026_11_01.json
```

## Notebook exit value

```json
/Volumes/sabir_freshmart/raw/landing/pos_sales/pos_2026_11_01.csv
/Volumes/sabir_freshmart/raw/landing/inventory/inv_2026_11_01.parquet
/Volumes/sabir_freshmart/raw/landing/deliveries/deliveries_2026_11_01.json
```
