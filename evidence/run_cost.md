# Run cost (system.billing.usage × list prices)

Captured 2026-11-06T21:57Z. Billing data lags a few hours; runs during the build include failed/iterative attempts.

```sql
SELECT u.usage_metadata.job_run_id AS job_run_id, u.billing_origin_product AS product, round(sum(u.usage_quantity), 2) AS dbus,
       round(sum(u.usage_quantity * p.pricing.effective_list.default), 2) AS list_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_date >= '2026-11-02' AND (u.usage_metadata.job_id = '113063708631415'
      OR u.usage_metadata.dlt_pipeline_id = '33b3763a-8e86-4849-9c1e-141f8273552d')
GROUP BY ALL ORDER BY 1, 2
```

```json
[
  {
    "dbus": "1.67",
    "job_run_id": "",
    "list_usd": "0.75",
    "product": "DLT"
  },
  {
    "dbus": "1.68",
    "job_run_id": "169986834634516",
    "list_usd": "0.76",
    "product": "JOBS"
  },
  {
    "dbus": "2.22",
    "job_run_id": "169986834634516",
    "list_usd": "0.16",
    "product": "MODEL_SERVING"
  },
  {
    "dbus": "5.90",
    "job_run_id": "300304639245199",
    "list_usd": "2.66",
    "product": "JOBS"
  },
  {
    "dbus": "0.03",
    "job_run_id": "588968642183638",
    "list_usd": "0.01",
    "product": "JOBS"
  },
  {
    "dbus": "1.43",
    "job_run_id": "671324601534710",
    "list_usd": "0.64",
    "product": "JOBS"
  },
  {
    "dbus": "2.49",
    "job_run_id": "691721739585203",
    "list_usd": "1.12",
    "product": "JOBS"
  },
  {
    "dbus": "2.07",
    "job_run_id": "691721739585203",
    "list_usd": "0.14",
    "product": "MODEL_SERVING"
  },
  {
    "dbus": "0.48",
    "job_run_id": "820765430740121",
    "list_usd": "0.22",
    "product": "JOBS"
  },
  {
    "dbus": "2.25",
    "job_run_id": "939105182118635",
    "list_usd": "1.01",
    "product": "JOBS"
  }
]
```
