# Job run 939105182118635 — `[dev sabir_ayappalli] freshmart_stockout_e2e`

Run page: https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#job/113063708631415/run/939105182118635

| task | state | result | duration (s) | evidence |
|---|---|---|---|---|
| land_raw_history | TERMINATED | SUCCESS | 105 | [land_raw_history.md](land_raw_history.md) |
| lakeflow_history | TERMINATED | SUCCESS | 114 | see lakeflow_pipeline_updates.md |
| land_live_day | TERMINATED | SUCCESS | 19 | [land_live_day.md](land_live_day.md) |
| lakeflow_incremental | TERMINATED | SUCCESS | 104 | see lakeflow_pipeline_updates.md |
| train_demand_forecast | TERMINATED | SUCCESS | 413 | [train_demand_forecast.md](train_demand_forecast.md) |
| backtest_policy_value | TERMINATED | SUCCESS | 164 | [backtest_policy_value.md](backtest_policy_value.md) |
| score_stockout_risk | TERMINATED | FAILED | 2275 | [score_stockout_risk.md](score_stockout_risk.md) |
| score_stockout_risk | TERMINATED | FAILED | 2265 | [score_stockout_risk.md](score_stockout_risk.md) |
