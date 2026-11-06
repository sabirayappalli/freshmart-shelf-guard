# Execution evidence (all plain text, all from real runs on Databricks)

Workspace: `e2-demo-field-eng` · Catalog: `sabir_freshmart` · Job: `freshmart_stockout_e2e` · Data: synthetic.

| Stage | Evidence | Key facts you can read in the file |
|---|---|---|
| Raw data landed | [run_939105182118635…/land_raw_history.md](run_939105182118635_ingest_train_backtest/land_raw_history.md) | 5,856,000 store-SKU-days simulated; files per source; baseline 4.91% OOS, 14.78% promo OOS, $9.41M true lost sales |
| Lakeflow | [run_939105182118635…/lakeflow_pipeline_updates.md](run_939105182118635_ingest_train_backtest/lakeflow_pipeline_updates.md) | History update: 5.84M inventory rows, 480 bad POS rows dropped by expectations. Incremental update: 16,000 rows (one day) |
| ML training | [run_939105182118635…/train_demand_forecast.md](run_939105182118635_ingest_train_backtest/train_demand_forecast.md) | 2.8M training rows; WAPE 0.402 vs 0.432 legacy; promo-day bias −25.5% → ≈0; promoted to `@prod` |
| Value backtest | [run_939105182118635…/backtest_policy_value.md](run_939105182118635_ingest_train_backtest/backtest_policy_value.md) | Lost sales −82.5%, OSA 95.6% → 99.2%, promo OOS 12.9% → 1.6%, +$236K inventory, +$1.34M/yr margin |
| Risk scoring + calibration | [run_314679096507983…/score_stockout_risk.md](run_314679096507983_score_genai_governance/score_stockout_risk.md) | Calibration table by band; AUC 0.81 (3-day), 0.83 (order window); 2.7-min runtime |
| GenAI | [run_314679096507983…/genai_alerts_and_publish.md](run_314679096507983_score_genai_governance/genai_alerts_and_publish.md) | 1,500 grounded alert reasons + 80 store briefings, verbatim; non-empty quality gate passed |
| Governance (job) | [run_314679096507983…/governance_and_semantics.md](run_314679096507983_score_genai_governance/governance_and_semantics.md) | Row filter, tags, comments, metric view, grants: all OK |
| Governance (enforced) | [governance_checks.md](governance_checks.md) | Same query: 16,000 rows as HQ vs 0 without a region mapping; `unit_cost` masked; lineage edges; grants |
| Lakebase + App + Genie | [lakebase_genie_app.md](lakebase_genie_app.md) | 6 ms Lakebase reads; approval written and read back; Genie answers 4 questions incl. one from the live Lakebase actions table |
| Cost | [run_cost.md](run_cost.md) | `system.billing` DBUs and list $ per run |

Regenerate: `python tools/export_run_evidence.py <run_id> <profile> evidence/<dir>` · `python tools/export_pipeline_evidence.py <pipeline_id> <profile> <file>` ·
`python tools/capture_app_evidence.py <profile> <app_url> <file>` · `bash tools/capture_governance_evidence.sh <profile> <file>`
