# FreshMart Shelf Guard — stopping grocery stockouts before the shelf goes empty

> **Industry:** Grocery retail · **Buyer:** VP Supply Chain / COO (sponsor), Director of Store Replenishment (domain owner)
> **Data:** 100% synthetic (inventory simulator tuned to grocery benchmarks). No customer data.

## The outcome

**FreshMart**, a fictional 80-store regional grocer, loses **2.7% of potential sales on its 200 highest-velocity items ($9.4M a year)** because the shelf is empty when the customer arrives. Promoted items are the worst: **14.8% of promo store-SKU-days end out of stock**. The replenishment system orders off a trailing 28-day average and can't see promotions or holidays. Store managers find out from the empty shelf.

Shelf Guard forecasts demand for every store × SKU, simulates the next 3 days under the current ordering rule, and gives each store manager a ranked, AI-explained list of items that will run out unless they act. One click sends the order.

**Measured in a 6-week hold-out replay on identical customer demand** ([evidence](evidence/run_939105182118635_ingest_train_backtest/backtest_policy_value.md)):

| KPI | Today (legacy min/max) | With Shelf Guard | Change |
|---|---|---|---|
| Lost sales (share of demand) | 2.00% | 0.35% | **−82.5%** |
| On-shelf availability | 95.6% | **99.2%** | +3.6 pts |
| Promo out-of-stock rate | 12.9% | **1.6%** | −11.3 pts |
| Net sales (6 weeks) | $37.85M | $38.49M | +$0.64M |
| Avg inventory at cost | $1.65M | $1.88M | +$0.24M (+14%) *(the cost)* |
| Shrink at cost (6 weeks) | $0.644M | $0.668M | +3.6% *(the cost)* |
| Gross margin after shrink (6 weeks) | $11.31M | $11.46M | +$0.15M |

**Annualised value, modelled scope (80 stores × 200 SKUs):** **$5.5M sales recovered → $1.34M gross margin after extra shrink**,
for +$236K of working capital. The [deck](deck/FreshMart_Shelf_Guard_deck.md) also gives a 50%-discounted *plan* case and a full-assortment extrapolation, with the assumptions stated.

## The journey (integrated: each stage reads the previous stage's governed output)

```
 POS (CSV)  WMS inventory + open POs (Parquet)  DC receipts (JSON)  Promo calendar (JSON)  Store/item master (CSV)
        \___________________________ UC Volume  sabir_freshmart.raw.landing ___________________________/
                                                    │  Lakeflow Declarative Pipeline (SQL, serverless, Auto Loader)
                                                    ▼  expectations: drop void/negative rows, fail on negative inventory
   bronze.*_raw  ──►  silver.pos_sales / inventory_snapshots / deliveries / promo_calendar / products(cost MASK) / stores
                                                    ▼
             gold.store_sku_daily (5.86M rows: OOS flag, est. lost sales)  ·  gold.store_daily_kpis  ·  gold.stockout_metrics (METRIC VIEW)
                                                    │                                        │
                       ML: LightGBM demand forecaster (MLflow, UC model @prod)               │  Genie Agent (metric view + gold + ml + serving
                       → ml.stockout_risk_scores (Monte Carlo 3-day risk, ROW FILTER by region)  + Lakebase actions)
                       → ml.backtest_* (legacy vs ML policy replay)                          │
                                                    ▼                                        │
                       GenAI: ai_query (Claude Haiku 5.5) → serving.stockout_alerts.ai_reason, serving.store_briefings
                                                    │  Lakebase synced tables (TRIGGERED, CDF)
                                                    ▼
                       Lakebase Postgres  serving.*  (reads, ms)   +   app.replenishment_actions  (manager writes)
                                                    ▼
                       Databricks App "Shelf Guard" (FastAPI): store view · chain impact · Ask Genie
                                                    │  approvals visible back in UC via the Lakebase catalog → Genie: "how many orders were approved?"
```

| Stage | What runs | Where |
|---|---|---|
| **Lakeflow** | Serverless SQL pipeline, 6 Auto Loader streaming tables → 6 silver → 2 gold MVs; a separate incremental update proves only new files are read | [`src/pipeline/`](src/pipeline), [`resources/pipeline.yml`](resources/pipeline.yml) · [evidence](evidence/run_939105182118635_ingest_train_backtest/lakeflow_pipeline_updates.md) |
| **Unity Catalog** | Medallion schemas, region **row filter**, supplier-cost **column mask**, tags, column comments, **metric view**, least-privilege grants to the app SP, lineage | [`06_governance_and_semantics.py`](src/notebooks/06_governance_and_semantics.py) |
| **ML** | Global LightGBM, direct multi-horizon (h=1–7), trained on in-stock days only; registered `sabir_freshmart.ml.demand_forecaster@prod`; Monte Carlo risk; policy backtest | [`02`](src/notebooks/02_train_demand_forecast.py) · [`03`](src/notebooks/03_score_stockout_risk.py) · [`04`](src/notebooks/04_backtest_policy_value.py) |
| **GenAI** | `ai_query` batch: one-line reason + action per at-risk item, 3-bullet morning briefing per store, grounded only on the row's facts | [`05`](src/notebooks/05_genai_alerts_and_publish.py) |
| **Lakebase** | Autoscaling project `freshmart-stockout`; synced tables for alerts + briefings; app-owned `app.replenishment_actions` | [`setup/lakebase_setup.sh`](setup/lakebase_setup.sh) |
| **Genie Agent** | Space over the metric view, gold, ML, serving and the live Lakebase actions table; instructions, example SQL, benchmarks as code | [`setup/genie_space.py`](setup/genie_space.py) |
| **Databricks App** | FastAPI + HTML: Lakebase reads/writes, metric-view KPIs, backtest charts, Genie chat | [`app/`](app) |

Everything is orchestrated by **one Declarative Automation Bundle and one Lakeflow Job** (`freshmart_stockout_e2e`):
`land_raw_history → lakeflow_history → land_live_day → lakeflow_incremental → train_demand_forecast → {score_stockout_risk → genai_alerts_and_publish, backtest_policy_value} → governance_and_semantics`.

## Evidence that it ran (all text, all committed)

| What | File |
|---|---|
| Executed notebooks with real outputs: raw landing, model training, policy backtest | [`evidence/run_939105182118635_ingest_train_backtest/`](evidence/run_939105182118635_ingest_train_backtest/README.md) |
| Executed notebooks: risk scoring + calibration, GenAI alerts/briefings, governance | [`evidence/run_314679096507983_score_genai_governance/`](evidence/run_314679096507983_score_genai_governance/README.md) |
| Lakeflow event log: rows per flow, expectation pass/fail (480 bad POS rows dropped), incremental proof (2nd update read 16,000 inventory rows, not 5.84M) | [`lakeflow_pipeline_updates.md`](evidence/run_939105182118635_ingest_train_backtest/lakeflow_pipeline_updates.md) |
| Deployed app driven end to end: Lakebase reads (6 ms server-side), an approval written to Lakebase, metric-view KPIs, 4 Genie Q&As incl. one answered from the live Lakebase actions table | [`evidence/lakebase_genie_app.md`](evidence/lakebase_genie_app.md) |
| UC policies *enforced*: same query as HQ (16,000 rows, cost visible) vs. without a region mapping (0 rows, cost masked); tags; lineage; grants | [`evidence/governance_checks.md`](evidence/governance_checks.md) |
| Run cost from `system.billing.usage` | [`evidence/run_cost.md`](evidence/run_cost.md) |

> Note: the first run's index lists two failed `score_stockout_risk` attempts. Those are real failures that were fixed (see [`docs/DECISIONS.md`](docs/DECISIONS.md) #7, #15, #16); the successful re-run is in the second evidence folder.

## Risk score quality (out-of-sample calibration check)

Scored as of 2026-10-29, compared with what actually happened on Oct 30 – Nov 1 ([evidence](evidence/run_314679096507983_score_genai_governance/score_stockout_risk.md)):

| Predicted P(stockout in 3 days) | Store-SKUs | Mean predicted | Actual stockout rate |
|---|---|---|---|
| < 5% | 7,629 | 2.3% | 3.9% |
| 5–20% | 6,034 | 10.5% | 14.0% |
| 20–50% | 1,721 | 30.9% | 36.7% |
| 50–80% | 409 | 62.4% | 66.7% |
| > 80% | 207 | 92.8% | 93.7% |

AUC **0.81** (vs 0.77 for the same simulation driven by a 28-day average). **75%** of HIGH-tier items really ran out. The order-window score
(*"if you approve today's suggested order, will the shelf still empty when it lands?"*) has AUC **0.83**.
Today's run recommends **1,491 orders to approve** (risk in their window drops from 21% to 2%) and **992 expedite/transfer** calls where no new order can land in time.

## Model quality (validation = last 6 weeks, in-stock days)

| Metric | Legacy trailing-28d | Model |
|---|---|---|
| WAPE, all days | 0.432 | **0.402** |
| WAPE, promo days | 0.417 | **0.367** |
| Bias, promo days | **−25.5%** (under-forecasts the spike) | ≈0% |

Daily store-SKU demand at 5–30 units is mostly Poisson noise, so WAPE alone understates the value. The money comes from removing the promo-day bias and from turning forecasts into **calibrated probabilities**. See [`docs/DECISIONS.md`](docs/DECISIONS.md) #5–#8.

## Reproduce

```bash
databricks bundle deploy --profile <profile>                 # pipeline + job
databricks bundle run freshmart_e2e --profile <profile>      # ~45 min end to end on serverless
bash setup/lakebase_setup.sh                                 # Lakebase project, UC catalog, synced tables, grants
python setup/genie_space.py > setup/genie_space.json         # Genie Agent definition
databricks apps deploy freshmart-shelf-guard --source-code-path /Workspace/.../app
python tools/export_run_evidence.py <run_id> <profile> evidence/<dir>   # executed notebooks -> markdown
```

## More

- [`docs/DECISIONS.md`](docs/DECISIONS.md): 14 decisions with alternatives and trade-offs, plus known limitations
- [`docs/AI_AS_TEAMMATE.md`](docs/AI_AS_TEAMMATE.md): how Claude Code was used, the patterns, and where the human had to decide
- [`deck/`](deck): business deck (outcome first, KPIs, sponsor + domain-owner value, demo script, objection handling)
