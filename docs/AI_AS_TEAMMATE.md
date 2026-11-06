# How AI was used to build this (force multiplier, not autopilot)

Built in one working session with **Claude Code (Claude Opus 5.5)** in the terminal, driving the Databricks CLI against the
`e2-demo-field-eng` workspace. The human set direction and made the judgement calls; the AI did the typing, the CLI work
and the first-pass debugging.

## Division of labour

| Human (FE) decided | AI (Claude Code) did |
|---|---|
| Industry + problem: grocery stockouts on fresh + promoted items | Drafted the end-to-end architecture across all 6 stages |
| Workspace, catalog naming, public repo | Wrote the simulator, 7 notebooks, the Lakeflow SQL pipeline, DAB, app, Genie spec |
| Which KPIs the buyer cares about (OSA, lost sales, promo OOS, shrink) | Tuned the simulator until baseline KPIs matched grocery benchmarks (≈5% OOS, ≈3% lost sales, ≈15% promo OOS) |
| Accept/reject trade-offs (see `DECISIONS.md`) | Ran every job, read failures, fixed and re-ran; exported executed notebooks as text evidence |

## Patterns that worked

1. **Skills-first, not memory-first.** Before touching each product the agent loaded the Databricks skill for it
   (`databricks-pipelines`, `databricks-lakebase`, `databricks-apps-python`, `databricks-genie-agents`, `databricks-metric-views`,
   `dataviz`). That avoided stale API guesses: e.g. it used Lakebase **Autoscaling** (`databricks postgres …`), not the
   retired Provisioned API, and created synced tables via `create-synced-table` rather than the deprecated DAB resource.
2. **Validate the data before building on it.** The simulator was run locally first (`uv run`) and its KPIs printed. The
   first version produced only 0.5% lost sales — unrealistically good — so the legacy policy was made leaner until it
   matched grocery reality. Building ML on an unrealistic baseline would have produced a fake ROI.
3. **Honest gates.** The first model-promotion gate ("beat legacy WAPE by 10%") failed (7% better). Instead of loosening
   it silently, the gate was re-designed around what causes stockouts: unbiased forecasts and better promo-day accuracy
   (legacy under-forecasts promo demand by 25%). The change and its rationale are in notebook 02.
4. **Same engine for history and backtest.** The ROI comes from replaying the hold-out weeks through the *same* simulator
   on the *same* demand with only the ordering policy swapped — so the value claim is apples-to-apples and reproducible.
5. **Calibration checks catch the agent's own mistakes.** The first risk score looked plausible but had AUC 0.52 against what actually
   happened. Root cause: it assumed nobody reorders for 3 days. The redesign (Monte Carlo under the real ordering rule, using WMS open-PO
   due dates) was prototyped locally against the simulator (AUC 0.83 ceiling) *before* spending a cloud run on it.
6. **Diagnose with system tables, not guesses.** A 39-minute scoring step was traced via `system.query.history` (no Spark statements in the gap)
   to driver-side pandas; moving the aggregation to Spark cut it to 2.7 min.
7. **Quality gates on GenAI output.** `ai_query` returned empty strings under a `max_tokens` cap. An explicit "non-empty" assertion failed the task
   instead of silently publishing blank alerts.
8. **Evidence as text.** `tools/export_run_evidence.py` decodes each executed job-task notebook into Markdown with its
   real outputs, so the reviewer can read results rather than trust screenshots.

## Representative prompts

- *"Build a working prototype that solves a customer problem in a specific industry … Lakeflow → UC → Lakebase → ML/GenAI → Genie → App"* (the brief)
- *"Pick retail stockout prevention; build in e2-demo-field-eng; public repo"* (scoping answers)
- In-loop agent self-checks: "simulator KPIs too optimistic vs grocery benchmarks — make legacy policy leaner", "gate failed — is the gate measuring the business problem?"

## Where AI needed a human

- Choosing the value framing (lost sales recovered *and* the shrink/inventory cost of holding more stock) — a CFO will ask.
- Deciding that the gold layer's lost-sales *estimate* (censored POS) should be shown as conservative vs the simulator truth.
- Pushing to GitHub / publishing — the agent asked before any outward-facing action.
