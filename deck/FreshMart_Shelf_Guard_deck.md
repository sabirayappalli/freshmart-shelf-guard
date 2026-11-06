# FreshMart Shelf Guard
### Stop losing sales to empty shelves: fix it before the truck leaves the DC

*Prepared for: COO / VP Supply Chain (executive sponsor) · Director of Store Replenishment (domain owner)*

*All figures from a working prototype on synthetic data modelled on grocery benchmarks. No customer data.*

---

## 1 · The outcome, up front

> **Recover ~$5.5M a year in lost sales on the 200 items that matter most, and take promo stockouts from 13% to under 2%, for about $240K of extra inventory.**

| | Today | With Shelf Guard |
|---|---|---|
| On-shelf availability | 95.6% | **99.2%** |
| Promo out-of-stock rate | 12.9% | **1.6%** |
| Sales lost to empty shelves | 2.0% of demand | **0.35%** |
| Gross margin, after extra shrink | — | **+$1.34M / yr** |

*Measured by replaying the last 6 weeks, held out from training, on identical customer demand. Only the ordering decision changes.*

---

## 2 · The problem in the buyer's KPIs

- **$9.4M a year walks out the door** on 200 high-velocity items across 80 stores: **2.7% of potential sales**.
- **Promotions are where it hurts: 14.8%** of promoted store-SKU-days end with an empty shelf, against 4.4% on other days. FreshMart pays for the circular, the customer comes in, and the shelf is empty.
- **Root cause:** replenishment orders off a trailing 28-day average. It **under-forecasts promo demand by 25%** and can't see holidays, so it under-orders exactly when demand spikes.
- **Store managers find out from the empty shelf.** Today's reports show yesterday's stockouts, when it's too late to act.

*Note: POS can only see sales, not the customers who found an empty shelf. Our gold-layer estimate of lost sales ($4.6M) is about half the true figure ($9.4M), so the problem is at least as big as the reports say.*

---

## 3 · What changes for the store manager (the domain owner's view)

**Every morning at 7am**, each manager opens Shelf Guard and sees:

1. **A three-bullet briefing** written by AI from that store's own numbers: what's at risk, which orders to approve before the 10am DC cutoff.
2. **A ranked list of items that will run out** if they rely on the current system, sorted by **revenue at risk** rather than alphabetically, each with **one recommended action**:
    - **Approve order** (1,491 items today): the suggested extra units cut the stockout risk in the window the order lands in from **21% to 2%**.
    - **Expedite / transfer** (992 items today): the shelf empties before any new order can land, so call the DC or the nearest store.
    - Each item shows on hand, deliveries due, the probability before and after, and one line of *why*.
3. **One click: Approve / Edit / Dismiss.** The decision is saved instantly (Lakebase) and visible to HQ and to Genie.

> **Morning briefing, FreshMart Salt Lake City #1065 (verbatim AI output):**
>
> - **$1,233 at risk over 3 days** (14 high, 13 medium). Approve expedites before the 10am DC cutoff.
> - **Northstar Coffee Classic:** 0 on hand, 155 units demand, 25% promo. Expedite or transfer now, since stock empties before a standard order lands.
> - **Blue Ridge Cookies Organic** (3 on hand, 69 units, promo) and **Northstar Baking Lite** (0 on hand, 65 units): approve expedites or transfers today.
>
> **Item alert, FreshMart Raleigh #1040 · Sunny Day Paper Towels, recommended action APPROVE ORDER:**
> *"Forecast demand of 30.0 units (1.18x normal, 25% off promo) exceeds 24 on hand; approve the 30-unit order to cut days 6-7 stockout risk from 43.0% to 2.0%."*

For items where a new order can't arrive in time (long-lead dry goods), the alert says so: **"expedite or transfer from a nearby store"**, not "order more".

---

## 4 · What changes for the executive sponsor

| Sponsor KPI | Impact | How we know |
|---|---|---|
| **Comparable-store sales** | +1.7% on the modelled assortment | Backtest net sales $37.85M → $38.49M in 6 weeks |
| **Gross margin $** | **+$1.34M / yr** after the extra shrink | Margin computed after shrink, at cost |
| **Promo ROI** | Promo stockouts 12.9% → 1.6%, so funded promotions actually sell | Backtest promo OOS |
| **Working capital** | +$236K average inventory (+14%), the price of availability | Shown openly, not hidden |
| **Customer trust / NPS** | Shelf availability 95.6% → 99.2% | OSA in the governed metric view |

**The trade-off we're choosing:** about $240K more stock on the shelf, carefully targeted, in exchange for $5.5M more sales.
Shrink rises 3.6% (the cost of holding more perishables) and is already netted out of the margin figure.

---

## 5 · Value: three numbers, three levels of confidence

| Case | Scope | Annual sales recovered | Annual gross margin | Basis |
|---|---|---|---|---|
| **Measured** | 80 stores × 200 SKUs | **$5.5M** | **$1.34M** | 6-week hold-out replay, same demand |
| **Plan** *(what we'd commit to)* | same | **$2.8M** | **$0.67M** | Measured × 50% for execution reality (manager adoption, data latency, supplier fill) |
| **Chain potential** | Fresh + promoted assortment, about 45% of a ~$2.1B chain | **~$11M** | **~$2.6M** | Same lost-sales rate (2.7%), half the modelled recovery (41%) |

**Run cost:** one nightly serverless pipeline + model + batch AI. The **whole build to date cost $7.47 at list price** (`system.billing`, failed attempts included), so a nightly production run is **under ~$5, about $1.5K a year**.
**Payback:** under one quarter on the *plan* case.

---

## 6 · How it works (one platform, one copy of the data)

```
 POS · WMS (on hand + open POs) · DC receipts · promo calendar · store/item master
   │  Lakeflow (Auto Loader, expectations)            ← nightly, incremental, data-quality gated
   ▼
 Unity Catalog: bronze → silver → gold   + metric view (OSA, OOS, lost sales)   ← one definition of every KPI
   │                                       row filter by region · cost column masked
   ▼
 ML: demand forecast per store × SKU (MLflow, @prod gate) → 3-day stockout simulation → suggested order
   ▼
 GenAI: one-line reason per at-risk item + morning briefing per store (ai_query, grounded on facts)
   ▼
 Lakebase (Postgres): alerts served in milliseconds · manager approvals written back
   ▼
 Shelf Guard app (store view · chain impact) + Genie Agent ("which stores lost the most sales on promos last month?")
```

---

## 7 · Why trust the numbers

- **The model is graded on what causes stockouts.** Forecast bias on promo days goes from −25% to ~0%. Accuracy (WAPE) improves 0.43 → 0.40; daily demand is mostly noise, so that's the expected size.
- **The risk score is calibrated, and we checked.** Scored as of Oct 29 and compared with what actually happened Oct 30 – Nov 1: AUC **0.81**, **75%** of items flagged HIGH actually ran out, and the predicted rates line up with actuals in every band (e.g. predicted 62% → actual 67%; predicted 93% → actual 94%). Our first version of this score failed the same check (AUC 0.52) and was rebuilt. We show that too.
- **The value test isolates the decision.** Same customers, same demand, same starting shelves. Only the order rule changes. The ML policy is even fed the old (biased) history, so the result is conservative.
- **The AI can't make things up.** It sees only the row's numbers, and the numbers sit next to the sentence for audit.
- **Governed end to end.** Lineage from raw file to the app, regional row-level security, a cost-price mask, and one KPI definition shared by Genie and the app.

---

## 8 · Proposed next step: a 6-week, 10-store pilot

| Week | Activity | Success metric |
|---|---|---|
| 0–1 | Connect real POS/WMS feeds (Lakeflow Connect / file drops); backfill 12 months | Data-quality pass rate > 99.5% |
| 1–2 | Retrain on FreshMart history; calibrate risk by region | Risk AUC > 0.75 on hold-out |
| 2–6 | 10 pilot stores use Shelf Guard daily; 10 matched control stores | **OSA +2 pts, promo OOS −50%** vs control |
| 6 | Readout to COO: go / no-go for chain rollout | Measured margin ≥ plan case |

**Ask:** a named replenishment lead, POS/WMS feed access, and 10 pilot store managers.

---

## 9 · Demo script (tell → show → tell, 7 minutes)

1. **Tell (30s):** "$9.4M of sales lost on 200 items, and promos are 3x worse. Today managers find out from the empty shelf."
2. **Show, store view (2 min):** Salt Lake City #1065 → AI morning briefing → top alert *Northstar Coffee, 0 on hand, promo, expedite* → an *Approve order* item (risk 43% → 2%) → **Approve** → "saved to Lakebase" (6 ms server-side).
3. **Show, chain impact (1.5 min):** backtest chart, legacy vs ML lost sales per day → scorecard including the inventory and shrink *costs*.
4. **Show, Ask Genie (1.5 min):** "Which 10 stores lost the most sales to stockouts in October?" → SQL shown → "How many orders did managers approve today?" (it reads the live Lakebase table).
5. **Show, governance (1 min):** lineage raw → app; the same query as HQ returns all 16,000 rows, and as a user with no region mapping returns 0, with unit cost masked.
6. **Tell (30s):** "$1.3M margin measured, $0.67M we'd commit to, payback inside a quarter. Let's prove it in 10 stores."

---

## 10 · Objections we expect, and the answers

| From | Objection | Answer |
|---|---|---|
| **COO** | "Isn't this just more inventory?" | Inventory rises 14% but sales rise 1.7% and margin $1.3M *after* shrink. It's targeted: stock goes to promo and high-velocity items, not the whole store. The scorecard shows both sides. |
| **COO** | "It's synthetic. Why believe $5.5M?" | Don't. Commit to the $2.8M plan case and prove it with a 10-store, 6-week controlled pilot. The simulator is tuned to grocery benchmarks (5% OOS, 2.7% lost sales, 15% promo OOS). |
| **Replenishment director** | "My buyers won't trust a black box." | Every alert shows its drivers (promo %, holiday, demand vs normal, days of cover, inbound) and a plain-English reason. Managers can override, and overrides are logged to retrain on. |
| **Replenishment director** | "Will this replace our replenishment system?" | No. It sits on top: the system keeps ordering, Shelf Guard flags where it will fail and suggests the extra units. Phase 2 can feed suggested orders back into the system's order file. |
| **CIO / data lead** | "Another tool, another copy of the data?" | One platform: ingestion, governance, ML, AI, the database and the app all run on the same Unity Catalog tables. The Postgres layer is a synced view of governed data, not a new source of truth. |
| **CIO / data lead** | "What about access control: can store A see store B?" | A row filter on region and a column mask on supplier cost, enforced in SQL, Genie and notebooks alike. The app runs as a least-privilege service principal; per-user scoping uses on-behalf-of-user auth. |
| **CFO** | "What does it cost to run?" | Under ~$5 per nightly run at list price (about $1.5K a year), measured from `system.billing`. That's well under 1% of the plan-case margin. Lakebase scales to zero overnight. |
| **Data scientist** | "WAPE only improved 7%?" | Store-SKU-day demand is mostly noise (the floor is ~0.35). The win is removing the −25% promo bias and producing calibrated probabilities (AUC 0.81, 75% HIGH precision). The backtest prices it in dollars. |
