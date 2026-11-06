"""Build the FreshMart Genie Agent definition (serialized_space v2) as code.

Usage:  python setup/genie_space.py > setup/genie_space.json
        databricks genie create-space <WAREHOUSE_ID> "$(cat setup/genie_space.json)" --title ... --profile ...
IDs are deterministic (md5 of the text) so re-generating produces a stable, diffable file.
"""
import hashlib
import json

C = "sabir_freshmart"
LB = "sabir_freshmart_lakebase"


def hid(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


SAMPLES = [
    "Which 10 stores lost the most sales to stockouts in October 2026?",
    "What is the out-of-stock rate on promo vs non-promo days by category?",
    "Show high-risk items in the Northeast with more than $200 revenue at risk",
    "How much revenue is at risk over the next 3 days by region?",
    "How many replenishment orders did store managers approve, and how many units?",
]

TEXT = [
    "FreshMart is an 80-store regional grocery chain (synthetic data). Users are merchandising analysts, supply-chain planners and store managers.",
    "KPIs: use the metric view sabir_freshmart.gold.stockout_metrics with MEASURE() for Net Sales, Estimated Lost Sales, Out Of Stock Rate, On Shelf Availability (OSA), Promo Out Of Stock Rate, Lost Sales Pct. Always group by the metric view's dimensions with GROUP BY ALL.",
    "Out of stock (OOS) = a store-SKU-day that ended with zero units on hand (is_oos). Lost sales are ESTIMATED from the in-stock run rate and are conservative.",
    "Forward-looking risk (next 3 days) lives in sabir_freshmart.ml.stockout_risk_scores: stockout_prob_3d (0-1), risk_tier (HIGH >= 0.5, MEDIUM 0.2-0.5, LOW), revenue_at_risk_usd, recommended_action (EXPEDITE_OR_TRANSFER, EXPEDITE_AND_ORDER, APPROVE_ORDER, MONITOR, NONE), recommended_order_units (extra units to order now), stockout_prob_order_window (risk in days lead_time..lead_time+2, when today's order would land) and stockout_prob_if_approved (same window after approving), revenue_at_risk_7d_usd, revenue_protected_if_approved_usd, inbound_3d_units, risk_drivers. It has one row per store-SKU for the latest as_of_date.",
    "AI-written explanations are in sabir_freshmart.serving.stockout_alerts.ai_reason; per-store morning briefings are in sabir_freshmart.serving.store_briefings.briefing.",
    "Manager actions taken in the Shelf Guard app (approve / override / dismiss, with order_units) are in sabir_freshmart_lakebase.app.replenishment_actions (live Lakebase Postgres table). Join to stockout alerts on alert_id.",
    "'Last month' means the latest complete calendar month in the data; the data ends on the max business_date in gold.store_sku_daily, not today's date. Prefer relative filters anchored on max(business_date).",
    "Format money as USD with no decimals, rates as percentages with one decimal.",
]

EXAMPLES = [
    ("Which 10 stores lost the most sales to stockouts in October 2026?",
     f"SELECT `Store`, `Region`, MEASURE(`Estimated Lost Sales`) AS est_lost_sales, MEASURE(`On Shelf Availability`) AS osa "
     f"FROM {C}.gold.stockout_metrics WHERE `Month` = DATE'2026-10-01' GROUP BY ALL ORDER BY est_lost_sales DESC LIMIT 10"),
    ("What is the out-of-stock rate on promo vs non-promo days by category?",
     f"SELECT `Category`, `On Promo`, MEASURE(`Out Of Stock Rate`) AS oos_rate FROM {C}.gold.stockout_metrics GROUP BY ALL ORDER BY `Category`, `On Promo`"),
    ("How much revenue is at risk over the next 3 days by region?",
     f"SELECT region, count_if(risk_tier = 'HIGH') AS high_risk_items, round(sum(revenue_at_risk_usd), 0) AS revenue_at_risk_usd "
     f"FROM {C}.ml.stockout_risk_scores GROUP BY region ORDER BY revenue_at_risk_usd DESC"),
    ("How many replenishment orders did store managers approve, and how many units?",
     f"SELECT action, count(*) AS actions, sum(order_units) AS units, round(sum(revenue_at_risk_usd), 0) AS revenue_protected_usd "
     f"FROM {LB}.app.replenishment_actions GROUP BY action ORDER BY actions DESC"),
]

BENCH = [
    ("What was the chain-wide on-shelf availability in October 2026?",
     f"SELECT MEASURE(`On Shelf Availability`) AS osa FROM {C}.gold.stockout_metrics WHERE `Month` = DATE'2026-10-01'"),
    ("Which category has the highest promo out-of-stock rate?",
     f"SELECT `Category`, MEASURE(`Promo Out Of Stock Rate`) AS promo_oos FROM {C}.gold.stockout_metrics GROUP BY ALL ORDER BY promo_oos DESC LIMIT 1"),
    ("How many store-SKUs are high risk right now?",
     f"SELECT count(*) AS high_risk FROM {C}.ml.stockout_risk_scores WHERE risk_tier = 'HIGH'"),
    ("Total estimated lost sales by region for the last 12 months",
     f"SELECT `Region`, MEASURE(`Estimated Lost Sales`) AS lost FROM {C}.gold.stockout_metrics GROUP BY ALL ORDER BY lost DESC"),
]

COLS_RISK = [
    {"column_name": "category", "enable_entity_matching": True, "enable_format_assistance": True},
    {"column_name": "region", "enable_entity_matching": True, "enable_format_assistance": True},
    {"column_name": "risk_tier", "enable_entity_matching": True, "enable_format_assistance": True},
    {"column_name": "store_name", "enable_entity_matching": True, "synonyms": ["store", "location"]},
    {"column_name": "sku_name", "enable_entity_matching": True, "synonyms": ["item", "product"]},
    {"column_name": "stockout_prob_3d", "description": ["Probability (0-1) of running out within 3 days"], "synonyms": ["stockout risk", "OOS risk"]},
    {"column_name": "revenue_at_risk_usd", "synonyms": ["sales at risk", "revenue at risk"]},
]

space = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": hid("sq" + q), "question": [q]} for q in SAMPLES], key=lambda x: x["id"])},
    "data_sources": {
        "tables": sorted([
            {"identifier": f"{C}.gold.store_daily_kpis"},
            {"identifier": f"{C}.gold.store_sku_daily"},
            {"identifier": f"{C}.ml.stockout_risk_scores", "column_configs": sorted(COLS_RISK, key=lambda c: c["column_name"])},
            {"identifier": f"{C}.serving.stockout_alerts"},
            {"identifier": f"{C}.serving.store_briefings"},
            {"identifier": f"{LB}.app.replenishment_actions"},
        ], key=lambda t: t["identifier"]),
        "metric_views": [{"identifier": f"{C}.gold.stockout_metrics"}],
    },
    "instructions": {
        "text_instructions": [{"id": hid("text"), "content": TEXT}],
        "example_question_sqls": sorted([{"id": hid("ex" + q), "question": [q], "sql": [s]} for q, s in EXAMPLES], key=lambda x: x["id"]),
    },
    "benchmarks": {"questions": sorted([{"id": hid("bm" + q), "question": [q], "answer": [{"format": "SQL", "content": [s]}]} for q, s in BENCH],
                                       key=lambda x: x["id"])},
}

if __name__ == "__main__":
    print(json.dumps(space, indent=1))
