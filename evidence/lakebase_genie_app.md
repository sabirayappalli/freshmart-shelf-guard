# Lakebase · App · Genie — recorded end-to-end run

App: https://freshmart-shelf-guard-1444828305810485.aws.databricksapps.com  ·  captured 2026-11-06 16:53:59 EST

All responses below are verbatim JSON from the deployed Databricks App (trimmed only where marked).

## 1 · Lakebase serving (synced from Unity Catalog)

### Health: row count + server-side Lakebase query latency

`GET /api/health` — round trip from laptop **470 ms**

Response:
```json
{
  "lakebase_rows": 16000,
  "lakebase_ms": 5.9
}
```

### Stores ranked by revenue at risk, with the AI morning briefing (top 2 shown)

`GET /api/stores` — round trip from laptop **661 ms**

Response:
```json
[
  {
    "store_id": "S1065",
    "store_name": "FreshMart Salt Lake City #1065",
    "region": "West",
    "high_risk_items": 14,
    "medium_risk_items": 13,
    "revenue_at_risk_usd": 1233.0,
    "briefing": "- **$1,233 at risk over 3 days** (14 high, 13 medium). Approve expedites before the 10am DC cutoff.\n- **Northstar Coffee Classic:** 0 on hand, 155 units demand, 25% promo. Expedite or transfer now, since stock empties before a standard order lands.\n- **Blue Ridge Cookies Organic** (3 on hand, 69 units, promo) and **Northstar Baking Lite** (0 on hand, 65 units): approve expedites or transfers today.",
    "as_of_date": "2026-11-01"
  },
  {
    "store_id": "S1080",
    "store_name": "FreshMart Salt Lake City #1080",
    "region": "West",
    "high_risk_items": 13,
    "medium_risk_items": 15,
    "revenue_at_risk_usd": 1178.0,
    "briefing": "- **Risk:** 13 high-risk and 15 medium-risk items, with $1,178 in revenue at risk over 3 days.\n- **Approve before 10am DC cutoff:** Harvest Co Juice Value (143 units demand, zero stock) and Northstar Coffee Classic (136 demand, 1.2 days cover).\n- **Expedite or transfer from nearby stores:** Blue Ridge Cookies Organic, FreshMart Meals Organic (both out, on promo), and Valley Farms Ice Cream Organic (out, 37 demand).",
    "as_of_date": "2026-11-01"
  }
]
```

### At-risk items for FreshMart Salt Lake City #1065 (top 5 shown)

`GET /api/alerts?store_id=S1065` — round trip from laptop **468 ms**

Response:
```json
{
  "store_id": "S1065",
  "query_ms": 7.3,
  "alerts": [
    {
      "alert_id": "S1065-SKU00139",
      "sku_id": "SKU00139",
      "sku_name": "Northstar Coffee Classic",
      "category": "Beverages",
      "on_hand_units": 0,
      "on_order_units": 312,
      "forecast_units_3d": 154.9,
      "days_of_cover": 0.0,
      "stockout_prob_3d": 1.0,
      "risk_tier": "HIGH",
      "revenue_at_risk_usd": 351.48,
      "recommended_order_units": 48,
      "recommended_action": "EXPEDITE_AND_ORDER",
      "order_window_start_day": 4,
      "order_window_end_day": 6,
      "stockout_prob_order_window": 0.11,
      "stockout_prob_if_approved": 0.005,
      "revenue_protected_if_approved_usd": 25.11,
      "inbound_3d_units": 312,
      "lead_time_days": 4,
      "max_promo_discount_3d": 0.25,
      "promo_type": "Circular",
      "risk_drivers": "promo 25% off (Circular); only 0.0 days of cover; out of stock 1 of last 7 days; a new order would land after the shelf empties: expedite or transfer from a nearby store",
      "ai_reason": "Northstar Coffee Classic will likely stock out within 3 days: 0 units on hand against 155.0 units of forecast demand (1.21x normal, boosted by a 25% off circular), with 100.0% stockout probability and $351.0 revenue at risk, so expedite the 312 units arriving within 3 days and place an extra 48-unit order now to cut days 4-6 stockout odds from 11.0% to 1.0%.",
      "case_pack": 24,
      "last_action": null,
      "last_order_units": null,
      "last_action_at": null
    },
    {
      "alert_id": "S1065-SKU00144",
      "sku_id": "SKU00144",
      "sku_name": "Blue Ridge Cookies Organic",
      "category": "Snacks",
      "on_hand_units": 3,
      "on_order_units": 162,
      "forecast_units_3d": 68.9,
      "days_of_cover": 0.1,
      "stockout_prob_3d": 1.0,
      "risk_tier": "HIGH",
      "revenue_at_risk_usd": 129.97,
      "recommended_order_units": 18,
      "recommended_action": "EXPEDITE_OR_TRANSFER",
      "order_window_start_day": 5,
      "order_window_end_day": 7,
      "stockout_prob_order_window": 0.01,
      "stockout_prob_if_approved": 0.0,
      "revenue_protected_if_approved_usd": 0.14,
      "inbound_3d_units": 72,
      "lead_time_days": 5,
      "max_promo_discount_3d": 0.25,
      "promo_type": "Circular",
      "risk_drivers": "promo 25% off (Circular); demand 1.3x normal run-rate; only 0.1 days of cover; a new order would land after the shelf empties: expedite or transfer from a nearby store",
      "ai_reason": "Blue Ridge Cookies Organic will stock out in 3 days: 3 units vs 69.0 forecast demand (1.32x normal), $130.0 at risk. Expedite or transfer stock and order 18 units to cut days 5\u20137 stockout risk from 1.0% to 0.0%.",
      "case_pack": 18,
      "last_action": null,
      "last_order_units": null,
      "last_action_at": null
    },
    {
      "alert_id": "S1065-SKU00172",
      "sku_id": "SKU00172",
      "sku_name": "Northstar Baking Lite",
      "category": "Pantry",
      "on_hand_units": 0,
      "on_order_units": 141,
      "forecast_units_3d": 65.2,
      "days_of_cover": 0.0,
      "stockout_prob_3d": 1.0,
      "risk_tier": "HIGH",
      "revenue_at_risk_usd": 107.9,
      "recommended_order_units": 24,
      "recommended_action": "EXPEDITE_OR_TRANSFER",
      "order_window_start_day": 4,
      "order_window_end_day": 6,
      "stockout_prob_order_window": 0.095,
      "stockout_prob_if_approved": 0.045,
      "revenue_protected_if_approved_usd": 2.31,
      "inbound_3d_units": 141,
      "lead_time_days": 4,
      "max_promo_discount_3d": 0.0,
      "promo_type": null,
      "risk_drivers": "only 0.0 days of cover; out of stock 2 of last 7 days; a new order would land after the shelf empties: expedite or transfer from a nearby store",
      "ai_reason": "Zero on hand against 65 units of 3-day demand puts $108.0 at 100% stockout risk; expedite or transfer, and order 24 units, cutting days 4-6 risk from 10.0% to 5.0%.",
      "case_pack": 12,
      "last_action": null,
      "last_order_units": null,
      "last_action_at": null
    },
    {
      "alert_id": "S1065-SKU00193",
      "sku_id": "SKU00193",
      "sku_name": "Peak Detergent Premium",
      "category": "Household",
      "on_hand_units": 0,
      "on_order_units": 54,
      "forecast_units_3d": 13.4,
      "days_of_cover": 0.0,
      "stockout_prob_3d": 1.0,
      "risk_tier": "HIGH",
      "revenue_at_risk_usd": 99.27,
      "recommended_order_units": 0,
      "recommended_action": "EXPEDITE_OR_TRANSFER",
      "order_window_start_day": 6,
      "order_window_end_day": 7,
      "stockout_prob_order_window": 0.005,
      "stockout_prob_if_approved": 0.005,
      "revenue_protected_if_approved_usd": 0.0,
      "inbound_3d_units": 24,
      "lead_time_days": 6,
      "max_promo_discount_3d": 0.0,
      "promo_type": null,
      "risk_drivers": "only 0.0 days of cover; out of stock 1 of last 7 days; a new order would land after the shelf empties: expedite or transfer from a nearby store",
      "ai_reason": "With 0 units on hand against 13.0 forecast demand, Peak Detergent faces a 100% stockout risk and $99.0 at risk; expedite or transfer from a nearby store.",
      "case_pack": 6,
      "last_action": null,
      "last_order_units": null,
      "last_action_at": null
    },
    {
      "alert_id": "S1065-SKU00168",
      "sku_id": "SKU00168",
      "sku_name": "Harvest Co Cookies Organic",
      "category": "Snacks",
      "on_hand_units": 18,
      "on_order_units": 144,
      "forecast_units_3d": 61.6,
      "days_of_cover": 0.9,
      "stockout_prob_3d": 0.995,
      "risk_tier": "HIGH",
      "revenue_at_risk_usd": 73.56,
      "recommended_order_units": 0,
      "recommended_action": "EXPEDITE_OR_TRANSFER",
      "order_window_start_day": 3,
      "order_window_end_day": 5,
      "stockout_prob_order_window": 0.005,
      "stockout_prob_if_approved": 0.005,
      "revenue_protected_if_approved_usd": 0.0,
      "inbound_3d_units": 144,
      "lead
```

## 2 · Manager action written to Lakebase (OLTP write)

### Approve suggested order for Northstar Coffee Classic

`POST /api/actions` — round trip from laptop **464 ms**

Request body:
```json
{
  "alert_id": "S1065-SKU00139",
  "action": "approve",
  "note": "evidence run: approve suggested order"
}
```

Response:
```json
{
  "action_id": "09ea2251-87d8-4c36-af06-65584cad29ba",
  "alert_id": "S1065-SKU00139",
  "store_id": "S1065",
  "sku_id": "SKU00139",
  "action": "approve",
  "order_units": 48,
  "recommended_units": 48,
  "stockout_prob_if_approved": 0.005,
  "revenue_at_risk_usd": 351.48,
  "note": "evidence run: approve suggested order",
  "actor": "sabir.ayappalli@databricks.com",
  "created_at": "2026-11-06T21:54:01.223811+00:00"
}
```

### Read back the store's latest actions

`GET /api/actions?store_id=S1065` — round trip from laptop **459 ms**

Response:
```json
[
  {
    "action_id": "09ea2251-87d8-4c36-af06-65584cad29ba",
    "alert_id": "S1065-SKU00139",
    "store_id": "S1065",
    "sku_id": "SKU00139",
    "action": "approve",
    "order_units": 48,
    "recommended_units": 48,
    "stockout_prob_if_approved": 0.005,
    "revenue_at_risk_usd": 351.48,
    "note": "evidence run: approve suggested order",
    "actor": "sabir.ayappalli@databricks.com",
    "created_at": "2026-11-06T21:54:01.223811+00:00"
  }
]
```

## 3 · Governed KPIs (UC metric view via SQL warehouse)

### Chain KPIs + backtest value

`GET /api/kpis` — round trip from laptop **8994 ms**

Response:
```json
{
  "value": {
    "window_days": 42.0,
    "lost_sales_recovered_usd_annual": 5533952.0,
    "net_sales_uplift_usd_annual": 5533952.0,
    "gross_margin_uplift_usd_annual_after_shrink": 1344897.0,
    "shrink_change_usd_annual": 201755.0,
    "inventory_change_usd": 236007.0,
    "oos_rate_legacy_pct": 4.41,
    "oos_rate_ml_pct": 0.84,
    "promo_oos_legacy_pct": 12.88,
    "promo_oos_ml_pct": 1.57,
    "osa_legacy_pct": 95.59,
    "osa_ml_pct": 99.16,
    "lost_sales_reduction_pct": 82.5
  },
  "weekly_last_4": [
    {
      "week": "2026-10-05T00:00:00+00:00",
      "net_sales": 6322951.1899999315,
      "lost_sales": 63843.967199999875,
      "osa": 0.9562053571428571,
      "promo_oos": 0.1240983606557377
    },
    {
      "week": "2026-10-12T00:00:00+00:00",
      "net_sales": 6312500.09999992,
      "lost_sales": 63859.19600000002,
      "osa": 0.9575,
      "promo_oos": 0.08813953488372094
    },
    {
      "week": "2026-10-19T00:00:00+00:00",
      "net_sales": 6274922.719999908,
      "lost_sales": 66859.24979999996,
      "osa": 0.95525,
      "promo_oos": 0.12085987261146497
    },
    {
      "week": "2026-10-26T00:00:00+00:00",
      "net_sales": 6221033.999999913,
      "lost_sales": 71878.83249999981,
      "osa": 0.9536071428571429,
      "promo_oos": 0.15281124497991969
    }
  ],
  "by_category_top_3": [
    {
      "category": "Beverages",
      "oos_rate": 0.059061524199843875,
      "promo_oos": 0.19212012320328542,
      "lost_sales": 1357297.7522500085
    },
    {
      "category": "Snacks",
      "oos_rate": 0.06207965531736023,
      "promo_oos": 0.2485595703125,
      "lost_sales": 927709.8703999992
    },
    {
      "category": "Frozen",
      "oos_rate": 0.06658594138102335,
      "promo_oos": 0.19607786170830913,
      "lost_sales": 711868.7413499962
    }
  ]
}
```

## 4 · Genie Agent (natural language → SQL over governed tables)

### Genie: “Which 10 stores lost the most sales to stockouts in October 2026?”

`POST /api/genie` — round trip from laptop **14126 ms**

Request body:
```json
{
  "question": "Which 10 stores lost the most sales to stockouts in October 2026?"
}
```

Response:
```json
{
  "conversation_id": "01f1da2c99731a29923a42311be45112",
  "message_id": "01f1da2c99851dda832732a0955cf2b4",
  "status": "MessageStatus.COMPLETED",
  "text": "In **October 2026**, the 10 stores with the highest estimated sales lost to stockouts were:\n- **FreshMart Raleigh #1030** (Southeast): **USD 6,020** lost sales, **95.4%** on-shelf availability\n- **FreshMart Providence #1004** (Northeast): **USD 5,847** lost sales, **95.4%** on-shelf availability\n- **FreshMart Indianapolis #1053** (Midwest): **USD 5,625** lost sales, **96.2%** on-shelf availability\n- **FreshMart Boston #1016** (Northeast): **USD 5,595** lost sales, **95.6%** on-shelf availability\n- **FreshMart Salt Lake City #1080** (West): **USD 5,479** lost sales, **95.6%** on-shelf availability\n\nAcross all 10 stores shown, estimated lost sales ranged from **USD 5,037** to **USD 6,020**, and on-shelf availability stayed in a narrow **95.4% to 96.2%** band.",
  "sql": "SELECT `Store`, `Region`, MEASURE(`Estimated Lost Sales`) AS `est_lost_sales`, MEASURE(`On Shelf Availability`) AS `osa` FROM `sabir_freshmart`.`gold`.`stockout_metrics` WHERE `Month` = DATE'2026-10-01' AND `Store` IS NOT NULL AND `Region` IS NOT NULL GROUP BY ALL ORDER BY `est_lost_sales` DESC LIMIT 10",
  "columns": [
    "Store",
    "Region",
    "est_lost_sales",
    "osa"
  ],
  "rows": [
    [
      "FreshMart Raleigh #1030",
      "Southeast",
      "6019.692000000003",
      "0.9541935483870968"
    ],
    [
      "FreshMart Providence #1004",
      "Northeast",
      "5846.821699999998",
      "0.9541935483870968"
    ],
    [
      "FreshMart Indianapolis #1053",
      "Midwest",
      "5624.802450000002",
      "0.9617741935483871"
    ],
    [
      "FreshMart Boston #1016",
      "Northeast",
      "5594.625950000001",
      "0.9556451612903226"
    ],
    [
      "FreshMart Salt Lake City #1080",
      "West",
      "5479.036499999997",
      "0.9556451612903226"
    ],
    [
      "FreshMart Raleigh #1035",
      "Southeast",
      "5474.209000000001",
      "0.9575806451612903"
    ],
    [
      "FreshMart Atlanta #1021",
      "Southeast",
      "5429.43525",
      "0.9570967741935484"
    ],
    [
      "FreshMart Hartford #1007",
      "Northeast",
      "5087.434800000001",
      "0.9583870967741935"
    ],
    [
      "FreshMart Portland #1079",
      "West",
      "5065.135700000001",
      "0.9598387096774194"
    ],
    [
      "FreshMart Tampa #1028",
      "Southeast",
      "5036.5343",
      "0.9585483870967741"
    ]
  ]
}
```

### Genie: “What is the out-of-stock rate on promo vs non-promo days by category?”

`POST /api/genie` — round trip from laptop **18307 ms**

Request body:
```json
{
  "question": "What is the out-of-stock rate on promo vs non-promo days by category?"
}
```

Response:
```json
{
  "conversation_id": "01f1da2c99731a29923a42311be45112",
  "message_id": "01f1da2ca1db1cf4bae9e22b9d7eaefa",
  "status": "MessageStatus.COMPLETED",
  "text": "Promo days have a **higher out-of-stock rate in every category** shown. Examples include:\n- **Bakery:** **3.4%** on promo vs **1.3%** on non-promo\n- **Beverages:** **19.2%** on promo vs **5.2%** on non-promo\n- **Dairy:** **7.9%** on promo vs **1.9%** on non-promo\n- **Frozen:** **19.6%** on promo vs **5.9%** on non-promo\n- **Household:** **20.6%** on promo vs **10.1%** on non-promo\n\nAcross all 18 category/promo combinations shown, **Snacks on promo** is the highest at **24.9%**, while **Bakery on non-promo** is the lowest at **1.3%**.",
  "sql": "SELECT `Category`, `On Promo`, MEASURE(`Out Of Stock Rate`) AS `oos_rate` FROM `sabir_freshmart`.`gold`.`stockout_metrics` WHERE `Category` IS NOT NULL AND `On Promo` IS NOT NULL GROUP BY ALL ORDER BY `Category`, `On Promo`",
  "columns": [
    "Category",
    "On Promo",
    "oos_rate"
  ],
  "rows": [
    [
      "Bakery",
      "false",
      "0.012696597656876906"
    ],
    [
      "Bakery",
      "true",
      "0.03371848739495798"
    ],
    [
      "Beverages",
      "false",
      "0.052422907488986786"
    ],
    [
      "Beverages",
      "true",
      "0.19212012320328542"
    ],
    [
      "Dairy",
      "false",
      "0.018764041827309792"
    ],
    [
      "Dairy",
      "true",
      "0.07904027846842367"
    ],
    [
      "Frozen",
      "false",
      "0.05927608488864106"
    ],
    [
      "Frozen",
      "true",
      "0.19607786170830913"
    ],
    [
      "Household",
      "false",
      "0.10141797686146543"
    ],
    [
      "Household",
      "true",
      "0.20589060308555399"
    ]
  ]
}
```

### Genie: “How much revenue is at risk over the next 3 days by region, and how many items need expediting?”

`POST /api/genie` — round trip from laptop **19116 ms**

Request body:
```json
{
  "question": "How much revenue is at risk over the next 3 days by region, and how many items need expediting?"
}
```

Response:
```json
{
  "conversation_id": "01f1da2c99731a29923a42311be45112",
  "message_id": "01f1da2cacc6170a979e606b58f1bd89",
  "status": "MessageStatus.COMPLETED",
  "text": "Would you prefer to see items needing only 'EXPEDITE_OR_TRANSFER' actions separately from other expediting actions?",
  "sql": "SELECT\n  `region`,\n  SUM(`revenue_at_risk_usd`) AS `revenue_at_risk_usd`,\n  COUNT(*) FILTER (WHERE `recommended_action` ILIKE '%EXPEDITE%') AS `items_need_expediting`\nFROM `sabir_freshmart`.`ml`.`stockout_risk_scores`\nWHERE `region` IS NOT NULL\nGROUP BY `region`\nORDER BY `revenue_at_risk_usd` DESC",
  "columns": [
    "region",
    "revenue_at_risk_usd",
    "items_need_expediting"
  ],
  "rows": [
    [
      "West",
      "12792.63",
      "292"
    ],
    [
      "Southeast",
      "10368.840000000002",
      "224"
    ],
    [
      "Midwest",
      "9163.140000000003",
      "249"
    ],
    [
      "Northeast",
      "7986.439999999991",
      "227"
    ]
  ]
}
```

### Genie: “How many replenishment orders did store managers approve, and how many units?”

`POST /api/genie` — round trip from laptop **26489 ms**

Request body:
```json
{
  "question": "How many replenishment orders did store managers approve, and how many units?"
}
```

Response:
```json
{
  "conversation_id": "01f1da2c99731a29923a42311be45112",
  "message_id": "01f1da2cb8261b3785b21e838b57c253",
  "status": "MessageStatus.COMPLETED",
  "text": "Store managers recorded **1** replenishment order approval with `action = approve`, totaling **48 units**.",
  "sql": "SELECT\n  `action`,\n  COUNT(*) AS `actions`,\n  SUM(`order_units`) AS `units`\nFROM `sabir_freshmart_lakebase`.`app`.`replenishment_actions`\nWHERE `action` IS NOT NULL\n  AND `action` ILIKE '%approve%'\nGROUP BY `action`\nORDER BY `actions` DESC",
  "columns": [
    "action",
    "actions",
    "units"
  ],
  "rows": [
    [
      "approve",
      "1",
      "48"
    ]
  ]
}
```

