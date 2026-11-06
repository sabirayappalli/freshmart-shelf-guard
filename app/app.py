"""FreshMart Shelf Guard - store-manager replenishment app.

Reads:  Lakebase (synced from UC)   serving.stockout_alerts, serving.store_briefings   -> sub-10ms OLTP reads
Writes: Lakebase (app-owned)        app.replenishment_actions                          -> manager approvals / overrides
KPIs:   SQL warehouse               gold.stockout_metrics (UC metric view), ml.backtest_* -> governed analytics
Ask:    Genie Agent                 natural-language questions over the same governed tables
"""
from __future__ import annotations

import os
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Literal

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

CATALOG = os.getenv("CATALOG", "sabir_freshmart")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")

cfg = Config()
w = WorkspaceClient()

# ---------------------------------------------------------------- Lakebase pool with OAuth token refresh
_token = {"value": None, "at": 0.0}
_token_lock = threading.Lock()


def _lakebase_endpoint() -> str:
    """Endpoint path for token generation: LAKEBASE_ENDPOINT if the platform injects it, else resolved from the app's
    postgres resource (valueFrom: postgres -> branch/database path) by matching the endpoint host to PGHOST."""
    if os.getenv("LAKEBASE_ENDPOINT"):
        return os.environ["LAKEBASE_ENDPOINT"]
    ref = os.getenv("LAKEBASE_RESOURCE", "")
    if "/endpoints/" in ref:                      # valueFrom: postgres resolves to the endpoint path
        os.environ["LAKEBASE_ENDPOINT"] = ref
        return ref
    branch = ref.split("/databases/")[0] if "/branches/" in ref else ref
    eps = list(w.postgres.list_endpoints(parent=branch))
    match = [e for e in eps if e.status and e.status.hosts and e.status.hosts.host == os.getenv("PGHOST")]
    ep = (match or eps)[0].name
    os.environ["LAKEBASE_ENDPOINT"] = ep
    return ep


def _lakebase_token() -> str:
    with _token_lock:
        if not _token["value"] or time.time() - _token["at"] > 30 * 60:
            _token["value"] = w.postgres.generate_database_credential(endpoint=_lakebase_endpoint()).token
            _token["at"] = time.time()
        return _token["value"]


class _TokenConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo: str = "", **kwargs):
        kwargs["password"] = _lakebase_token()
        return super().connect(conninfo, **kwargs)


pool = ConnectionPool(
    conninfo=f"host={os.getenv('PGHOST')} port={os.getenv('PGPORT', '5432')} dbname={os.getenv('PGDATABASE')} "
             f"user={os.getenv('PGUSER')} sslmode=require",
    connection_class=_TokenConnection, min_size=1, max_size=8, max_lifetime=45 * 60,
    kwargs={"row_factory": dict_row, "autocommit": True}, open=False)


@contextmanager
def pg():
    with pool.connection() as conn:
        yield conn


DDL = """
CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.replenishment_actions (
  action_id      UUID PRIMARY KEY,
  alert_id       TEXT NOT NULL,
  store_id       TEXT NOT NULL,
  sku_id         TEXT NOT NULL,
  action         TEXT NOT NULL CHECK (action IN ('approve', 'override', 'dismiss')),
  order_units    INT,
  recommended_units INT,
  stockout_prob_if_approved NUMERIC(6,4),
  revenue_at_risk_usd NUMERIC(12,2),
  note           TEXT,
  actor          TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE app.replenishment_actions DROP COLUMN IF EXISTS legacy_units;
ALTER TABLE app.replenishment_actions ADD COLUMN IF NOT EXISTS stockout_prob_if_approved NUMERIC(6,4);
CREATE INDEX IF NOT EXISTS ix_actions_store ON app.replenishment_actions (store_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_actions_alert ON app.replenishment_actions (alert_id, created_at DESC);
"""

app = FastAPI(title="FreshMart Shelf Guard", version="1.0")


@app.on_event("startup")
def _startup():
    print("lakebase env present:", sorted(k for k in os.environ if k.startswith(("PG", "LAKEBASE"))), "resource:", os.getenv("LAKEBASE_RESOURCE"))
    print("lakebase endpoint:", _lakebase_endpoint())
    pool.open(wait=True, timeout=60)
    with pg() as c:
        c.execute(DDL)


# ---------------------------------------------------------------- models
class ActionIn(BaseModel):
    alert_id: str = Field(..., pattern=r"^S\d+-SKU\d+$")
    action: Literal["approve", "override", "dismiss"]
    order_units: int | None = Field(None, ge=0, le=10000)
    note: str | None = Field(None, max_length=500)


class GenieIn(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)
    conversation_id: str | None = None


# ---------------------------------------------------------------- Lakebase-backed endpoints
@app.get("/api/health")
def health():
    t = time.perf_counter()
    with pg() as c:
        n = c.execute("SELECT count(*) AS n FROM serving.stockout_alerts").fetchone()["n"]
    return {"lakebase_rows": n, "lakebase_ms": round((time.perf_counter() - t) * 1000, 1)}


@app.get("/api/stores")
def stores():
    with pg() as c:
        rows = c.execute("""SELECT store_id, store_name, region, high_risk_items, medium_risk_items, revenue_at_risk_usd, briefing, as_of_date
                            FROM serving.store_briefings ORDER BY revenue_at_risk_usd DESC""").fetchall()
    return rows


@app.get("/api/alerts")
def alerts(store_id: str, tier: str = "HIGH,MEDIUM", limit: int = 50):
    tiers = [t for t in tier.upper().split(",") if t in ("HIGH", "MEDIUM", "LOW")]
    t = time.perf_counter()
    with pg() as c:
        rows = c.execute("""
            SELECT a.alert_id, a.sku_id, a.sku_name, a.category, a.on_hand_units, a.on_order_units, a.forecast_units_3d,
                   a.days_of_cover, a.stockout_prob_3d, a.risk_tier, a.revenue_at_risk_usd, a.recommended_order_units,
                   a.recommended_action, a.order_window_start_day, a.order_window_end_day, a.stockout_prob_order_window,
                   a.stockout_prob_if_approved, a.revenue_protected_if_approved_usd, a.inbound_3d_units, a.lead_time_days, a.max_promo_discount_3d, a.promo_type, a.risk_drivers, a.ai_reason, a.case_pack,
                   act.action AS last_action, act.order_units AS last_order_units, act.created_at AS last_action_at
            FROM serving.stockout_alerts a
            LEFT JOIN LATERAL (SELECT action, order_units, created_at FROM app.replenishment_actions r
                               WHERE r.alert_id = a.alert_id ORDER BY created_at DESC LIMIT 1) act ON true
            WHERE a.store_id = %s AND a.risk_tier = ANY(%s)
            ORDER BY a.revenue_at_risk_usd DESC LIMIT %s""", (store_id, tiers, min(limit, 200))).fetchall()
    return {"store_id": store_id, "query_ms": round((time.perf_counter() - t) * 1000, 1), "alerts": rows}


@app.post("/api/actions")
def create_action(body: ActionIn, request: Request):
    actor = request.headers.get("x-forwarded-email") or request.headers.get("x-forwarded-preferred-username") or "local-dev"
    with pg() as c:
        a = c.execute("""SELECT store_id, sku_id, recommended_order_units, stockout_prob_if_approved, revenue_at_risk_usd
                         FROM serving.stockout_alerts WHERE alert_id = %s""", (body.alert_id,)).fetchone()
        if not a:
            raise HTTPException(404, "unknown alert")
        units = body.order_units if body.action == "override" else (a["recommended_order_units"] if body.action == "approve" else 0)
        row = c.execute("""INSERT INTO app.replenishment_actions
              (action_id, alert_id, store_id, sku_id, action, order_units, recommended_units, stockout_prob_if_approved, revenue_at_risk_usd, note, actor)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
              (uuid.uuid4(), body.alert_id, a["store_id"], a["sku_id"], body.action, units, a["recommended_order_units"],
               a["stockout_prob_if_approved"], a["revenue_at_risk_usd"], body.note, actor)).fetchone()
    return row


@app.get("/api/actions")
def list_actions(store_id: str | None = None, limit: int = 50):
    with pg() as c:
        q = "SELECT * FROM app.replenishment_actions" + (" WHERE store_id = %s" if store_id else "") + " ORDER BY created_at DESC LIMIT %s"
        return c.execute(q, ((store_id, limit) if store_id else (limit,))).fetchall()


# ---------------------------------------------------------------- SQL warehouse (governed analytics)
def _query(q: str):
    with dbsql.connect(server_hostname=cfg.host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                       credentials_provider=lambda: cfg.authenticate) as conn, conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


_kpi_cache: dict = {}


@app.get("/api/kpis")
def kpis():
    if _kpi_cache.get("at", 0) > time.time() - 600:
        return _kpi_cache["data"]
    weekly = _query(f"""SELECT `Week` AS week, MEASURE(`Net Sales`) AS net_sales, MEASURE(`Estimated Lost Sales`) AS lost_sales,
                               MEASURE(`On Shelf Availability`) AS osa, MEASURE(`Promo Out Of Stock Rate`) AS promo_oos
                        FROM {CATALOG}.gold.stockout_metrics WHERE `Business Date` >= date_sub((SELECT max(business_date) FROM {CATALOG}.gold.store_sku_daily), 84)
                        GROUP BY ALL ORDER BY 1""")
    by_cat = _query(f"""SELECT `Category` AS category, MEASURE(`Out Of Stock Rate`) AS oos_rate, MEASURE(`Promo Out Of Stock Rate`) AS promo_oos,
                               MEASURE(`Estimated Lost Sales`) AS lost_sales
                        FROM {CATALOG}.gold.stockout_metrics GROUP BY ALL ORDER BY lost_sales DESC""")
    value = {r["k"]: r["v"] for r in _query(f"SELECT k, v FROM {CATALOG}.ml.value_summary")}
    backtest = _query(f"SELECT * FROM {CATALOG}.ml.backtest_summary")
    data = {"weekly": weekly, "by_category": by_cat, "value": value, "backtest": backtest}
    _kpi_cache.update(at=time.time(), data=data)
    return data


@app.get("/api/backtest_daily")
def backtest_daily():
    return _query(f"""SELECT CAST(business_date AS STRING) AS business_date, policy, round(lost_sales_usd, 2) AS lost_sales_usd
                      FROM {CATALOG}.ml.backtest_daily ORDER BY business_date, policy""")


# ---------------------------------------------------------------- Genie Agent
@app.post("/api/genie")
def ask_genie(body: GenieIn):
    if not GENIE_SPACE_ID:
        raise HTTPException(503, "Genie space not configured")
    if body.conversation_id:
        msg = w.genie.create_message_and_wait(GENIE_SPACE_ID, body.conversation_id, body.question)
    else:
        msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, body.question)
    out = {"conversation_id": msg.conversation_id, "message_id": msg.message_id, "status": str(msg.status), "text": None, "sql": None,
           "columns": [], "rows": []}
    for att in msg.attachments or []:
        if att.text and att.text.content:
            out["text"] = att.text.content
        if att.query:
            out["sql"] = att.query.query
            out["text"] = out["text"] or att.query.description
            res = w.genie.get_message_attachment_query_result(GENIE_SPACE_ID, msg.conversation_id, msg.message_id, att.attachment_id)
            sr = res.statement_response
            if sr and sr.manifest and sr.result:
                out["columns"] = [c.name for c in sr.manifest.schema.columns]
                out["rows"] = (sr.result.data_array or [])[:50]
    return out


app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))
