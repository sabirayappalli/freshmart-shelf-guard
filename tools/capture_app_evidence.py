"""Exercise the deployed Shelf Guard app + Lakebase + Genie end to end and record every request/response as Markdown.

Usage: python tools/capture_app_evidence.py <PROFILE> <APP_URL> <OUT_FILE>
Steps: health (Lakebase latency) -> stores -> alerts for the top store -> approve one alert (Lakebase write) ->
       read it back -> KPIs (metric view) -> Genie questions (incl. one that reads the Lakebase actions table via UC).
"""
import json
import subprocess
import sys
import time
import urllib.request


def token(profile):
    return json.loads(subprocess.check_output(["databricks", "auth", "token", "--profile", profile], text=True))["access_token"]


def call(url, tok, method="GET", body=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=300) as r:
        out = json.loads(r.read())
    return out, round((time.perf_counter() - t) * 1000)


def block(title, method, path, body, resp, ms, trim=None):
    shown = resp
    if trim:
        shown = trim(resp)
    md = [f"### {title}\n", f"`{method} {path}` — round trip from laptop **{ms} ms**\n"]
    if body:
        md.append("Request body:\n```json\n" + json.dumps(body, indent=2) + "\n```\n")
    md.append("Response:\n```json\n" + json.dumps(shown, indent=2, default=str)[:6000] + "\n```\n")
    return "\n".join(md)


def main(profile, app_url, out_file):
    tok = token(profile)
    md = ["# Lakebase · App · Genie — recorded end-to-end run\n",
          f"App: {app_url}  ·  captured {time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n",
          "All responses below are verbatim JSON from the deployed Databricks App (trimmed only where marked).\n"]

    h, ms = call(f"{app_url}/api/health", tok)
    md.append("## 1 · Lakebase serving (synced from Unity Catalog)\n")
    md.append(block("Health: row count + server-side Lakebase query latency", "GET", "/api/health", None, h, ms))

    stores, ms = call(f"{app_url}/api/stores", tok)
    md.append(block("Stores ranked by revenue at risk, with the AI morning briefing (top 2 shown)", "GET", "/api/stores", None, stores, ms,
                    trim=lambda r: r[:2]))
    store = stores[0]["store_id"]

    al, ms = call(f"{app_url}/api/alerts?store_id={store}&tier=HIGH,MEDIUM&limit=8", tok)
    md.append(block(f"At-risk items for {stores[0]['store_name']} (top 5 shown)", "GET", f"/api/alerts?store_id={store}", None, al, ms,
                    trim=lambda r: {**r, "alerts": r["alerts"][:5]}))

    md.append("## 2 · Manager action written to Lakebase (OLTP write)\n")
    target = next((a for a in al["alerts"] if a["recommended_action"] in ("APPROVE_ORDER", "EXPEDITE_AND_ORDER") and not a["last_action"]),
                  al["alerts"][0])
    body = {"alert_id": target["alert_id"], "action": "approve", "note": "evidence run: approve suggested order"}
    w, ms = call(f"{app_url}/api/actions", tok, "POST", body)
    md.append(block(f"Approve suggested order for {target['sku_name']}", "POST", "/api/actions", body, w, ms))
    acts, ms = call(f"{app_url}/api/actions?store_id={store}&limit=5", tok)
    md.append(block("Read back the store's latest actions", "GET", f"/api/actions?store_id={store}", None, acts, ms))

    md.append("## 3 · Governed KPIs (UC metric view via SQL warehouse)\n")
    k, ms = call(f"{app_url}/api/kpis", tok)
    md.append(block("Chain KPIs + backtest value", "GET", "/api/kpis", None, k, ms,
                    trim=lambda r: {"value": r["value"], "weekly_last_4": r["weekly"][-4:], "by_category_top_3": r["by_category"][:3]}))

    md.append("## 4 · Genie Agent (natural language → SQL over governed tables)\n")
    convo = None
    for q in ["Which 10 stores lost the most sales to stockouts in October 2026?",
              "What is the out-of-stock rate on promo vs non-promo days by category?",
              "How much revenue is at risk over the next 3 days by region, and how many items need expediting?",
              "How many replenishment orders did store managers approve, and how many units?"]:
        body = {"question": q, "conversation_id": convo}
        g, ms = call(f"{app_url}/api/genie", tok, "POST", body)
        convo = g.get("conversation_id")
        md.append(block(f"Genie: “{q}”", "POST", "/api/genie", {"question": q}, g, ms,
                        trim=lambda r: {**r, "rows": r.get("rows", [])[:10]}))
    open(out_file, "w").write("\n".join(md) + "\n")
    print(open(out_file).read()[:3000])


if __name__ == "__main__":
    main(*sys.argv[1:4])
