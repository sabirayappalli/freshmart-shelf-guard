"""Export executed job-task notebooks (code + real outputs) to readable Markdown for the repo's evidence/ folder.

Usage: python tools/export_run_evidence.py <JOB_RUN_ID> <PROFILE> <OUT_DIR>

Uses `databricks jobs export-run` (HTML containing the executed notebook model), decodes the embedded notebook JSON
and writes one .md per task with every command's source and its text/table output, plus the task's exit value.
"""
import base64
import json
import re
import subprocess
import sys
import urllib.parse
from pathlib import Path


def cli(*args):
    return json.loads(subprocess.check_output(["databricks", *args, "-o", "json"], text=True))


def decode_model(html: str):
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    if not m:
        return None
    raw = base64.b64decode(m.group(1)).decode("utf-8")
    return json.loads(urllib.parse.unquote(raw))


ANSI = re.compile(r"\x1b\[[0-9;]*m")
NOISE = ("Warning", "warnings.warn", "you may need to restart", "INFO mlflow", "site-packages", "FutureWarning", "  return ", "  warnings")


def clean_text(txt: str) -> str:
    lines = [ln for ln in ANSI.sub("", txt).splitlines() if not any(n in ln for n in NOISE)]
    return "\n".join(lines).strip()


def render_table(data, schema) -> str:
    cols = [c.get("name") if isinstance(c, dict) else str(c) for c in (schema or [])]
    rows = data[:30]
    if not cols and rows:
        cols = [f"c{i}" for i in range(len(rows[0]))]
    out = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
    for r in rows:
        out += "| " + " | ".join(str(v).replace("|", "\\|").replace("\n", " ")[:120] for v in r) + " |\n"
    if len(data) > 30:
        out += f"\n_({len(data)} rows, first 30 shown)_\n"
    return out + "\n"


def render_result(res) -> str:
    if not res:
        return ""
    t, data = res.get("type"), res.get("data")
    if t == "listResults" and isinstance(data, list):
        return "".join(render_result(d) for d in data)
    if t == "table" and isinstance(data, list):
        return render_table(data, res.get("schema"))
    if t in ("text", "ansi", "raw"):
        txt = clean_text(re.sub(r"<[^>]+>", "", str(data or "")))
        return f"```text\n{txt[:12000]}\n```\n" if txt else ""
    if t == "html":
        txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", str(data or ""))).strip()
        return f"```text\n{txt[:4000]}\n```\n" if txt else ""
    if t == "error":
        return f"```text\nERROR: {clean_text(str(res.get('summary') or res.get('cause') or data))[:3000]}\n```\n"
    return ""


def main(run_id: str, profile: str, out_dir: str):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    run = cli("jobs", "get-run", run_id, "--profile", profile)
    index = [f"# Job run {run_id} — `{run.get('run_name')}`\n", f"Run page: {run.get('run_page_url')}\n",
             "| task | state | result | duration (s) | evidence |", "|---|---|---|---|---|"]
    for t in sorted(run.get("tasks", []), key=lambda t: t.get("start_time", 0)):
        st = t.get("state", {})
        if st.get("life_cycle_state") == "SKIPPED":
            continue
        dur = round((t.get("end_time", 0) - t.get("start_time", 0)) / 1000) if t.get("end_time") else None
        link = ""
        if "notebook_task" in t:
            exp = cli("jobs", "export-run", str(t["run_id"]), "--views-to-export", "CODE", "--profile", profile)
            model = decode_model(exp["views"][0]["content"])
            exit_val = cli("jobs", "get-run-output", str(t["run_id"]), "--profile", profile).get("notebook_output", {}).get("result")
            md = [f"# {t['task_key']}\n", f"- Task run id: `{t['run_id']}`  \n- Result: **{st.get('result_state')}**  \n- Duration: {dur}s\n"]
            for cmd in (model or {}).get("commands", []):
                src = cmd.get("command", "")
                if src.strip().startswith("%md"):
                    md.append(src.strip()[3:].strip() + "\n")
                    continue
                lang = "sql" if src.strip().startswith("%sql") else "python"
                md.append(f"```{lang}\n{src.strip()}\n```\n")
                rendered = render_result(cmd.get("results"))
                if rendered:
                    md.append("**Output:**\n\n" + rendered)
            if exit_val:
                try:
                    pretty = json.dumps(json.loads(exit_val), indent=2)
                except ValueError:
                    pretty = exit_val.replace(",", "\n")
                md.append("## Notebook exit value\n\n```json\n" + pretty + "\n```\n")
            fname = f"{t['task_key']}.md"
            (out / fname).write_text("\n".join(md))
            link = f"[{fname}]({fname})"
        elif "pipeline_task" in t:
            link = "see lakeflow_pipeline_updates.md"
        index.append(f"| {t['task_key']} | {st.get('life_cycle_state')} | {st.get('result_state')} | {dur} | {link} |")
    (out / "README.md").write_text("\n".join(index) + "\n")
    print("\n".join(index))


if __name__ == "__main__":
    main(*sys.argv[1:4])
