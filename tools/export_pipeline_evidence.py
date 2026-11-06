"""Export Lakeflow pipeline update evidence (rows written per flow, expectation pass/fail counts) to Markdown.

Usage: python tools/export_pipeline_evidence.py <PIPELINE_ID> <PROFILE> <OUT_FILE> [N_UPDATES]
Queries the pipeline event log with the `event_log()` table-valued function on a SQL warehouse.
"""
import json
import subprocess
import sys


def query(sql, profile):
    out = subprocess.check_output(["databricks", "experimental", "aitools", "tools", "query", sql, "--profile", profile], text=True)
    return json.loads(out[out.index("["):]) if "[" in out else []


def table(rows, cols):
    md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    md += ["| " + " | ".join(str(r.get(c, "")) for c in cols) + " |" for r in rows]
    return "\n".join(md) + "\n"


def main(pipeline_id, profile, out_file, n_updates="2"):
    ups = query(f"""SELECT origin.update_id AS update_id, min(timestamp) AS started, max(timestamp) AS ended,
                           max_by(details:update_progress.state::string, timestamp) AS final_state
                    FROM event_log('{pipeline_id}') WHERE event_type = 'update_progress'
                    GROUP BY 1 ORDER BY started DESC LIMIT {int(n_updates)}""", profile)
    md = [f"# Lakeflow pipeline evidence — `{pipeline_id}`\n",
          "Source: pipeline event log via `event_log()` (SQL). Most recent updates first.\n"]
    for u in ups:
        uid = u["update_id"]
        flows = query(f"""SELECT origin.flow_name AS flow,
                                 max_by(details:flow_progress.status::string, timestamp) AS status,
                                 sum(coalesce(details:flow_progress.metrics.num_output_rows::bigint, 0)) AS rows_written,
                                 sum(coalesce(details:flow_progress.data_quality.dropped_records::bigint, 0)) AS rows_dropped
                          FROM event_log('{pipeline_id}')
                          WHERE event_type = 'flow_progress' AND origin.update_id = '{uid}'
                          GROUP BY 1 ORDER BY 1""", profile)
        flows = [f for f in flows if f["flow"] and "missingFlowName" not in f["flow"]]
        exps = query(f"""SELECT origin.flow_name AS flow, e.name AS expectation, e.dataset AS dataset,
                                sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
                         FROM event_log('{pipeline_id}'),
                              LATERAL explode(from_json(details:flow_progress.data_quality.expectations,
                                  'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS t(e)
                         WHERE event_type = 'flow_progress' AND origin.update_id = '{uid}'
                         GROUP BY ALL ORDER BY 1, 2""", profile)
        md.append(f"## Update `{uid}` — **{u['final_state']}** ({u['started']} → {u['ended']})\n")
        md.append("### Rows per flow\n")
        md.append(table(flows, ["flow", "status", "rows_written", "rows_dropped"]))
        if exps:
            md.append("### Expectations\n")
            md.append(table(exps, ["flow", "expectation", "passed", "failed"]))
    open(out_file, "w").write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main(*sys.argv[1:])
