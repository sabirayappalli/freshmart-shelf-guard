#!/usr/bin/env bash
# Prove UC policies are enforced: query as HQ, then temporarily drop our own HQ mapping (as if we were a user with no
# region access), re-query, and restore. Also captures policy definitions, tags, lineage and the metric view.
set -uo pipefail
PROFILE=${1:-e2-demo-field-eng}; OUT=${2:-evidence/governance_checks.md}; C=sabir_freshmart
ME=$(databricks current-user me --profile $PROFILE -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
q() { echo -e "\n\`\`\`sql\n$1\n\`\`\`\n\n\`\`\`json"; databricks experimental aitools tools query "$1" --profile $PROFILE 2>&1 | sed -n '/^\[/,$p'; echo -e "\`\`\`"; }
restore() { databricks experimental aitools tools query "INSERT INTO $C.gov.user_region_access SELECT '$ME', 'ALL', 'hq_supply_chain_admin' WHERE NOT EXISTS (SELECT 1 FROM $C.gov.user_region_access WHERE principal = '$ME')" --profile $PROFILE >/dev/null; }
trap restore EXIT
{
echo "# Unity Catalog governance — enforced, not just declared"
echo -e "\nCaptured $(date -u +%Y-%m-%dT%H:%MZ) as \`$ME\`."
echo -e "\n## 1 · Policies in force"
q "DESCRIBE FUNCTION EXTENDED $C.gov.region_filter"
q "DESCRIBE FUNCTION EXTENDED $C.gov.mask_unit_cost"
q "SELECT * FROM $C.gov.user_region_access ORDER BY role, principal"
echo -e "\n## 2 · As HQ (mapped to region ALL): all regions visible, supplier cost visible"
q "SELECT region, count(*) store_skus FROM $C.ml.stockout_risk_scores GROUP BY region ORDER BY region"
q "SELECT sku_id, sku_name, unit_price, unit_cost FROM $C.silver.products ORDER BY sku_id LIMIT 3"
echo -e "\n## 3 · Same queries after removing our HQ mapping (we are now a user with no region and not in finance)"
databricks experimental aitools tools query "DELETE FROM $C.gov.user_region_access WHERE principal = '$ME'" --profile $PROFILE >/dev/null
q "SELECT count(*) AS visible_store_skus FROM $C.ml.stockout_risk_scores"
q "SELECT sku_id, sku_name, unit_price, unit_cost FROM $C.silver.products ORDER BY sku_id LIMIT 3"
restore
echo -e "\n_HQ mapping restored:_"
q "SELECT count(*) AS visible_store_skus FROM $C.ml.stockout_risk_scores"
echo -e "\n## 4 · Tags"
q "SELECT table_schema, table_name, tag_name, tag_value FROM $C.information_schema.table_tags ORDER BY 1, 2, 3"
q "SELECT schema_name, tag_name, tag_value FROM $C.information_schema.schema_tags ORDER BY 1"
q "SELECT table_schema, table_name, column_name, tag_name, tag_value FROM $C.information_schema.column_tags"
echo -e "\n## 5 · Lineage (system.access.table_lineage): raw files → bronze → silver → gold → ml → serving"
q "SELECT DISTINCT coalesce(source_table_full_name, source_path) AS source, target_table_full_name AS target FROM system.access.table_lineage WHERE target_table_catalog = '$C' AND event_date >= '2026-11-02' AND coalesce(source_table_full_name, source_path) IS NOT NULL AND target_table_full_name IS NOT NULL ORDER BY 2, 1"
echo -e "\n## 6 · Metric view: one governed KPI definition (used by Genie and the app)"
q "SELECT \`Region\`, MEASURE(\`Net Sales\`) net_sales, MEASURE(\`Estimated Lost Sales\`) est_lost_sales, MEASURE(\`On Shelf Availability\`) osa, MEASURE(\`Promo Out Of Stock Rate\`) promo_oos FROM $C.gold.stockout_metrics GROUP BY ALL ORDER BY 1"
echo -e "\n## 7 · Grants to the app service principal (least privilege)"
q "SHOW GRANTS \`5b2c13f0-0134-4430-8465-0317907aff13\` ON CATALOG $C"
q "SHOW GRANTS ON SCHEMA $C.serving"
} > "$OUT"
echo "wrote $OUT"
