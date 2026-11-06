#!/usr/bin/env bash
# One-time Lakebase setup for FreshMart Shelf Guard (idempotent where the CLI allows).
#   1. Lakebase Autoscaling project  freshmart-stockout   (production branch, primary endpoint)
#   2. Register its Postgres DB as UC catalog  sabir_freshmart_lakebase
#   3. Synced tables (UC Delta -> Postgres, TRIGGERED, CDF):  serving.stockout_alerts, serving.store_briefings
#   4. Grant the app service principal read on the synced schema
set -euo pipefail
PROFILE=${PROFILE:-e2-demo-field-eng}
PROJECT=freshmart-stockout
BRANCH=projects/$PROJECT/branches/production
LB_CATALOG=sabir_freshmart_lakebase
CATALOG=sabir_freshmart
APP_SP=${APP_SP:-5b2c13f0-0134-4430-8465-0317907aff13}

databricks postgres get-project projects/$PROJECT --profile $PROFILE >/dev/null 2>&1 || \
  databricks postgres create-project $PROJECT --json '{"spec": {"display_name": "FreshMart Stockout Prevention"}}' --profile $PROFILE

databricks postgres get-catalog catalogs/$LB_CATALOG --profile $PROFILE >/dev/null 2>&1 || \
  databricks postgres create-catalog $LB_CATALOG --json "{\"spec\": {\"postgres_database\": \"databricks_postgres\", \"branch\": \"$BRANCH\"}}" --profile $PROFILE

for T in stockout_alerts:alert_id store_briefings:store_id; do
  NAME=${T%%:*}; PK=${T##*:}
  if ! databricks postgres get-synced-table "synced_tables/$LB_CATALOG.serving.$NAME" --profile $PROFILE >/dev/null 2>&1; then
    databricks postgres create-synced-table "$LB_CATALOG.serving.$NAME" --json "{
      \"spec\": {\"source_table_full_name\": \"$CATALOG.serving.$NAME\", \"primary_key_columns\": [\"$PK\"],
                 \"scheduling_policy\": \"TRIGGERED\", \"branch\": \"$BRANCH\", \"postgres_database\": \"databricks_postgres\",
                 \"create_database_objects_if_missing\": true,
                 \"new_pipeline_spec\": {\"storage_catalog\": \"$CATALOG\", \"storage_schema\": \"serving\"}}}" --profile $PROFILE
  fi
done

EP=$BRANCH/endpoints/primary
HOST=$(databricks postgres get-endpoint $EP --profile $PROFILE -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['status']['hosts']['host'])")
TOKEN=$(databricks postgres generate-database-credential $EP --profile $PROFILE -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
USER=$(databricks current-user me --profile $PROFILE -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")
PGPASSWORD="$TOKEN" psql "host=$HOST user=$USER dbname=databricks_postgres sslmode=require" -v ON_ERROR_STOP=1 <<SQL
GRANT USAGE ON SCHEMA serving TO "$APP_SP";
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO "$APP_SP";
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT SELECT ON TABLES TO "$APP_SP";
SQL
echo "Lakebase ready: $HOST"
