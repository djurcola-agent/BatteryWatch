#!/usr/bin/env bash
set -euo pipefail

: "${BATTERYWATCH_DATABASE_URL:?BATTERYWATCH_DATABASE_URL is required}"
: "${BATTERYWATCH_BACKUP_MANIFEST:?BATTERYWATCH_BACKUP_MANIFEST is required}"
app_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)

if [[ ! -f "$BATTERYWATCH_BACKUP_MANIFEST" ]]; then
  printf 'backup manifest does not exist\n' >&2
  exit 1
fi
now=$(date +%s)
manifest_mtime=$(stat -c %Y -- "$BATTERYWATCH_BACKUP_MANIFEST")
manifest_age=$((now - manifest_mtime))
if (( manifest_age < 0 || manifest_age > 3600 )); then
  printf 'backup manifest is not fresh (maximum age: 3600 seconds)\n' >&2
  exit 1
fi
sha256sum --check --status "$BATTERYWATCH_BACKUP_MANIFEST"

database_url=$BATTERYWATCH_DATABASE_URL
unset BATTERYWATCH_DATABASE_URL

psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/001_initial_schema.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/002_dispatch_scada_raw_ingestion.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/003_dispatch_price_artifacts.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/004_historical_backfill_ledger.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/005_historical_source_artifacts.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/006_artifact_recorded_event.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/007_backfill_runtime_details.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/008_authoritative_soc.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/009_allow_archive_urls.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/010_nextday_fcas.sql"
psql -d "$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/011_fcas_validator_compatibility.sql"
psql -d "$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 \
  --file="$app_dir/migrations/012_current_nextday_artifacts.sql"
psql --dbname="$database_url" --no-psqlrc --set=ON_ERROR_STOP=1 --tuples-only --no-align \
  <<'SQL'
SELECT count(*) = 15
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
    'generators', 'generator_power_5m', 'generator_soc_5m', 'nem_price_5m',
    'dispatch_scada_artifacts', 'raw_dispatch_scada_observations',
    'dispatch_price_artifacts',
    'historical_backfill_runs',
    'historical_backfill_items', 'historical_backfill_events',
    'historical_source_artifacts', 'historical_backfill_item_artifacts',
    'raw_nextday_soc_observations', 'raw_nextday_fcas_observations',
    'generator_fcas_5m'
  );
SQL
unset database_url
