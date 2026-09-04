"""Contract tests for the immutable historical source-artifact schema."""

from pathlib import Path
import unittest


class HistoricalArtifactMigrationTests(unittest.TestCase):
    def test_migration_defines_immutable_artifacts_and_deploys_last(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration = (app_root / "migrations" / "005_historical_source_artifacts.sql").read_text(
            encoding="utf-8"
        )
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS historical_source_artifacts", migration)
        self.assertIn(
            "artifact_sha256 TEXT PRIMARY KEY CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$')",
            migration,
        )
        self.assertIn("feed", migration)
        self.assertIn("dispatch_price", migration)
        self.assertIn("dispatch_scada", migration)
        self.assertIn("report_date", migration)
        self.assertIn("source_url", migration)
        self.assertIn("filename", migration)
        self.assertIn("byte_count", migration)
        self.assertIn("raw_bytes BYTEA NOT NULL", migration)
        self.assertIn("stored_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP", migration)
        self.assertIn("UNIQUE (artifact_sha256, feed, report_date)", migration)
        self.assertIn("byte_count = octet_length(raw_bytes)", migration)
        self.assertIn("DispatchIS_Reports/", migration)
        self.assertIn("Dispatch_SCADA/", migration)
        self.assertIn("to_char(report_date, 'YYYYMMDD')", migration)
        self.assertIn("CREATE TABLE IF NOT EXISTS historical_backfill_item_artifacts", migration)
        self.assertIn("PRIMARY KEY (run_id, feed, report_date, attempt_number)", migration)
        self.assertIn("attempt_number > 0", migration)
        self.assertIn("REFERENCES historical_backfill_items", migration)
        self.assertIn("REFERENCES historical_source_artifacts", migration)
        self.assertIn("ON DELETE RESTRICT", migration)
        self.assertIn("downloaded_at TIMESTAMPTZ NOT NULL", migration)
        self.assertIn("source_last_modified TIMESTAMPTZ", migration)
        self.assertIn("recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP", migration)
        self.assertIn("source_last_modified IS NULL OR source_last_modified <= downloaded_at", migration)
        self.assertIn("historical_source_artifacts_feed_report_date_idx", migration)
        self.assertIn("historical_backfill_item_artifacts_artifact_sha256_idx", migration)

        self.assertIn("004_historical_backfill_ledger.sql", migrate_script)
        self.assertIn("005_historical_source_artifacts.sql", migrate_script)
        self.assertIn("SELECT count(*) = 15", migrate_script)
        for table in ("historical_source_artifacts", "historical_backfill_item_artifacts"):
            self.assertIn(f"'{table}'", migrate_script)
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)

        upper = migration.upper()
        for forbidden in ("DROP ", "ALTER ", "TRUNCATE "):
            self.assertNotIn(forbidden, upper)
        self.assertNotRegex(upper, r"\bDELETE\s+FROM\b")

    def test_expands_event_constraint_for_artifact_recorded_atomically(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "006_artifact_recorded_event.sql"
        self.assertTrue(migration_path.exists(), "006 event migration is missing")

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(
            encoding="utf-8"
        )
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        self.assertIn("ALTER TABLE historical_backfill_events", migration)
        self.assertIn(
            "DROP CONSTRAINT IF EXISTS historical_backfill_events_event_type_check",
            migration,
        )
        self.assertIn(
            "ADD CONSTRAINT historical_backfill_events_event_type_check",
            migration,
        )
        for event_type in (
            "planned",
            "recovered",
            "claimed",
            "artifact_recorded",
            "completed",
            "failed",
        ):
            self.assertIn(f"'{event_type}'", migration)
        self.assertIn("006_artifact_recorded_event.sql", migrate_script)
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)
        self.assertNotIn("TRUNCATE ", upper)
        self.assertNotRegex(upper, r"\bDELETE\s+FROM\b")

    def test_adds_runtime_ledger_error_and_event_detail_columns(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "007_backfill_runtime_details.sql"
        self.assertTrue(migration_path.exists(), "007 runtime-details migration is missing")

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(
            encoding="utf-8"
        )
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        self.assertIn("ALTER TABLE historical_backfill_items", migration)
        self.assertIn("ADD COLUMN IF NOT EXISTS last_error TEXT", migration)
        self.assertIn("historical_backfill_items_last_error_check", migration)
        self.assertIn("char_length(last_error) BETWEEN 1 AND 2048", migration)
        self.assertIn("ALTER TABLE historical_backfill_events", migration)
        self.assertIn("ADD COLUMN IF NOT EXISTS details JSONB", migration)
        self.assertIn("DEFAULT '{}'::jsonb", migration)
        self.assertIn("historical_backfill_events_details_check", migration)
        self.assertIn("jsonb_typeof(details) = 'object'", migration)
        self.assertIn("007_backfill_runtime_details.sql", migrate_script)
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)
        self.assertNotIn("TRUNCATE ", upper)
        self.assertNotRegex(upper, r"\bDELETE\s+FROM\b")

    def test_adds_authoritative_individual_soc_schema_after_runtime_details(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "008_authoritative_soc.sql"
        self.assertTrue(migration_path.exists(), "008 authoritative-SOC migration is missing")

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(encoding="utf-8")
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        self.assertIn("CREATE TABLE IF NOT EXISTS raw_nextday_soc_observations", migration)
        for column in (
            "artifact_sha256",
            "generator_id",
            "interval_start",
            "soc_mwh",
            "intervention",
            "run_number",
            "dispatch_interval",
            "last_changed",
            "report_timestamp",
            "downloaded_at",
            "ingestion_version",
            "correction_version",
        ):
            self.assertIn(column, migration)
        self.assertIn("REFERENCES historical_source_artifacts", migration)
        self.assertIn("REFERENCES generators", migration)
        self.assertIn("ON DELETE RESTRICT", migration)
        self.assertIn("raw_nextday_soc_observations_artifact_time_idx", migration)
        self.assertIn("raw_nextday_soc_observations_generator_time_idx", migration)

        self.assertIn("ALTER TABLE generator_soc_5m", migration)
        for column in (
            "soc_mwh",
            "capacity_mwh",
            "capacity_effective_from",
            "capacity_effective_to",
            "capacity_source_id",
            "capacity_source_timestamp",
            "report_timestamp",
            "downloaded_at",
            "intervention",
            "run_number",
            "dispatch_interval",
            "source_artifact_sha256",
        ):
            self.assertIn(f"ADD COLUMN IF NOT EXISTS {column}", migration)
        self.assertIn("generator_soc_5m_authoritative_soc_check", migration)
        self.assertIn("generator_soc_5m_capacity_provenance_check", migration)
        self.assertIn("generator_soc_5m_authoritative_metadata_check", migration)
        self.assertGreaterEqual(
            migration.count("num_nonnulls("),
            4,
            "all-or-none SOC provenance checks must not pass through SQL NULL",
        )
        self.assertIn("'nextday_soc'", migration)
        self.assertIn("PUBLIC_NEXT_DAY_DISPATCH_", migration)
        self.assertIn("Next_Day_Dispatch/", migration)
        for column in (
            "parent_artifact_sha256",
            "artifact_published_at",
            "artifact_downloaded_at",
            "publication_id",
        ):
            self.assertIn(f"ADD COLUMN IF NOT EXISTS {column}", migration)
        self.assertIn("historical_source_artifacts_parent_fk", migration)
        self.assertIn("REFERENCES historical_source_artifacts (artifact_sha256)", migration)
        self.assertIn("historical_source_artifacts_nested_metadata_ck", migration)
        self.assertRegex(
            migration,
            r"num_nonnulls\(\s*parent_artifact_sha256,\s*"
            r"artifact_published_at,\s*artifact_downloaded_at,\s*"
            r"publication_id\s*\)",
        )
        self.assertIn("artifact_published_at <= artifact_downloaded_at", migration)
        self.assertIn("source_url =", migration)
        self.assertIn("|| '#' || filename", migration)
        self.assertIn("historical_source_artifacts_parent_idx", migration)
        for constraint in (
            "historical_backfill_items_feed_check",
            "historical_backfill_events_feed_check",
            "historical_source_artifacts_feed_check",
            "historical_source_artifacts_archive_identity_ck",
            "historical_backfill_item_artifacts_feed_check",
        ):
            self.assertIn(f"DROP CONSTRAINT IF EXISTS {constraint}", migration)
            self.assertIn(f"ADD CONSTRAINT {constraint}", migration)

        self.assertIn("008_authoritative_soc.sql", migrate_script)
        self.assertLess(
            migrate_script.index("007_backfill_runtime_details.sql"),
            migrate_script.index("008_authoritative_soc.sql"),
        )
        self.assertIn("SELECT count(*) = 15", migrate_script)
        self.assertIn("'raw_nextday_soc_observations'", migrate_script)
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)
        for forbidden in ("DROP TABLE", "DROP COLUMN", "TRUNCATE ", "DELETE FROM"):
            self.assertNotIn(forbidden, upper)

    def test_adds_grouped_nextday_fcas_raw_and_effective_schema(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "010_nextday_fcas.sql"
        self.assertTrue(migration_path.exists(), "010 Next Day FCAS migration is missing")

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(encoding="utf-8")
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        self.assertIn("CREATE TABLE IF NOT EXISTS raw_nextday_fcas_observations", migration)
        self.assertIn("CREATE TABLE IF NOT EXISTS generator_fcas_5m", migration)
        self.assertIn("fcas_services JSONB NOT NULL", migration)
        self.assertIn("nextday_fcas_map_jsonb_valid", migration)
        self.assertIn("value ?& ARRAY", migration)
        self.assertIn("(value -> 'target_mw') < '0'::jsonb", migration)
        self.assertIn("(value -> 'actual_availability_mw') < '0'::jsonb", migration)
        self.assertIn("target_type NOT IN ('null', 'number')", migration)
        self.assertIn("status_type NOT IN ('null', 'number')", migration)
        self.assertIn("actual_type NOT IN ('null', 'number')", migration)
        self.assertIn(
            "NOT IN ('0', '1', '2', '3', '4')",
            migration,
        )
        self.assertNotIn("::DOUBLE PRECISION", upper)
        self.assertIn("CREATE OR REPLACE FUNCTION", upper)
        self.assertEqual(migration.count("CREATE TABLE IF NOT EXISTS"), 2)
        self.assertEqual(migration.count("CREATE INDEX IF NOT EXISTS"), 4)
        self.assertIn("PRIMARY KEY (generator_id, interval_start)", migration)
        self.assertIn(
            "PRIMARY KEY (\n        artifact_sha256,\n        generator_id,\n        interval_start,",
            migration,
        )
        for service in (
            "raise_1s",
            "lower_1s",
            "raise_6s",
            "lower_6s",
            "raise_60s",
            "lower_60s",
            "raise_5m",
            "lower_5m",
            "raise_reg",
            "lower_reg",
        ):
            self.assertIn(service, migration)
        for field in ("target_mw", "enablement_status", "actual_availability_mw"):
            self.assertIn(f"'{field}'", migration)
        for column in (
            "artifact_sha256",
            "generator_id",
            "interval_start",
            "intervention",
            "run_number",
            "dispatch_interval",
            "last_changed",
            "report_timestamp",
            "downloaded_at",
            "ingestion_version",
            "correction_version",
            "source_artifact_sha256",
        ):
            self.assertIn(column, migration)
        for constraint in (
            "CHECK (intervention IN (0, 1))",
            "CHECK (run_number > 0)",
            "CHECK (ingestion_version >= 0)",
            "CHECK (correction_version >= 0)",
            "CHECK (last_changed <= report_timestamp)",
            "CHECK (report_timestamp <= downloaded_at)",
        ):
            self.assertGreaterEqual(migration.count(constraint), 2)
        self.assertIn("UNIQUE (", migration)
        self.assertIn("REFERENCES historical_source_artifacts", migration)
        self.assertIn("REFERENCES generators", migration)
        self.assertIn("ON DELETE RESTRICT", migration)
        self.assertIn("raw_nextday_fcas_observations_artifact_time_idx", migration)
        self.assertIn("raw_nextday_fcas_observations_generator_time_idx", migration)
        self.assertIn("generator_fcas_5m_time_idx", migration)
        self.assertIn("010_nextday_fcas.sql", migrate_script)
        self.assertLess(
            migrate_script.index("009_allow_archive_urls.sql"),
            migrate_script.index("010_nextday_fcas.sql"),
        )
        self.assertIn("SELECT count(*) = 15", migrate_script)
        self.assertIn("'raw_nextday_fcas_observations'", migrate_script)
        self.assertIn("'generator_fcas_5m'", migrate_script)
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)
        for forbidden in ("DROP TABLE", "DROP COLUMN", "TRUNCATE ", "DELETE FROM"):
            self.assertNotIn(forbidden, upper)

    def test_replaces_fcas_validators_with_supported_additive_migration(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "011_fcas_validator_compatibility.sql"
        self.assertTrue(
            migration_path.exists(),
            "011 FCAS validator compatibility migration is missing",
        )

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(
            encoding="utf-8"
        )
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        for function_name in (
            "nextday_fcas_service_jsonb_valid",
            "nextday_fcas_map_jsonb_valid",
        ):
            self.assertIn(
                f"CREATE OR REPLACE FUNCTION {function_name}",
                migration,
            )
        self.assertIn("jsonb_object_keys(value)", migration)
        self.assertIn("COUNT(*)", upper)
        self.assertNotIn("jsonb_object_length", migration)
        self.assertIn("010_nextday_fcas.sql", migrate_script)
        self.assertIn("011_fcas_validator_compatibility.sql", migrate_script)
        self.assertLess(
            migrate_script.index("010_nextday_fcas.sql"),
            migrate_script.index("011_fcas_validator_compatibility.sql"),
        )
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)
        for forbidden in ("DROP TABLE", "DROP COLUMN", "TRUNCATE ", "DELETE FROM"):
            self.assertNotIn(forbidden, upper)

    def test_allows_exact_archive_interval_receipt_urls_after_soc_migration(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "009_allow_archive_urls.sql"
        self.assertTrue(migration_path.exists(), "009 archive-URL migration is missing")

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(
            encoding="utf-8"
        )
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        self.assertIn("REPORTS/CURRENT/Dispatch_SCADA/", migration)
        self.assertIn("REPORTS/ARCHIVE/Dispatch_SCADA/", migration)
        self.assertIn("REPORTS/CURRENT/DispatchIS_Reports/", migration)
        self.assertIn("REPORTS/ARCHIVE/DispatchIS_Reports/", migration)
        self.assertEqual(migration.count("|| '.zip#' || zip_filename"), 2)
        self.assertIn("substring(zip_filename FROM 30 FOR 4) = '0000'", migration)
        self.assertIn("substring(zip_filename FROM 27 FOR 4) = '0000'", migration)
        self.assertEqual(migration.count("- INTERVAL '1 day'"), 2)
        self.assertIn("009_allow_archive_urls.sql", migrate_script)
        self.assertLess(
            migrate_script.index("008_authoritative_soc.sql"),
            migrate_script.index("009_allow_archive_urls.sql"),
        )
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)
        for forbidden in ("DROP TABLE", "DROP COLUMN", "TRUNCATE ", "DELETE FROM"):
            self.assertNotIn(forbidden, upper)


    def test_allows_only_direct_current_nextday_identity_additively(self) -> None:
        app_root = Path(__file__).resolve().parents[2]
        migration_path = app_root / "migrations" / "012_current_nextday_artifacts.sql"
        self.assertTrue(migration_path.exists(), "012 CURRENT Next Day migration is missing")

        migration = migration_path.read_text(encoding="utf-8")
        migrate_script = (app_root / "deploy" / "migrate.sh").read_text(encoding="utf-8")
        upper = migration.upper()

        self.assertIn("BEGIN;", upper)
        self.assertIn("COMMIT;", upper)
        self.assertIn("012_current_nextday_artifacts.sql", migrate_script)
        migration_files = (
            "001_initial_schema.sql",
            "002_dispatch_scada_raw_ingestion.sql",
            "003_dispatch_price_artifacts.sql",
            "004_historical_backfill_ledger.sql",
            "005_historical_source_artifacts.sql",
            "006_artifact_recorded_event.sql",
            "007_backfill_runtime_details.sql",
            "008_authoritative_soc.sql",
            "009_allow_archive_urls.sql",
            "010_nextday_fcas.sql",
            "011_fcas_validator_compatibility.sql",
            "012_current_nextday_artifacts.sql",
        )
        self.assertEqual(
            [name for name in migration_files if name in migrate_script],
            list(migration_files),
        )
        self.assertEqual(
            sum(migrate_script.count(name) for name in migration_files),
            len(migration_files),
        )
        migration_positions = [
            migrate_script.index(name) for name in migration_files
        ]
        self.assertEqual(migration_positions, sorted(migration_positions))
        self.assertLess(
            migrate_script.index("011_fcas_validator_compatibility.sql"),
            migrate_script.index("012_current_nextday_artifacts.sql"),
        )
        self.assertEqual(
            migration.count("DROP CONSTRAINT IF EXISTS"),
            2,
        )
        self.assertEqual(migration.count("ADD CONSTRAINT"), 2)
        self.assertIn("REPORTS/CURRENT/Next_Day_Dispatch/", migration)
        self.assertIn("PUBLIC_NEXT_DAY_DISPATCH_", migration)
        self.assertIn("artifact_published_at", migration)
        self.assertIn("artifact_downloaded_at", migration)
        self.assertIn("publication_id", migration)
        self.assertIn("parent_artifact_sha256 IS NULL", migration)
        self.assertIn("num_nonnulls(", migration)
        self.assertIn(
            "feed = 'nextday_soc'\n                AND parent_artifact_sha256 IS NULL\n                AND num_nonnulls(",
            migration,
        )
        self.assertIn(
            "feed = 'dispatch_price'\n            AND parent_artifact_sha256 IS NULL\n            AND source_url =",
            migration,
        )
        self.assertIn(
            "feed = 'dispatch_scada'\n            AND parent_artifact_sha256 IS NULL\n            AND source_url =",
            migration,
        )
        self.assertIn(
            "parent_artifact_sha256 IS NOT NULL\n            AND source_url =",
            migration,
        )
        self.assertIn(
            "'https://www.nemweb.com.au/REPORTS/CURRENT/Next_Day_Dispatch/'\n"
            "                || filename\n"
            "            AND num_nonnulls(",
            migration,
        )
        identity = migration[
            migration.index(
                "ADD CONSTRAINT historical_source_artifacts_archive_identity_ck"
            ):]
        self.assertEqual(identity.count("feed = "), 5)
        self.assertEqual(identity.count("source_url ="), 5)
        self.assertEqual(identity.count("filename ="), 5)
        for preserved_identity in (
            "'https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/' || filename\n"
            "            AND filename = 'PUBLIC_DISPATCHIS_' || to_char(report_date, 'YYYYMMDD') || '.zip'",
            "'https://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/' || filename\n"
            "            AND filename = 'PUBLIC_DISPATCHSCADA_' || to_char(report_date, 'YYYYMMDD') || '.zip'",
            "'https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/' || filename\n"
            "            AND filename =\n"
            "                'PUBLIC_NEXT_DAY_DISPATCH_' || to_char(report_date, 'YYYYMM') || '01.zip'",
            "'https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/'\n"
            "                || 'PUBLIC_NEXT_DAY_DISPATCH_'\n"
            "                || to_char(report_date, 'YYYYMM') || '01.zip'\n"
            "                || '#' || filename",
            "'https://www.nemweb.com.au/REPORTS/CURRENT/Next_Day_Dispatch/'\n"
            "                || filename\n"
            "            AND num_nonnulls(",
        ):
            self.assertIn(preserved_identity, identity)
        self.assertIn(
            "            AND num_nonnulls(\n"
            "                artifact_published_at,\n"
            "                artifact_downloaded_at,\n"
            "                publication_id\n"
            "            ) = 3\n"
            "            AND filename =\n"
            "                'PUBLIC_NEXT_DAY_DISPATCH_' || to_char(report_date, 'YYYYMMDD')\n"
            "                || '_' || publication_id || '.zip'",
            identity,
        )
        self.assertIn("DROP CONSTRAINT IF EXISTS historical_source_artifacts_nested_metadata_ck", migration)
        self.assertIn("DROP CONSTRAINT IF EXISTS historical_source_artifacts_archive_identity_ck", migration)
        for forbidden in ("DROP TABLE", "DROP COLUMN", "TRUNCATE ", "DELETE FROM", "UPDATE "):
            self.assertNotIn(forbidden, upper)
        self.assertNotIn("SOURCE_URL LIKE", upper)
        self.assertNotIn("SOURCE_URL ~", upper)
        self.assertEqual(migrate_script.count('--dbname="$database_url"'), 11)


if __name__ == "__main__":
    unittest.main()
