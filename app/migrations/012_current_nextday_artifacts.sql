-- Permit direct immutable daily Next Day artifacts retained from CURRENT.
-- These rows have no monthly parent and carry the CURRENT listing metadata.

BEGIN;

ALTER TABLE historical_source_artifacts
    DROP CONSTRAINT IF EXISTS historical_source_artifacts_nested_metadata_ck,
    ADD CONSTRAINT historical_source_artifacts_nested_metadata_ck CHECK (
        (
            num_nonnulls(
                parent_artifact_sha256,
                artifact_published_at,
                artifact_downloaded_at,
                publication_id
            ) IN (0, 4)
            OR (
                feed = 'nextday_soc'
                AND parent_artifact_sha256 IS NULL
                AND num_nonnulls(
                    artifact_published_at,
                    artifact_downloaded_at,
                    publication_id
                ) = 3
            )
        )
        AND (
            parent_artifact_sha256 IS NULL
            OR parent_artifact_sha256 <> artifact_sha256
        )
        AND (
            publication_id IS NULL
            OR publication_id ~ '^[0-9]{1,32}$'
        )
        AND (
            artifact_published_at IS NULL
            OR artifact_published_at <= artifact_downloaded_at
        )
    );

ALTER TABLE historical_source_artifacts
    DROP CONSTRAINT IF EXISTS historical_source_artifacts_archive_identity_ck,
    ADD CONSTRAINT historical_source_artifacts_archive_identity_ck CHECK (
        (
            feed = 'dispatch_price'
            AND parent_artifact_sha256 IS NULL
            AND source_url =
                'https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/' || filename
            AND filename = 'PUBLIC_DISPATCHIS_' || to_char(report_date, 'YYYYMMDD') || '.zip'
        )
        OR (
            feed = 'dispatch_scada'
            AND parent_artifact_sha256 IS NULL
            AND source_url =
                'https://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/' || filename
            AND filename = 'PUBLIC_DISPATCHSCADA_' || to_char(report_date, 'YYYYMMDD') || '.zip'
        )
        OR (
            feed = 'nextday_soc'
            AND parent_artifact_sha256 IS NULL
            AND report_date = date_trunc('month', report_date)::date
            AND source_url =
                'https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/' || filename
            AND filename =
                'PUBLIC_NEXT_DAY_DISPATCH_' || to_char(report_date, 'YYYYMM') || '01.zip'
        )
        OR (
            feed = 'nextday_soc'
            AND parent_artifact_sha256 IS NOT NULL
            AND source_url =
                'https://www.nemweb.com.au/REPORTS/ARCHIVE/Next_Day_Dispatch/'
                || 'PUBLIC_NEXT_DAY_DISPATCH_'
                || to_char(report_date, 'YYYYMM') || '01.zip'
                || '#' || filename
            AND filename =
                'PUBLIC_NEXT_DAY_DISPATCH_' || to_char(report_date, 'YYYYMMDD')
                || '_' || publication_id || '.zip'
        )
        OR (
            feed = 'nextday_soc'
            AND parent_artifact_sha256 IS NULL
            AND source_url =
                'https://www.nemweb.com.au/REPORTS/CURRENT/Next_Day_Dispatch/'
                || filename
            AND num_nonnulls(
                artifact_published_at,
                artifact_downloaded_at,
                publication_id
            ) = 3
            AND filename =
                'PUBLIC_NEXT_DAY_DISPATCH_' || to_char(report_date, 'YYYYMMDD')
                || '_' || publication_id || '.zip'
        )
    );

COMMIT;
