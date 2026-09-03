"""Two-pass monthly Next Day SOC backfill claim execution."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from importlib import import_module
from typing import Any, Iterator

from .backfill_artifacts import (
    BackfillArtifactReceipt,
    PostgreSQLBackfillArtifactRegistrar,
)
from .backfill_ledger import (
    BackfillClaim,
    PostgreSQLBackfillLedger,
)
from .battery_assets import BatteryAsset
from .nemweb_http import NemwebHttpResource, fetch_nemweb_resource
from .nested_source_artifacts import (
    NestedSourceArtifactReceipt,
    PostgreSQLNestedSourceArtifactRegistrar,
)
from .nextday_archives import NextDayMonthlyArchiveRef, nextday_report_date_bounds
from .nextday_monthly_extraction import (
    NextDayDailyArtifact,
    NextDayDailyMemberRef,
    NextDayMonthlyArchiveManifest,
    read_nextday_daily_artifact,
    validate_nextday_monthly_archive,
)
from .nextday_fcas_ingestion import PostgreSQLNextDayFcasIngestor
from .nextday_soc import parse_nextday_unit_solution_soc
from .nextday_soc_ingestion import PostgreSQLNextDaySocIngestor

UTC = timezone.utc
_MAX_MONTHLY_ARCHIVE_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class HistoricalNextDayBackfillResult:
    artifact_sha256: str
    outer_replayed: bool
    daily_artifact_count: int
    daily_artifacts_replayed: int
    source_rows: int
    raw_inserted: int
    raw_replayed: int
    effective_candidates: int
    effective_applied: int
    effective_replayed: int
    source_null_count: int
    percentage_count: int
    fcas_raw_inserted: int = 0
    fcas_raw_replayed: int = 0
    fcas_effective_candidates: int = 0
    fcas_effective_applied: int = 0
    fcas_effective_replayed: int = 0
    fcas_reported_service_count: int = 0

    @property
    def fcas_inserted_count(self) -> int:
        return self.fcas_raw_inserted

    @property
    def fcas_replayed_count(self) -> int:
        return self.fcas_raw_replayed

    @property
    def fcas_candidate_count(self) -> int:
        return self.fcas_effective_candidates

    @property
    def fcas_applied_count(self) -> int:
        return self.fcas_effective_applied

    @property
    def fcas_effective_replayed_count(self) -> int:
        return self.fcas_effective_replayed

    @property
    def fcas_reported_count(self) -> int:
        return self.fcas_reported_service_count


@dataclass(frozen=True, slots=True)
class _ValidatedDaily:
    member: NextDayDailyMemberRef
    artifact_sha256: str
    downloaded_at: datetime
    selected_rows: int


def _default_connect(database_url: str, **kwargs: object) -> Any:
    return import_module("psycopg").connect(database_url, **kwargs)


@contextmanager
def _connection_scope(
    connect: Callable[..., Any],
    database_url: str,
) -> Iterator[Any]:
    connection = connect(database_url)
    try:
        yield connection
    finally:
        connection.close()


def _require_utc(value: datetime, field: str) -> None:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"{field} must be UTC-aware")


def _source_last_modified(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        raise ValueError("source Last-Modified must include a timezone")
    return parsed.astimezone(UTC)


def _candidate_report_dates(start: datetime, end: datetime) -> frozenset[date]:
    bounds = nextday_report_date_bounds(start, end)
    if bounds is None:
        return frozenset()
    first, last = bounds
    dates: set[date] = set()
    current = first
    while current <= last:
        dates.add(current)
        current += timedelta(days=1)
    return frozenset(dates)


def _validate_inputs(
    assets: Iterable[BatteryAsset],
    claim: BackfillClaim,
    start: datetime,
    end: datetime,
    ingestion_version: int,
) -> tuple[BatteryAsset, ...]:
    materialized_assets = tuple(assets)
    if not materialized_assets or any(type(asset) is not BatteryAsset for asset in materialized_assets):
        raise ValueError("assets must contain reviewed BatteryAsset records")
    if type(claim) is not BackfillClaim or claim.feed != "nextday_soc":
        raise ValueError("claim must be a nextday_soc BackfillClaim")
    if claim.report_date.day != 1:
        raise ValueError("nextday_soc claim must use the first day of the month")
    _require_utc(start, "start")
    _require_utc(end, "end")
    if end <= start:
        raise ValueError("end must be later than start")
    if type(ingestion_version) is not int or ingestion_version < 1:
        raise ValueError("ingestion_version must be a positive integer")
    return materialized_assets


def _fail_claim(
    database_url: str,
    claim: BackfillClaim,
    error_summary: str,
    *,
    connect: Callable[..., Any],
    ledger_factory: Callable[[Any], Any],
) -> None:
    with _connection_scope(connect, database_url) as connection:
        ledger_factory(connection).fail(claim, error_summary=error_summary)


def run_nextday_soc_backfill_claim(
    database_url: str,
    assets: Iterable[BatteryAsset],
    claim: BackfillClaim,
    start: datetime,
    end: datetime,
    *,
    ingestion_version: int,
    connect: Callable[..., Any] = _default_connect,
    fetch: Callable[..., NemwebHttpResource] = fetch_nemweb_resource,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ledger_factory: Callable[[Any], Any] = PostgreSQLBackfillLedger,
    registrar_factory: Callable[[Any], Any] = PostgreSQLBackfillArtifactRegistrar,
    nested_registrar_factory: Callable[[Any], Any] = PostgreSQLNestedSourceArtifactRegistrar,
    validate_archive: Callable[
        [NextDayMonthlyArchiveRef, bytes], NextDayMonthlyArchiveManifest
    ] = validate_nextday_monthly_archive,
    read_daily: Callable[
        [NextDayMonthlyArchiveManifest, bytes, NextDayDailyMemberRef],
        NextDayDailyArtifact,
    ] = read_nextday_daily_artifact,
    parse_csv: Callable[..., tuple[Any, ...]] = parse_nextday_unit_solution_soc,
    ingestor_factory: Callable[[Any], Any] = PostgreSQLNextDaySocIngestor,
    fcas_ingestor_factory: Callable[[Any], Any] = PostgreSQLNextDayFcasIngestor,
) -> Any:
    """Validate all selected daily reports before any SOC observation write."""

    materialized_assets = _validate_inputs(
        assets, claim, start, end, ingestion_version
    )
    try:
        downloaded_at = clock()
        _require_utc(downloaded_at, "downloaded_at")
        resource = fetch(claim.source_url, max_bytes=_MAX_MONTHLY_ARCHIVE_BYTES)
        if resource.requested_url != claim.source_url or resource.resolved_url != claim.source_url:
            raise ValueError("monthly archive response URL changed")
        outer_receipt = BackfillArtifactReceipt(
            claim,
            downloaded_at,
            _source_last_modified(resource.last_modified),
            resource.body,
        )
        with _connection_scope(connect, database_url) as connection:
            outer_result = registrar_factory(connection).record(outer_receipt)

        reference = NextDayMonthlyArchiveRef(
            claim.report_date,
            claim.source_url.rsplit("/", 1)[-1],
            claim.source_url,
            len(resource.body),
            downloaded_at,
        )
        manifest = validate_archive(reference, resource.body)
        if manifest.sha256 != outer_result.artifact_sha256:
            raise ValueError("monthly manifest SHA does not match registered artifact")
        candidate_dates = _candidate_report_dates(start, end)
        selected_members = tuple(
            member for member in manifest.members if member.report_date in candidate_dates
        )
        if not selected_members:
            raise ValueError("no daily Next Day artifact intersects the requested range")
        duids = frozenset(asset.duid for asset in materialized_assets)
        validated: list[_ValidatedDaily] = []
        daily_artifacts_replayed = 0
        for member in selected_members:
            daily = read_daily(manifest, resource.body, member)
            nested_receipt = NestedSourceArtifactReceipt(
                outer_result.artifact_sha256,
                member.report_date,
                claim.source_url,
                member.filename,
                member.publication_id,
                member.artifact_published_at,
                downloaded_at,
                daily.raw_zip_bytes,
            )
            with _connection_scope(connect, database_url) as connection:
                nested_result = nested_registrar_factory(connection).record(
                    nested_receipt
                )
            if nested_result.artifact_sha256 != daily.sha256:
                raise ValueError("daily artifact SHA does not match registration")
            daily_artifacts_replayed += int(nested_result.replayed)
            observations = tuple(parse_csv(
                daily.csv_bytes.decode("utf-8-sig", errors="strict"),
                duids=duids,
                source_artifact_id=daily.sha256,
                downloaded_at=nested_result.downloaded_at,
                ingestion_version=ingestion_version,
                correction_version=int(member.publication_id),
            ))
            selected = tuple(
                observation
                for observation in observations
                if start <= observation.interval_start < end
            )
            validated.append(
                _ValidatedDaily(
                    member,
                    daily.sha256,
                    nested_result.downloaded_at,
                    len(selected),
                )
            )

        source_rows = 0
        raw_inserted = 0
        raw_replayed = 0
        effective_candidates = 0
        effective_applied = 0
        effective_replayed = 0
        source_null_count = 0
        percentage_count = 0
        fcas_raw_inserted = 0
        fcas_raw_replayed = 0
        fcas_effective_candidates = 0
        fcas_effective_applied = 0
        fcas_effective_replayed = 0
        fcas_reported_service_count = 0
        fcas_projection_seen = False
        for validated_daily in validated:
            daily = read_daily(
                manifest, resource.body, validated_daily.member
            )
            if daily.sha256 != validated_daily.artifact_sha256:
                raise ValueError("daily artifact changed between validation passes")
            observations = tuple(parse_csv(
                daily.csv_bytes.decode("utf-8-sig", errors="strict"),
                duids=duids,
                source_artifact_id=daily.sha256,
                downloaded_at=validated_daily.downloaded_at,
                ingestion_version=ingestion_version,
                correction_version=int(validated_daily.member.publication_id),
            ))
            selected = tuple(
                observation
                for observation in observations
                if start <= observation.interval_start < end
            )
            if len(selected) != validated_daily.selected_rows:
                raise ValueError("selected daily rows changed between validation passes")
            if not selected:
                continue
            with _connection_scope(connect, database_url) as connection:
                ingestion = ingestor_factory(connection).ingest(
                    selected, materialized_assets
                )
            source_rows += ingestion.source_rows
            raw_inserted += ingestion.raw_inserted
            raw_replayed += ingestion.raw_replayed
            effective_candidates += ingestion.effective_candidates
            effective_applied += ingestion.effective_applied
            effective_replayed += ingestion.effective_replayed
            source_null_count += ingestion.source_null_count
            percentage_count += ingestion.percentage_count

            # Older injected parsers used by callers may still return envelope
            # objects without the additive FCAS field.  The production parser
            # always supplies it; retaining this narrow compatibility path does
            # not let a real FCAS-bearing observation skip its projection.
            has_fcas = [hasattr(item, "fcas") for item in selected]
            if any(has_fcas) and not all(has_fcas):
                raise ValueError("mixed Next Day SOC/FCAS observation shape")
            if not has_fcas or not has_fcas[0]:
                continue
            with _connection_scope(connect, database_url) as connection:
                fcas_ingestion = fcas_ingestor_factory(connection).ingest(selected)
            fcas_projection_seen = True
            fcas_raw_inserted += fcas_ingestion.raw_inserted
            fcas_raw_replayed += fcas_ingestion.raw_replayed
            fcas_effective_candidates += fcas_ingestion.effective_candidates
            fcas_effective_applied += fcas_ingestion.effective_applied
            fcas_effective_replayed += fcas_ingestion.effective_replayed
            fcas_reported_service_count += fcas_ingestion.reported_service_count

        fully_replayed = (
            outer_result.replayed
            and daily_artifacts_replayed == len(validated)
            and raw_inserted == 0
            and raw_replayed == source_rows
            and effective_applied == 0
            and effective_replayed == effective_candidates
            and (
                not fcas_projection_seen
                or (
                    fcas_raw_inserted == 0
                    and fcas_raw_replayed == source_rows
                    and fcas_effective_applied == 0
                    and fcas_effective_replayed == fcas_effective_candidates
                )
            )
        )
        with _connection_scope(connect, database_url) as connection:
            ledger_factory(connection).complete(
                claim, records_imported=source_rows
            )
        return HistoricalNextDayBackfillResult(
            outer_result.artifact_sha256,
            outer_result.replayed,
            len(validated),
            daily_artifacts_replayed,
            source_rows,
            raw_inserted,
            raw_replayed,
            effective_candidates,
            effective_applied,
            effective_replayed,
            source_null_count,
            percentage_count,
            fcas_raw_inserted,
            fcas_raw_replayed,
            fcas_effective_candidates,
            fcas_effective_applied,
            fcas_effective_replayed,
            fcas_reported_service_count,
        )
    except Exception as exc:
        _fail_claim(
            database_url,
            claim,
            type(exc).__name__,
            connect=connect,
            ledger_factory=ledger_factory,
        )
        raise


__all__ = [
    "HistoricalNextDayBackfillResult",
    "run_nextday_soc_backfill_claim",
]
