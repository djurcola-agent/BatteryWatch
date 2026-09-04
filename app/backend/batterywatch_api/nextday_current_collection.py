"""Bounded, independently callable CURRENT Next Day collection cycle."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from importlib import import_module
import re
from typing import Any

from .battery_assets import BatteryAsset
from .nemweb_http import NemwebHttpResource, fetch_nemweb_resource
from .nemweb_nextday_current import (
    NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES,
    NEXTDAY_CURRENT_INDEX_MAX_BYTES,
    NEXTDAY_CURRENT_INDEX_URL,
    NemwebNextDayCurrentError,
    NextDayCurrentArtifact,
    NextDayCurrentArtifactRef,
    discover_nextday_current_artifacts,
    extract_nextday_current_zip,
    validate_nextday_current_reference,
)
from .nextday_current_artifacts import (
    NextDayCurrentArtifactReceipt,
    NextDayCurrentArtifactResult,
    PostgreSQLNextDayCurrentArtifactRegistrar,
)
from .nextday_fcas_ingestion import (
    NextDayFcasIngestionResult,
    PostgreSQLNextDayFcasIngestor,
)
from .nextday_soc import NextDaySocObservation, parse_nextday_unit_solution_soc
from .nextday_soc_ingestion import (
    NextDaySocIngestionResult,
    PostgreSQLNextDaySocIngestor,
)


UTC = timezone.utc
_MAX_BIGINT = (1 << 63) - 1
_SAFE_ERROR_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$", re.ASCII)


@dataclass(frozen=True, slots=True)
class NextDayCurrentArtifactFailure:
    """Sanitized failure evidence for one selected report date."""

    report_date: date
    error_type: str
    location: str


@dataclass(frozen=True, slots=True)
class NextDayCurrentProjectionTotals:
    """Bounded totals for one of the two durable projections."""

    attempted: int
    completed: int
    replayed: int
    failed: int
    remaining: int
    source_rows: int
    raw_inserted: int
    raw_replayed: int
    effective_candidates: int
    effective_applied: int
    effective_replayed: int
    source_null_count: int = 0
    percentage_count: int = 0
    reported_service_count: int = 0


@dataclass(frozen=True, slots=True)
class NextDayCurrentCollectionResult:
    """Structured result of one bounded CURRENT convergence attempt."""

    attempted: int
    completed: int
    replayed: int
    failed: int
    remaining: int
    completed_report_dates: tuple[date, ...]
    min_completed_report_date: date | None
    max_completed_report_date: date | None
    failures: tuple[NextDayCurrentArtifactFailure, ...]
    soc: NextDayCurrentProjectionTotals
    fcas: NextDayCurrentProjectionTotals


class NextDayCurrentCollectionError(ValueError):
    """Raised when cycle-wide inputs or the CURRENT index are unusable."""


def _default_connect(database_url: str, *, connect_timeout: int = 10) -> Any:
    return import_module("psycopg").connect(
        database_url,
        connect_timeout=connect_timeout,
    )


@contextmanager
def _connection_scope(
    connect: Callable[..., Any],
    database_url: str,
) -> Iterator[Any]:
    connection = connect(database_url, connect_timeout=10)
    try:
        yield connection
    finally:
        connection.close()


def _validate_cycle_inputs(
    database_url: object,
    assets: Iterable[BatteryAsset],
    limit: int,
    ingestion_version: int,
) -> tuple[BatteryAsset, ...]:
    if type(database_url) is not str or not database_url:
        raise ValueError("database URL is required")
    try:
        materialized = tuple(assets)
    except (TypeError, ValueError):
        raise ValueError("invalid reviewed battery assets") from None
    if not materialized or any(type(asset) is not BatteryAsset for asset in materialized):
        raise ValueError("invalid reviewed battery assets")
    if len({asset.duid for asset in materialized}) != len(materialized):
        raise ValueError("duplicate reviewed battery asset")
    if (
        isinstance(limit, bool)
        or type(limit) is not int
        or not 1 <= limit <= 2
    ):
        raise ValueError("CURRENT Next Day limit must be 1 or 2")
    if (
        isinstance(ingestion_version, bool)
        or type(ingestion_version) is not int
        or not 0 <= ingestion_version <= _MAX_BIGINT
    ):
        raise ValueError("invalid Next Day ingestion version")
    return materialized


def _require_resource(
    resource: object,
    expected_url: str,
    max_bytes: int,
) -> NemwebHttpResource:
    if not isinstance(resource, NemwebHttpResource):
        raise NextDayCurrentCollectionError("invalid NEMWeb resource")
    if (
        resource.requested_url != expected_url
        or resource.resolved_url != expected_url
        or type(resource.body) is not bytes
        or not resource.body
        or len(resource.body) > max_bytes
    ):
        raise NextDayCurrentCollectionError("invalid NEMWeb resource")
    return resource


def _fetch_index(
    fetch: Callable[..., NemwebHttpResource],
    discover: Callable[..., tuple[NextDayCurrentArtifactRef, ...]],
) -> tuple[NextDayCurrentArtifactRef, ...]:
    resource = _require_resource(
        fetch(
            NEXTDAY_CURRENT_INDEX_URL,
            max_bytes=NEXTDAY_CURRENT_INDEX_MAX_BYTES,
        ),
        NEXTDAY_CURRENT_INDEX_URL,
        NEXTDAY_CURRENT_INDEX_MAX_BYTES,
    )
    try:
        html = resource.body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise NextDayCurrentCollectionError("invalid CURRENT index encoding") from error
    references = discover(html, index_url=NEXTDAY_CURRENT_INDEX_URL)
    try:
        materialized = tuple(references)
    except (TypeError, ValueError):
        raise NextDayCurrentCollectionError("invalid CURRENT references") from None
    if not materialized or any(
        type(reference) is not NextDayCurrentArtifactRef
        for reference in materialized
    ):
        raise NextDayCurrentCollectionError("invalid CURRENT references")
    seen: set[date] = set()
    for reference in materialized:
        try:
            validate_nextday_current_reference(reference)
        except (TypeError, ValueError, NemwebNextDayCurrentError) as error:
            raise NextDayCurrentCollectionError("invalid CURRENT references") from error
        if reference.report_date in seen:
            raise NextDayCurrentCollectionError("duplicate CURRENT report date")
        seen.add(reference.report_date)
    if materialized != tuple(sorted(materialized, key=lambda item: item.report_date)):
        raise NextDayCurrentCollectionError("CURRENT references are not ordered")
    return materialized


def _fetch_artifact(
    reference: NextDayCurrentArtifactRef,
    fetch: Callable[..., NemwebHttpResource],
    extract: Callable[[NextDayCurrentArtifactRef, bytes], NextDayCurrentArtifact],
) -> NextDayCurrentArtifact:
    resource = _require_resource(
        fetch(reference.url, max_bytes=NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES),
        reference.url,
        NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES,
    )
    artifact = extract(reference, resource.body)
    if type(artifact) is not NextDayCurrentArtifact:
        raise NextDayCurrentCollectionError("invalid CURRENT artifact result")
    if (
        artifact.reference != reference
        or type(artifact.raw_zip) is not bytes
        or len(artifact.raw_zip) != reference.size_bytes
        or artifact.raw_zip != resource.body
        or artifact.zip_sha256 != sha256(resource.body).hexdigest()
        or type(artifact.csv_bytes) is not bytes
        or type(artifact.csv_text) is not str
        or not artifact.csv_text
    ):
        raise NextDayCurrentCollectionError("invalid CURRENT artifact result")
    return artifact


def _download_time(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError("download clock must return UTC-aware time")
    return value.astimezone(UTC)


def _publication_version(reference: NextDayCurrentArtifactRef) -> int:
    value = reference.publication_id
    if (
        type(value) is not str
        or not value.isascii()
        or not value.isdecimal()
    ):
        raise ValueError("publication id is not numeric")
    try:
        result = int(value)
    except (OverflowError, ValueError):
        raise ValueError("publication id is not numeric") from None
    if not 0 <= result <= _MAX_BIGINT:
        raise ValueError("publication id exceeds database version bounds")
    return result


def _validate_registrar_result(
    result: object,
    receipt: NextDayCurrentArtifactReceipt,
    artifact: NextDayCurrentArtifact,
) -> NextDayCurrentArtifactResult:
    if type(result) is not NextDayCurrentArtifactResult:
        raise NextDayCurrentCollectionError("invalid CURRENT registrar result")
    expected_digest = artifact.zip_sha256
    if (
        result.artifact_sha256 != expected_digest
        or type(result.byte_count) is not int
        or isinstance(result.byte_count, bool)
        or result.byte_count != len(artifact.raw_zip)
        or type(result.replayed) is not bool
        or type(result.downloaded_at) is not datetime
        or result.downloaded_at.tzinfo is None
        or result.downloaded_at.utcoffset() != timedelta(0)
        or result.downloaded_at < receipt.artifact_published_at
        or result.downloaded_at > receipt.downloaded_at
    ):
        raise NextDayCurrentCollectionError("invalid CURRENT registrar result")
    return result


def _validate_soc_result(
    result: object,
    expected_source_rows: int,
) -> NextDaySocIngestionResult:
    if type(result) is not NextDaySocIngestionResult:
        raise NextDayCurrentCollectionError("invalid SOC projection result")
    _validate_nonnegative_counts(result, (
        "source_rows", "raw_inserted", "raw_replayed", "effective_candidates",
        "effective_applied", "effective_replayed", "source_null_count",
        "percentage_count",
    ))
    if (
        result.source_rows != expected_source_rows
        or result.raw_inserted + result.raw_replayed != result.source_rows
        or result.effective_applied + result.effective_replayed
        != result.effective_candidates
        or result.effective_candidates > result.source_rows
        or result.source_null_count > result.source_rows
        or result.percentage_count > result.source_rows
    ):
        raise NextDayCurrentCollectionError("invalid SOC projection result counts")
    return result


def _validate_fcas_result(
    result: object,
    expected_source_rows: int,
) -> NextDayFcasIngestionResult:
    if type(result) is not NextDayFcasIngestionResult:
        raise NextDayCurrentCollectionError("invalid FCAS projection result")
    _validate_nonnegative_counts(result, (
        "source_rows", "raw_inserted", "raw_replayed", "effective_candidates",
        "effective_applied", "effective_replayed", "reported_service_count",
    ))
    if (
        result.source_rows != expected_source_rows
        or result.raw_inserted + result.raw_replayed != result.source_rows
        or result.effective_applied + result.effective_replayed
        != result.effective_candidates
        or result.effective_candidates > result.source_rows
    ):
        raise NextDayCurrentCollectionError("invalid FCAS projection result counts")
    return result


def _validate_nonnegative_counts(result: object, fields: tuple[str, ...]) -> None:
    for field in fields:
        value = getattr(result, field)
        if isinstance(value, bool) or type(value) is not int or value < 0:
            raise NextDayCurrentCollectionError("invalid projection result counts")


def _safe_error_type(error: BaseException) -> str:
    name = type(error).__name__
    return name if _SAFE_ERROR_TYPE.fullmatch(name) else "Error"


@dataclass
class _ProjectionAccumulator:
    attempted: int = 0
    completed: int = 0
    replayed: int = 0
    failed: int = 0
    source_rows: int = 0
    raw_inserted: int = 0
    raw_replayed: int = 0
    effective_candidates: int = 0
    effective_applied: int = 0
    effective_replayed: int = 0
    source_null_count: int = 0
    percentage_count: int = 0
    reported_service_count: int = 0

    def soc(self, result: NextDaySocIngestionResult) -> None:
        self.source_rows += result.source_rows
        self.raw_inserted += result.raw_inserted
        self.raw_replayed += result.raw_replayed
        self.effective_candidates += result.effective_candidates
        self.effective_applied += result.effective_applied
        self.effective_replayed += result.effective_replayed
        self.source_null_count += result.source_null_count
        self.percentage_count += result.percentage_count

    def fcas(self, result: NextDayFcasIngestionResult) -> None:
        self.source_rows += result.source_rows
        self.raw_inserted += result.raw_inserted
        self.raw_replayed += result.raw_replayed
        self.effective_candidates += result.effective_candidates
        self.effective_applied += result.effective_applied
        self.effective_replayed += result.effective_replayed
        self.reported_service_count += result.reported_service_count

    def result(self, remaining: int) -> NextDayCurrentProjectionTotals:
        return NextDayCurrentProjectionTotals(
            self.attempted,
            self.completed,
            self.replayed,
            self.failed,
            remaining,
            self.source_rows,
            self.raw_inserted,
            self.raw_replayed,
            self.effective_candidates,
            self.effective_applied,
            self.effective_replayed,
            self.source_null_count,
            self.percentage_count,
            self.reported_service_count,
        )


def _select_references(
    references: tuple[NextDayCurrentArtifactRef, ...],
    completed: frozenset[date],
    limit: int,
) -> tuple[NextDayCurrentArtifactRef, ...]:
    incomplete = tuple(
        reference
        for reference in references
        if reference.report_date not in completed
    )
    if not incomplete:
        return ()
    if limit == 1:
        return (incomplete[-1],)
    selected = {incomplete[0].report_date, incomplete[-1].report_date}
    return tuple(
        reference for reference in incomplete
        if reference.report_date in selected
    )


def _validate_completed_result(
    completed: object,
    references: tuple[NextDayCurrentArtifactRef, ...],
) -> frozenset[date]:
    if type(completed) is not frozenset or any(
        type(value) is not date for value in completed
    ):
        raise NextDayCurrentCollectionError("invalid CURRENT completion result")
    available = {reference.report_date for reference in references}
    if not completed.issubset(available):
        raise NextDayCurrentCollectionError("invalid CURRENT completion result")
    return completed


def run_nextday_current_cycle(
    database_url: str,
    assets: Iterable[BatteryAsset],
    *,
    limit: int = 2,
    ingestion_version: int = 1,
    connect: Callable[..., Any] = _default_connect,
    fetch: Callable[..., NemwebHttpResource] = fetch_nemweb_resource,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    discover: Callable[..., tuple[NextDayCurrentArtifactRef, ...]] = discover_nextday_current_artifacts,
    extract: Callable[[NextDayCurrentArtifactRef, bytes], NextDayCurrentArtifact] = extract_nextday_current_zip,
    parse_csv: Callable[..., tuple[NextDaySocObservation, ...]] = parse_nextday_unit_solution_soc,
    registrar_factory: Callable[[Any], Any] = PostgreSQLNextDayCurrentArtifactRegistrar,
    soc_ingestor_factory: Callable[[Any], Any] = PostgreSQLNextDaySocIngestor,
    fcas_ingestor_factory: Callable[[Any], Any] = PostgreSQLNextDayFcasIngestor,
) -> NextDayCurrentCollectionResult:
    """Converge at most two incomplete CURRENT report dates per invocation."""

    materialized_assets = _validate_cycle_inputs(
        database_url, assets, limit, ingestion_version
    )
    references = _fetch_index(fetch, discover)
    with _connection_scope(connect, database_url) as connection:
        completed = _validate_completed_result(
            registrar_factory(connection).read_completed_report_dates(references),
            references,
        )
    selected = _select_references(references, completed, limit)
    duids = frozenset(asset.duid for asset in materialized_assets)
    failures: list[NextDayCurrentArtifactFailure] = []
    completed_this_cycle: list[date] = []
    soc_totals = _ProjectionAccumulator()
    fcas_totals = _ProjectionAccumulator()
    replayed = 0

    for reference in selected:
        location = "fetch"
        try:
            artifact = _fetch_artifact(reference, fetch, extract)
            location = "downloaded_at"
            downloaded_at = _download_time(clock)
            receipt = NextDayCurrentArtifactReceipt.from_reference(
                reference,
                downloaded_at,
                artifact.raw_zip,
            )

            location = "registration"
            with _connection_scope(connect, database_url) as connection:
                registered = _validate_registrar_result(
                    registrar_factory(connection).record(receipt),
                    receipt,
                    artifact,
                )
            replayed += int(registered.replayed)

            location = "parse"
            correction_version = _publication_version(reference)
            parsed = parse_csv(
                artifact.csv_text,
                duids=duids,
                source_artifact_id=registered.artifact_sha256,
                downloaded_at=registered.downloaded_at,
                ingestion_version=ingestion_version,
                correction_version=correction_version,
            )
            try:
                observations = tuple(parsed)
            except (TypeError, ValueError):
                raise NextDayCurrentCollectionError(
                    "invalid SOC parser result"
                ) from None
            if not observations or any(
                type(observation) is not NextDaySocObservation
                for observation in observations
            ):
                raise NextDayCurrentCollectionError("invalid SOC parser result")
            if any(
                observation.source_artifact_id != registered.artifact_sha256
                or observation.downloaded_at != registered.downloaded_at
                or observation.ingestion_version != ingestion_version
                or observation.correction_version != correction_version
                for observation in observations
            ):
                raise NextDayCurrentCollectionError(
                    "SOC parser provenance does not match artifact"
                )

            location = "soc_projection"
            soc_totals.attempted += 1
            with _connection_scope(connect, database_url) as connection:
                soc_result = _validate_soc_result(
                    soc_ingestor_factory(connection).ingest(
                        observations,
                        materialized_assets,
                    ),
                    len(observations),
                )
            soc_totals.completed += 1
            soc_totals.replayed += int(
                soc_result.raw_inserted == 0
                and soc_result.raw_replayed == soc_result.source_rows
            )
            soc_totals.soc(soc_result)

            location = "fcas_projection"
            fcas_totals.attempted += 1
            with _connection_scope(connect, database_url) as connection:
                fcas_result = _validate_fcas_result(
                    fcas_ingestor_factory(connection).ingest(
                        observations
                    ),
                    len(observations),
                )
            fcas_totals.completed += 1
            fcas_totals.replayed += int(
                fcas_result.raw_inserted == 0
                and fcas_result.raw_replayed == fcas_result.source_rows
            )
            fcas_totals.fcas(fcas_result)
            completed_this_cycle.append(reference.report_date)
        except Exception as error:
            if location == "soc_projection":
                soc_totals.failed += 1
            elif location == "fcas_projection":
                fcas_totals.failed += 1
            failures.append(
                NextDayCurrentArtifactFailure(
                    reference.report_date,
                    _safe_error_type(error),
                    location,
                )
            )

    completed_dates = tuple(sorted(set(completed_this_cycle)))
    remaining = len(
        tuple(
            reference for reference in references
            if reference.report_date not in completed
            and reference.report_date not in completed_dates
        )
    )
    attempted = len(selected)
    completed_count = len(completed_dates)
    failed = len(failures)
    min_completed = completed_dates[0] if completed_dates else None
    max_completed = completed_dates[-1] if completed_dates else None
    # A failure before a projection is invoked is still a remaining artifact;
    # projection totals retain their own attempted/completed distinction.
    return NextDayCurrentCollectionResult(
        attempted=attempted,
        completed=completed_count,
        replayed=replayed,
        failed=failed,
        remaining=remaining,
        completed_report_dates=completed_dates,
        min_completed_report_date=min_completed,
        max_completed_report_date=max_completed,
        failures=tuple(failures),
        soc=soc_totals.result(remaining),
        fcas=fcas_totals.result(remaining),
    )


__all__ = [
    "NextDayCurrentArtifactFailure",
    "NextDayCurrentCollectionError",
    "NextDayCurrentCollectionResult",
    "NextDayCurrentProjectionTotals",
    "run_nextday_current_cycle",
]
