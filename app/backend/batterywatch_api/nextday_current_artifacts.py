"""Immutable provenance and completion reads for CURRENT Next Day artifacts."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import re
from typing import Any, Protocol

from .nemweb_nextday_current import (
    NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES,
    NEXTDAY_CURRENT_INDEX_URL,
    NemwebNextDayCurrentError,
    NextDayCurrentArtifact,
    NextDayCurrentArtifactRef,
    validate_nextday_current_reference,
)


UTC = timezone.utc
_SHA_RE = re.compile(r"[0-9a-f]{64}", re.ASCII)
_FILENAME_RE = re.compile(
    r"PUBLIC_NEXT_DAY_DISPATCH_(?P<date>[0-9]{8})_(?P<publication>[0-9]{1,32})\.zip",
    re.ASCII,
)


@dataclass(frozen=True, slots=True)
class NextDayCurrentArtifactReceipt:
    """Evidence supplied to the direct CURRENT artifact registrar."""

    report_date: date
    source_url: str
    filename: str
    publication_id: str
    artifact_published_at: datetime
    downloaded_at: datetime
    raw_bytes: bytes

    @classmethod
    def from_reference(
        cls,
        reference: NextDayCurrentArtifactRef,
        downloaded_at: datetime,
        raw_bytes: bytes,
    ) -> "NextDayCurrentArtifactReceipt":
        reference = validate_nextday_current_reference(reference)
        return cls(
            reference.report_date,
            reference.url,
            reference.filename,
            reference.publication_id,
            reference.listing_timestamp,
            downloaded_at,
            raw_bytes,
        )

    @classmethod
    def from_artifact(
        cls,
        artifact: NextDayCurrentArtifact,
        downloaded_at: datetime,
    ) -> "NextDayCurrentArtifactReceipt":
        if type(artifact) is not NextDayCurrentArtifact:
            raise TypeError("artifact must be NextDayCurrentArtifact")
        return cls.from_reference(artifact.reference, downloaded_at, artifact.raw_zip)


@dataclass(frozen=True, slots=True)
class NextDayCurrentArtifactResult:
    """Identity of a retained artifact and its canonical first-seen clock."""

    artifact_sha256: str
    byte_count: int
    downloaded_at: datetime
    replayed: bool


class NextDayCurrentArtifactConflictError(ValueError):
    """Stored CURRENT evidence conflicts with immutable supplied evidence."""


class _Cursor(Protocol):
    def execute(self, statement: str, parameters: tuple[Any, ...]) -> None: ...

    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> Iterable[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


@contextmanager
def _managed_cursor(connection: _Connection) -> Iterator[_Cursor]:
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


_ARTIFACT_INSERT_SQL = """
INSERT INTO historical_source_artifacts (
    artifact_sha256, feed, report_date, source_url, filename,
    byte_count, raw_bytes, parent_artifact_sha256,
    artifact_published_at, artifact_downloaded_at, publication_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING RETURNING 1
"""

_ARTIFACT_SELECT_SQL = """
SELECT feed, report_date, source_url, filename, byte_count, raw_bytes,
       parent_artifact_sha256, artifact_published_at,
       artifact_downloaded_at, publication_id
FROM historical_source_artifacts
WHERE artifact_sha256 = %s
"""

_COMPLETION_SELECT_SQL = """
SELECT a.report_date, a.artifact_sha256, a.source_url, a.filename,
       a.artifact_published_at, a.artifact_downloaded_at, a.publication_id,
       EXISTS (
           SELECT 1
           FROM raw_nextday_soc_observations soc
           WHERE soc.artifact_sha256 = a.artifact_sha256
       ) AS has_raw_soc,
       EXISTS (
           SELECT 1
           FROM raw_nextday_fcas_observations fcas
           WHERE fcas.artifact_sha256 = a.artifact_sha256
       ) AS has_raw_fcas
FROM historical_source_artifacts AS a
WHERE a.feed = 'nextday_soc'
  AND a.parent_artifact_sha256 IS NULL
  AND a.report_date = ANY(%s)
  AND a.source_url = ANY(%s)
  AND a.filename = ANY(%s)
  AND a.publication_id = ANY(%s)
  AND a.artifact_published_at IS NOT NULL
  AND a.artifact_downloaded_at IS NOT NULL
  AND octet_length(a.raw_bytes) > 0
"""


def _utc(value: object, field: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise ValueError(f"{field} must be UTC-aware")
    try:
        if value.utcoffset() is None:
            raise ValueError
        return value.astimezone(UTC)
    except Exception:
        raise ValueError(f"{field} must be UTC-aware") from None


def _validate_receipt(receipt: object) -> NextDayCurrentArtifactReceipt:
    if type(receipt) is not NextDayCurrentArtifactReceipt:
        raise TypeError("receipt must be NextDayCurrentArtifactReceipt")
    if type(receipt.report_date) is not date:
        raise TypeError("report_date must be a date")
    if type(receipt.filename) is not str:
        raise TypeError("filename must be a string")
    match = _FILENAME_RE.fullmatch(receipt.filename)
    if match is None:
        raise ValueError("invalid CURRENT Next Day filename")
    try:
        filename_date = datetime.strptime(
            match.group("date"), "%Y%m%d"
        ).date()
    except (OverflowError, ValueError):
        raise ValueError("invalid CURRENT Next Day filename") from None
    if filename_date != receipt.report_date:
        raise ValueError("filename does not match report_date")
    if (
        type(receipt.publication_id) is not str
        or receipt.publication_id != match.group("publication")
    ):
        raise ValueError("invalid publication identity")
    expected_url = NEXTDAY_CURRENT_INDEX_URL + receipt.filename
    if receipt.source_url != expected_url:
        raise ValueError("source_url does not match CURRENT Next Day identity")
    published = _utc(receipt.artifact_published_at, "artifact_published_at")
    downloaded = _utc(receipt.downloaded_at, "downloaded_at")
    if published > downloaded:
        raise ValueError("artifact publication cannot follow download")
    if type(receipt.raw_bytes) is not bytes:
        raise TypeError("raw_bytes must be immutable bytes")
    if not 1 <= len(receipt.raw_bytes) <= NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES:
        raise ValueError("raw_bytes size is outside accepted bounds")
    return receipt


def _stored_bytes(value: object) -> bytes | None:
    if type(value) is bytes:
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    return None


def _stored_downloaded_at(value: object) -> datetime:
    return _utc(value, "stored artifact_downloaded_at")


def _stored_matches(
    receipt: NextDayCurrentArtifactReceipt,
    stored: object,
) -> tuple[bool, datetime | None]:
    if not isinstance(stored, tuple) or len(stored) != 10:
        return False, None
    stored_bytes = _stored_bytes(stored[5])
    if stored_bytes is None:
        return False, None
    try:
        stored_downloaded = _stored_downloaded_at(stored[8])
        stored_published = _utc(
            stored[7], "stored artifact_published_at"
        )
    except (TypeError, ValueError):
        return False, None
    if type(stored[4]) is not int or isinstance(stored[4], bool):
        return False, None
    expected = (
        "nextday_soc",
        receipt.report_date,
        receipt.source_url,
        receipt.filename,
        len(receipt.raw_bytes),
        receipt.raw_bytes,
        None,
        receipt.artifact_published_at,
        receipt.publication_id,
    )
    actual = (
        stored[0],
        stored[1],
        stored[2],
        stored[3],
        stored[4],
        stored_bytes,
        stored[6],
        stored_published,
        stored[9],
    )
    return actual == expected and stored_downloaded >= stored_published, stored_downloaded


def _validate_completion_references(
    references: Iterable[NextDayCurrentArtifactRef],
) -> tuple[NextDayCurrentArtifactRef, ...]:
    try:
        materialized = tuple(references)
    except (TypeError, ValueError):
        raise ValueError("invalid CURRENT Next Day references") from None
    if not materialized or any(
        type(reference) is not NextDayCurrentArtifactRef
        for reference in materialized
    ):
        raise ValueError("invalid CURRENT Next Day references")
    normalized: list[NextDayCurrentArtifactRef] = []
    seen: set[date] = set()
    for reference in materialized:
        try:
            validated = validate_nextday_current_reference(reference)
        except (TypeError, ValueError, NemwebNextDayCurrentError) as error:
            raise ValueError("invalid CURRENT Next Day references") from error
        if validated.report_date in seen:
            raise ValueError("duplicate CURRENT Next Day report date")
        seen.add(validated.report_date)
        normalized.append(validated)
    return tuple(normalized)


class PostgreSQLNextDayCurrentArtifactRegistrar:
    """Register direct CURRENT bytes and expose their durable completion state."""

    def __init__(self, connection: _Connection):
        self._connection = connection

    def record(
        self,
        receipt: NextDayCurrentArtifactReceipt,
    ) -> NextDayCurrentArtifactResult:
        try:
            receipt = _validate_receipt(receipt)
            digest = sha256(receipt.raw_bytes).hexdigest()
            byte_count = len(receipt.raw_bytes)
        except Exception:
            try:
                self._connection.rollback()
            except Exception:
                pass
            raise
        try:
            with _managed_cursor(self._connection) as cursor:
                cursor.execute(
                    _ARTIFACT_INSERT_SQL,
                    (
                        digest,
                        "nextday_soc",
                        receipt.report_date,
                        receipt.source_url,
                        receipt.filename,
                        byte_count,
                        receipt.raw_bytes,
                        None,
                        receipt.artifact_published_at,
                        receipt.downloaded_at,
                        receipt.publication_id,
                    ),
                )
                insert_result = cursor.fetchone()
                if insert_result is not None and insert_result != (1,):
                    raise NextDayCurrentArtifactConflictError(
                        "invalid CURRENT artifact insert result"
                    )
                inserted = insert_result is not None
                if inserted:
                    canonical_downloaded_at = receipt.downloaded_at
                else:
                    cursor.execute(_ARTIFACT_SELECT_SQL, (digest,))
                    stored = cursor.fetchone()
                    matches, canonical_downloaded_at = _stored_matches(
                        receipt, stored
                    )
                    if not matches or canonical_downloaded_at is None:
                        raise NextDayCurrentArtifactConflictError(
                            "conflicting CURRENT Next Day source artifact"
                        )
            self._connection.commit()
            return NextDayCurrentArtifactResult(
                digest,
                byte_count,
                canonical_downloaded_at,
                not inserted,
            )
        except Exception:
            try:
                self._connection.rollback()
            except Exception:
                pass
            raise

    def read_completed_report_dates(
        self,
        references: Iterable[NextDayCurrentArtifactRef],
    ) -> frozenset[date]:
        """Return supplied dates having both non-empty raw projections."""

        materialized = _validate_completion_references(references)
        report_dates = [reference.report_date for reference in materialized]
        source_urls = [reference.url for reference in materialized]
        filenames = [reference.filename for reference in materialized]
        publication_ids = [reference.publication_id for reference in materialized]
        by_date = {reference.report_date: reference for reference in materialized}
        completed: set[date] = set()
        seen_rows: set[date] = set()
        try:
            with _managed_cursor(self._connection) as cursor:
                cursor.execute(
                    _COMPLETION_SELECT_SQL,
                    (report_dates, source_urls, filenames, publication_ids),
                )
                rows = cursor.fetchall()
                try:
                    materialized_rows = tuple(rows)
                except (TypeError, ValueError):
                    raise NextDayCurrentArtifactConflictError(
                        "invalid CURRENT Next Day completion result"
                    ) from None
                for row in materialized_rows:
                    if not isinstance(row, tuple) or len(row) != 9:
                        raise NextDayCurrentArtifactConflictError(
                            "invalid CURRENT Next Day completion result"
                        )
                    (
                        report_date,
                        digest,
                        source_url,
                        filename,
                        published,
                        downloaded,
                        publication_id,
                        has_soc,
                        has_fcas,
                    ) = row
                    if type(report_date) is not date:
                        raise NextDayCurrentArtifactConflictError(
                            "invalid CURRENT Next Day completion result"
                        )
                    reference = by_date.get(report_date)
                    if reference is None:
                        raise NextDayCurrentArtifactConflictError(
                            "invalid CURRENT Next Day completion result"
                        )
                    if report_date in seen_rows:
                        raise NextDayCurrentArtifactConflictError(
                            "conflicting CURRENT Next Day completion result"
                        )
                    seen_rows.add(report_date)
                    try:
                        downloaded_utc = _utc(
                            downloaded, "stored artifact_downloaded_at"
                        )
                        published_utc = _utc(
                            published, "stored artifact_published_at"
                        )
                    except (TypeError, ValueError):
                        raise NextDayCurrentArtifactConflictError(
                            "conflicting CURRENT Next Day completion result"
                        ) from None
                    if (
                        type(digest) is not str
                        or _SHA_RE.fullmatch(digest) is None
                        or type(source_url) is not str
                        or type(filename) is not str
                        or type(publication_id) is not str
                        or source_url != reference.url
                        or filename != reference.filename
                        or publication_id != reference.publication_id
                        or published_utc != reference.listing_timestamp
                        or downloaded_utc < published_utc
                        or type(has_soc) is not bool
                        or type(has_fcas) is not bool
                    ):
                        raise NextDayCurrentArtifactConflictError(
                            "conflicting CURRENT Next Day completion result"
                        )
                    if has_soc and has_fcas:
                        completed.add(report_date)
            return frozenset(completed)
        except Exception:
            try:
                self._connection.rollback()
            except Exception:
                pass
            raise


def read_completed_nextday_current_report_dates(
    connection: _Connection,
    references: Iterable[NextDayCurrentArtifactRef],
) -> frozenset[date]:
    """Read completion for a bounded supplied reference set."""

    return PostgreSQLNextDayCurrentArtifactRegistrar(connection).read_completed_report_dates(
        references
    )


__all__ = [
    "NextDayCurrentArtifactConflictError",
    "NextDayCurrentArtifactReceipt",
    "NextDayCurrentArtifactResult",
    "PostgreSQLNextDayCurrentArtifactRegistrar",
    "read_completed_nextday_current_report_dates",
]
