"""Contract tests for bounded CURRENT Next Day acquisition and projections."""

from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta, timezone, tzinfo
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from stat import S_IFDIR, S_IFLNK
from typing import Any
import unittest
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

from batterywatch_api.battery_assets import BatteryAsset
from batterywatch_api.nemweb_http import NemwebHttpResource
from batterywatch_api.nemweb_nextday_current import (
    NEXTDAY_CURRENT_CSV_MAX_BYTES,
    NEXTDAY_CURRENT_EXPANDED_MAX_BYTES,
    NEXTDAY_CURRENT_INDEX_MAX_BYTES,
    NEXTDAY_CURRENT_INDEX_URL,
    NemwebNextDayCurrentError,
    NextDayCurrentArtifact,
    NextDayCurrentArtifactRef,
    discover_nextday_current_artifacts,
    extract_nextday_current_zip,
    fetch_nextday_current_artifact,
    fetch_nextday_current_index,
)
from batterywatch_api.nextday_current_artifacts import (
    NextDayCurrentArtifactConflictError,
    NextDayCurrentArtifactReceipt,
    NextDayCurrentArtifactResult,
    PostgreSQLNextDayCurrentArtifactRegistrar,
)
from batterywatch_api.nextday_current_collection import (
    NextDayCurrentCollectionResult,
    run_nextday_current_cycle,
)
from batterywatch_api.nextday_fcas_ingestion import NextDayFcasIngestionResult
from batterywatch_api.nextday_soc import NextDaySocObservation
from batterywatch_api.nextday_soc_ingestion import NextDaySocIngestionResult


UTC = timezone.utc
UTC_PLUS_TEN = timezone(timedelta(hours=10))


class _BrokenTzinfo(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        raise ValueError("broken timezone")


BACKEND = Path(__file__).resolve().parents[1]
CSV_TEXT = (
    BACKEND / "tests" / "fixtures" / "historical"
    / "nextday-unit-solution-soc-20260829-reduced.csv"
).read_text(encoding="utf-8")


def _zip(filename: str, payload: str = CSV_TEXT, *, comment: bytes = b"") -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.comment = comment
        archive.writestr(filename.removesuffix(".zip") + ".CSV", payload.encode())
    return buffer.getvalue()


def _zip_with_member_metadata(
    filename: str,
    *,
    extra: bytes = b"",
    member_comment: bytes = b"",
) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        member = ZipInfo(filename.removesuffix(".zip") + ".CSV")
        member.compress_type = ZIP_DEFLATED
        member.extra = extra
        member.comment = member_comment
        archive.writestr(member, CSV_TEXT.encode())
    return buffer.getvalue()


def _zip_with_encryption_flag(filename: str) -> bytes:
    payload = bytearray(_zip(filename))
    for signature in (b"PK\x03\x04", b"PK\x01\x02"):
        offset = 0
        while True:
            offset = payload.find(signature, offset)
            if offset < 0:
                break
            flags = int.from_bytes(payload[offset + 6:offset + 8], "little")
            payload[offset + 6:offset + 8] = (flags | 1).to_bytes(2, "little")
            offset += len(signature)
    return bytes(payload)


def _zip_with_directory_mode(filename: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        member = ZipInfo(filename.removesuffix(".zip") + ".CSV")
        member.compress_type = ZIP_DEFLATED
        member.create_system = 3
        member.external_attr = (S_IFDIR | 0o777) << 16
        archive.writestr(member, CSV_TEXT.encode())
    return buffer.getvalue()


def _zip_with_symlink(filename: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        member = ZipInfo(filename.removesuffix(".zip") + ".CSV")
        member.compress_type = ZIP_DEFLATED
        member.create_system = 3
        member.external_attr = (S_IFLNK | 0o777) << 16
        archive.writestr(member, CSV_TEXT.encode())
    return buffer.getvalue()


def _zip_with_two_members(filename: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(filename.removesuffix(".zip") + ".CSV", CSV_TEXT.encode())
        archive.writestr("UNEXPECTED.CSV", b"unexpected")
    return buffer.getvalue()


def _index_row(filename: str, size: int, timestamp: str) -> str:
    return (
        f"{timestamp} {size} <A HREF=\"/Reports/CURRENT/Next_Day_Dispatch/"
        f"{filename}\">{filename}</A><br>"
    )


def _reference(report_date: date, publication_id: str = "42", *, size: int = 1) -> NextDayCurrentArtifactRef:
    filename = f"PUBLIC_NEXT_DAY_DISPATCH_{report_date:%Y%m%d}_{publication_id}.zip"
    return NextDayCurrentArtifactRef(
        report_date,
        filename,
        NEXTDAY_CURRENT_INDEX_URL + filename,
        publication_id,
        size,
        datetime(2026, 8, 30, 18, 10, tzinfo=UTC),
    )


def _resource(url: str, body: bytes, *, resolved_url: str | None = None) -> NemwebHttpResource:
    return NemwebHttpResource(
        requested_url=url,
        resolved_url=url if resolved_url is None else resolved_url,
        body=body,
        content_type=None,
        etag=None,
        last_modified=None,
    )


def _asset() -> BatteryAsset:
    return BatteryAsset(
        "ADPBA1", "Adelaide Power Battery", "SA1", 10.0, 12.6,
        "reviewed", datetime(2025, 3, 31, tzinfo=UTC),
    )


def _observation(
    source_id: str,
    interval: datetime,
    *,
    ingestion_version: int = 1,
    correction_version: int = 0,
    downloaded_at: datetime | None = None,
) -> NextDaySocObservation:
    return NextDaySocObservation(
        "ADPBA1", interval, 1.0, 0, 1, "20260829001",
        interval - timedelta(minutes=1), source_id,
        interval + timedelta(days=1),
        datetime(2026, 8, 31, tzinfo=UTC) if downloaded_at is None else downloaded_at,
        ingestion_version, correction_version,
    )


class _Cursor:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.rowcount = 1
        self.query = ""

    def execute(self, statement, parameters) -> None:
        self.connection.executions.append((statement, tuple(parameters)))
        self.query = statement

    def fetchone(self):
        if self.connection.fetchone_results:
            return self.connection.fetchone_results.pop(0)
        return None

    def fetchall(self):
        return self.connection.fetchall_result

    def close(self) -> None:
        self.connection.closed += 1


class _Connection:
    def __init__(self, *, fetchone_results=(), fetchall_result=()) -> None:
        self.fetchone_results = list(fetchone_results)
        self.fetchall_result = list(fetchall_result)
        self.executions = []
        self.closed = self.commits = self.rollbacks = 0

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


class _Registrar:
    def __init__(self, connection, completed, calls, failures=()) -> None:
        self.completed = completed
        self.calls = calls
        self.failures = failures

    def read_completed_report_dates(self, references):
        return frozenset(self.completed)

    def record(self, receipt):
        self.calls.append(receipt)
        if receipt.report_date in self.failures:
            raise RuntimeError("deliberately hidden")
        digest = sha256(receipt.raw_bytes).hexdigest()
        self.completed.add(receipt.report_date)
        return NextDayCurrentArtifactResult(
            digest, len(receipt.raw_bytes), receipt.downloaded_at, False
        )


class _ProjectionIngestor:
    def __init__(self, result, calls, failure_dates=()) -> None:
        self.result = result
        self.calls = calls
        self.failure_dates = failure_dates

    def ingest(self, observations, *assets):
        observations = tuple(observations)
        self.calls.append(observations)
        if observations[0].interval_start.date() in self.failure_dates:
            raise RuntimeError("deliberately hidden")
        return self.result


class CurrentNextDayAdapterTests(unittest.TestCase):
    def test_observed_current_member_fits_128_mib_ceiling(self) -> None:
        accepted_ceiling = 128 * 1024 * 1024
        observed_member_bytes = 122_565_144

        self.assertEqual(NEXTDAY_CURRENT_CSV_MAX_BYTES, accepted_ceiling)
        self.assertEqual(
            NEXTDAY_CURRENT_EXPANDED_MAX_BYTES,
            accepted_ceiling,
        )
        self.assertLess(observed_member_bytes, NEXTDAY_CURRENT_CSV_MAX_BYTES)
        self.assertLess(
            observed_member_bytes,
            NEXTDAY_CURRENT_EXPANDED_MAX_BYTES,
        )

    def test_discovers_strict_metadata_and_extracts_immutable_csv(self) -> None:
        first = "PUBLIC_NEXT_DAY_DISPATCH_20260828_41.zip"
        second = "PUBLIC_NEXT_DAY_DISPATCH_20260829_42.zip"
        html = (
            "<html><pre>"
            + _index_row(first, 100, "Friday, August 28, 2026 04:10 AM")
            + _index_row(second, 101, "Saturday, August 29, 2026 04:11 AM")
            + "</pre></html>"
        )

        references = discover_nextday_current_artifacts(
            html, index_url=NEXTDAY_CURRENT_INDEX_URL
        )

        self.assertEqual(
            tuple((item.report_date, item.publication_id, item.size_bytes,
                   item.listing_timestamp) for item in references),
            (
                (date(2026, 8, 28), "41", 100,
                 datetime(2026, 8, 27, 18, 10, tzinfo=UTC)),
                (date(2026, 8, 29), "42", 101,
                 datetime(2026, 8, 28, 18, 11, tzinfo=UTC)),
            ),
        )
        with self.assertRaises(FrozenInstanceError):
            references[0].filename = "changed"  # type: ignore[misc]

        payload = _zip(second)
        artifact = extract_nextday_current_zip(
            _reference(date(2026, 8, 29), size=len(payload)), payload
        )
        self.assertEqual(artifact.csv_bytes, CSV_TEXT.encode())
        self.assertEqual(artifact.csv_text, CSV_TEXT)
        self.assertEqual(artifact.zip_sha256, sha256(payload).hexdigest())
        self.assertEqual(artifact.raw_zip, payload)

    def test_index_fetch_boundary_rejects_redirect_and_oversized_response(self) -> None:
        filename = "PUBLIC_NEXT_DAY_DISPATCH_20260829_42.zip"
        html = _index_row(
            filename, 100, "Saturday, August 29, 2026 04:10 AM"
        )
        with self.assertRaises(NemwebNextDayCurrentError):
            fetch_nextday_current_index(
                fetch=lambda url, *, max_bytes: _resource(
                    url, html.encode(), resolved_url=url + "?redirected"
                ),
            )

        with self.assertRaises(NemwebNextDayCurrentError):
            fetch_nextday_current_index(
                fetch=lambda url, *, max_bytes: _resource(
                    url, b"x" * (NEXTDAY_CURRENT_INDEX_MAX_BYTES + 1)
                ),
            )

    def test_rejects_bad_listing_identity_and_unsafe_zip_evidence(self) -> None:
        filename = "PUBLIC_NEXT_DAY_DISPATCH_20260829_42.zip"
        valid = _index_row(filename, 100, "Saturday, August 29, 2026 04:10 AM")
        cases = (
            valid.replace(filename + "</A>", "WRONG.zip</A>"),
            valid.replace("/Reports/CURRENT/Next_Day_Dispatch/", "/wrong/"),
            valid.replace("_42.zip\">", "_43.zip\">"),
            valid.replace(filename + "</A>", filename + "<!--comment--></A>"),
            valid + valid,
            "<a href=\"readme.txt\">readme</a>",
        )
        for html in cases:
            with self.subTest(html=html):
                with self.assertRaises(NemwebNextDayCurrentError):
                    discover_nextday_current_artifacts(
                        html, index_url=NEXTDAY_CURRENT_INDEX_URL
                    )

    def test_republished_report_date_keeps_final_current_listing_entry(self) -> None:
        """AEMO can republish a report date with a later publication id.

        The live CURRENT listing appends the replacement entry after the
        original row, so the last listing entry per report date is the
        authoritative CURRENT artifact.
        """

        original = _index_row(
            "PUBLIC_NEXT_DAY_DISPATCH_20261028_1.zip", 8647057,
            "Thursday, October 29, 2026 04:12 AM",
        )
        republished = _index_row(
            "PUBLIC_NEXT_DAY_DISPATCH_20261028_2.zip", 8647059,
            "Thursday, October 29, 2026 12:44 PM",
        )
        following = _index_row(
            "PUBLIC_NEXT_DAY_DISPATCH_20261029_3.zip", 8422193,
            "Friday, October 30, 2026 04:12 AM",
        )
        html = "<html><pre>" + original + republished + following + "</pre></html>"

        references = discover_nextday_current_artifacts(
            html, index_url=NEXTDAY_CURRENT_INDEX_URL
        )

        self.assertEqual(
            tuple((item.report_date, item.publication_id, item.size_bytes)
                  for item in references),
            (
                (date(2026, 10, 28), "2", 8647059),
                (date(2026, 10, 29), "3", 8422193),
            ),
        )

    def test_rejects_bad_listing_identity_and_unsafe_zip_evidence(self) -> None:
        reference = _reference(date(2026, 8, 29), size=1)
        invalid_zips = (
            b"not zip",
            _zip(reference.filename, comment=b"comment"),
            _zip_with_member_metadata("WRONG_MEMBER.zip"),
            _zip_with_member_metadata(
                reference.filename, extra=b"\x01\x00\x00\x00"
            ),
            _zip_with_member_metadata(
                reference.filename, member_comment=b"comment"
            ),
            _zip_with_encryption_flag(reference.filename),
            _zip_with_two_members(reference.filename),
            _zip_with_directory_mode(reference.filename),
            _zip_with_symlink(reference.filename),
            _zip(reference.filename, payload="x" * 100_000),
            _zip(reference.filename) + b"trailing-bytes",
        )
        for payload in invalid_zips:
            with self.subTest(size=len(payload)):
                invalid_reference = _reference(
                    reference.report_date,
                    reference.publication_id,
                    size=len(payload),
                )
                with self.assertRaises(NemwebNextDayCurrentError):
                    extract_nextday_current_zip(invalid_reference, payload)


class CurrentNextDayRegistrarTests(unittest.TestCase):
    def test_completion_normalizes_non_utc_stored_timestamps(self) -> None:
        ref = _reference(date(2026, 8, 29), size=123)
        stored_published = datetime(
            2026, 8, 31, 4, 10, tzinfo=UTC_PLUS_TEN
        )
        stored_downloaded = datetime(
            2026, 8, 31, 10, 0, tzinfo=UTC_PLUS_TEN
        )
        row = (
            ref.report_date,
            "a" * 64,
            ref.url,
            ref.filename,
            stored_published,
            stored_downloaded,
            ref.publication_id,
            True,
            True,
        )

        complete = PostgreSQLNextDayCurrentArtifactRegistrar(
            _Connection(fetchall_result=(row,))
        ).read_completed_report_dates((ref,))

        self.assertEqual(complete, frozenset({ref.report_date}))

    def test_completion_rejects_naive_and_broken_stored_timestamps(self) -> None:
        ref = _reference(date(2026, 8, 29), size=123)
        valid_downloaded = datetime(2026, 8, 31, tzinfo=UTC)
        cases = (
            (datetime(2026, 8, 30, 18, 10), valid_downloaded),
            (ref.listing_timestamp, datetime(2026, 8, 31)),
            (
                datetime(2026, 8, 30, 18, 10, tzinfo=_BrokenTzinfo()),
                valid_downloaded,
            ),
            (
                ref.listing_timestamp,
                datetime(2026, 8, 31, tzinfo=_BrokenTzinfo()),
            ),
        )
        for published, downloaded in cases:
            with self.subTest(published=published, downloaded=downloaded):
                row = (
                    ref.report_date,
                    "a" * 64,
                    ref.url,
                    ref.filename,
                    published,
                    downloaded,
                    ref.publication_id,
                    True,
                    True,
                )
                with self.assertRaises(NextDayCurrentArtifactConflictError):
                    PostgreSQLNextDayCurrentArtifactRegistrar(
                        _Connection(fetchall_result=(row,))
                    ).read_completed_report_dates((ref,))

    def test_completion_requires_both_raw_projections_for_exact_current_artifact(self) -> None:
        ref = _reference(date(2026, 8, 29), size=123)
        downloaded_at = datetime(2026, 8, 31, tzinfo=UTC)
        row = (
            ref.report_date,
            "a" * 64,
            ref.url,
            ref.filename,
            ref.listing_timestamp,
            downloaded_at,
            ref.publication_id,
            True,
            True,
        )
        complete_connection = _Connection(fetchall_result=(row,))
        complete = PostgreSQLNextDayCurrentArtifactRegistrar(
            complete_connection
        ).read_completed_report_dates((ref,))
        self.assertEqual(complete, frozenset({ref.report_date}))
        self.assertIn("raw_nextday_soc", complete_connection.executions[0][0])
        self.assertIn("raw_nextday_fcas", complete_connection.executions[0][0])
        self.assertIn(
            "octet_length(a.raw_bytes) > 0",
            complete_connection.executions[0][0],
        )

        partial: list[Any] = list(row)
        partial[-1] = False
        incomplete = PostgreSQLNextDayCurrentArtifactRegistrar(
            _Connection(fetchall_result=(tuple(partial),))
        ).read_completed_report_dates((ref,))
        self.assertEqual(incomplete, frozenset())

    def test_fetch_boundary_rejects_redirect_and_declared_byte_mismatch(self) -> None:
        reference = _reference(date(2026, 8, 29), size=1)
        payload = _zip(reference.filename)
        matching_reference = _reference(
            reference.report_date, reference.publication_id, size=len(payload)
        )

        with self.assertRaises(NemwebNextDayCurrentError):
            fetch_nextday_current_artifact(
                matching_reference,
                fetch=lambda url, *, max_bytes: _resource(
                    url, payload, resolved_url=url + "?redirected"
                ),
            )

        with self.assertRaises(NemwebNextDayCurrentError):
            fetch_nextday_current_artifact(
                _reference(
                    reference.report_date,
                    reference.publication_id,
                    size=len(payload) + 1,
                ),
                fetch=lambda url, *, max_bytes: _resource(url, payload),
            )

    def test_completion_fails_closed_on_malformed_or_conflicting_query_rows(self) -> None:
        ref = _reference(date(2026, 8, 29), size=123)
        downloaded_at = datetime(2026, 8, 31, tzinfo=UTC)
        valid = [
            ref.report_date,
            "a" * 64,
            ref.url,
            ref.filename,
            ref.listing_timestamp,
            downloaded_at,
            ref.publication_id,
            True,
            True,
        ]
        cases = []
        wrong_digest = valid.copy()
        wrong_digest[1] = "not-a-sha"
        cases.append(tuple(wrong_digest))
        wrong_url = valid.copy()
        wrong_url[2] = ref.url + "?different"
        cases.append(tuple(wrong_url))
        wrong_projection_flag = valid.copy()
        wrong_projection_flag[7] = 1
        cases.append(tuple(wrong_projection_flag))
        for bad_row in cases:
            with self.subTest(row=bad_row):
                connection = _Connection(fetchall_result=(bad_row,))
                with self.assertRaises(NextDayCurrentArtifactConflictError):
                    PostgreSQLNextDayCurrentArtifactRegistrar(
                        connection
                    ).read_completed_report_dates((ref,))
                self.assertEqual(connection.rollbacks, 1)

        duplicate_connection = _Connection(
            fetchall_result=(tuple(valid), tuple(valid))
        )
        with self.assertRaises(NextDayCurrentArtifactConflictError):
            PostgreSQLNextDayCurrentArtifactRegistrar(
                duplicate_connection
            ).read_completed_report_dates((ref,))
        self.assertEqual(duplicate_connection.rollbacks, 1)

    def test_rolls_back_receipt_validation_failures(self) -> None:
        payload = b"retained-current-zip"
        ref = _reference(date(2026, 8, 29), size=len(payload))
        invalid = NextDayCurrentArtifactReceipt(
            ref.report_date, "https://invalid.example/", ref.filename,
            ref.publication_id, ref.listing_timestamp,
            datetime(2026, 8, 31, tzinfo=UTC), payload,
        )
        connection = _Connection()

        with self.assertRaises(ValueError):
            PostgreSQLNextDayCurrentArtifactRegistrar(connection).record(invalid)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))
        self.assertEqual(connection.executions, [])

    def test_rejects_malformed_artifact_insert_result(self) -> None:
        payload = b"retained-current-zip"
        ref = _reference(date(2026, 8, 29), size=len(payload))
        receipt = NextDayCurrentArtifactReceipt(
            ref.report_date, ref.url, ref.filename, ref.publication_id,
            ref.listing_timestamp, datetime(2026, 8, 31, tzinfo=UTC), payload,
        )
        connection = _Connection(fetchone_results=((0,),))

        with self.assertRaises(NextDayCurrentArtifactConflictError):
            PostgreSQLNextDayCurrentArtifactRegistrar(connection).record(receipt)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))

    def test_registers_direct_current_artifact_without_a_parent(self) -> None:
        payload = b"retained-current-zip"
        ref = _reference(date(2026, 8, 29), size=len(payload))
        downloaded_at = datetime(2026, 8, 31, tzinfo=UTC)
        receipt = NextDayCurrentArtifactReceipt(
            ref.report_date, ref.url, ref.filename, ref.publication_id,
            ref.listing_timestamp, downloaded_at, payload,
        )
        connection = _Connection(fetchone_results=((1,),))

        result = PostgreSQLNextDayCurrentArtifactRegistrar(connection).record(receipt)

        self.assertFalse(result.replayed)
        self.assertEqual(result.downloaded_at, downloaded_at)
        self.assertEqual((connection.commits, connection.rollbacks), (1, 0))
        parameters = connection.executions[0][1]
        self.assertEqual(
            parameters[1:4], ("nextday_soc", ref.report_date, ref.url)
        )
        self.assertIsNone(parameters[7])
        self.assertEqual(parameters[8:], (
            ref.listing_timestamp, downloaded_at, ref.publication_id
        ))

    def test_replay_normalizes_non_utc_stored_timestamps_and_download_clock(self) -> None:
        payload = b"retained-current-zip"
        ref = _reference(date(2026, 8, 29), size=len(payload))
        stored_published = datetime(
            2026, 8, 31, 4, 10, tzinfo=UTC_PLUS_TEN
        )
        stored_downloaded = datetime(
            2026, 8, 31, 10, 0, tzinfo=UTC_PLUS_TEN
        )
        first_seen = datetime(2026, 8, 31, tzinfo=UTC)
        digest = sha256(payload).hexdigest()
        stored = (
            "nextday_soc", ref.report_date, ref.url, ref.filename, len(payload), payload,
            None, stored_published, stored_downloaded, ref.publication_id,
        )
        connection = _Connection(fetchone_results=(None, stored))

        result = PostgreSQLNextDayCurrentArtifactRegistrar(connection).record(
            NextDayCurrentArtifactReceipt(
                ref.report_date, ref.url, ref.filename, ref.publication_id,
                ref.listing_timestamp, first_seen + timedelta(days=1), payload,
            )
        )

        self.assertEqual(
            result,
            NextDayCurrentArtifactResult(digest, len(payload), first_seen, True),
        )
        self.assertIs(result.downloaded_at.tzinfo, UTC)

    def test_replays_by_digest_without_replacing_first_seen_download_clock(self) -> None:
        payload = b"retained-current-zip"
        ref = _reference(date(2026, 8, 29), size=len(payload))
        published = ref.listing_timestamp
        first_seen = datetime(2026, 8, 31, tzinfo=UTC)
        receipt = NextDayCurrentArtifactReceipt(
            ref.report_date, ref.url, ref.filename, ref.publication_id,
            published, first_seen, payload,
        )
        digest = sha256(payload).hexdigest()
        stored = (
            "nextday_soc", ref.report_date, ref.url, ref.filename, len(payload), payload,
            None, published, first_seen, ref.publication_id,
        )
        connection = _Connection(fetchone_results=(None, stored))

        result = PostgreSQLNextDayCurrentArtifactRegistrar(connection).record(
            NextDayCurrentArtifactReceipt(
                ref.report_date, ref.url, ref.filename, ref.publication_id,
                published, first_seen + timedelta(days=1), payload,
            )
        )

        self.assertEqual(
            result,
            NextDayCurrentArtifactResult(digest, len(payload), first_seen, True),
        )
        self.assertEqual((connection.commits, connection.rollbacks), (1, 0))
        self.assertEqual(connection.executions[1][1], (digest,))
        self.assertEqual(receipt.raw_bytes, payload)

    def test_changed_publication_metadata_rolls_back_and_fails_closed(self) -> None:
        payload = b"retained-current-zip"
        ref = _reference(date(2026, 8, 29), size=len(payload))
        receipt = NextDayCurrentArtifactReceipt(
            ref.report_date, ref.url, ref.filename, ref.publication_id,
            ref.listing_timestamp, datetime(2026, 8, 31, tzinfo=UTC), payload,
        )
        stored = (
            "nextday_soc", ref.report_date, ref.url, ref.filename, len(payload), payload,
            None, ref.listing_timestamp + timedelta(seconds=1),
            receipt.downloaded_at, ref.publication_id,
        )
        connection = _Connection(fetchone_results=(None, stored))

        with self.assertRaises(NextDayCurrentArtifactConflictError):
            PostgreSQLNextDayCurrentArtifactRegistrar(connection).record(receipt)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))


class CurrentNextDayCollectionTests(unittest.TestCase):
    def test_limit_one_selects_newest_incomplete_report_date(self) -> None:
        dates = (date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 29))
        initial_refs = tuple(
            _reference(item, str(index + 41), size=1)
            for index, item in enumerate(dates)
        )
        payloads = {ref.url: _zip(ref.filename) for ref in initial_refs}
        refs = tuple(
            _reference(ref.report_date, ref.publication_id, size=len(payloads[ref.url]))
            for ref in initial_refs
        )
        payloads = {ref.url: _zip(ref.filename) for ref in refs}
        completed = {date(2026, 8, 28)}
        registration_calls = []

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                return _resource(
                    url,
                    ("<pre>" + "".join(
                        _index_row(ref.filename, ref.size_bytes,
                                   "Saturday, August 29, 2026 04:10 AM")
                        for ref in refs
                    ) + "</pre>").encode(),
                )
            return _resource(url, payloads[url])

        result = run_nextday_current_cycle(
            "postgresql://private", (_asset(),), limit=1,
            connect=lambda _url, *, connect_timeout: _Connection(),
            fetch=fetch,
            clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
            registrar_factory=lambda connection: _Registrar(
                connection, completed, registration_calls
            ),
            parse_csv=lambda _payload, **kwargs: (_observation(
                kwargs["source_artifact_id"],
                datetime(2026, 8, 29, 18, 5, tzinfo=UTC),
                ingestion_version=kwargs["ingestion_version"],
                correction_version=kwargs["correction_version"],
                downloaded_at=kwargs["downloaded_at"],
            ),),
            soc_ingestor_factory=lambda _connection: _ProjectionIngestor(
                NextDaySocIngestionResult(1, 1, 0, 1, 1, 0, 0, 1), []
            ),
            fcas_ingestor_factory=lambda _connection: _ProjectionIngestor(
                NextDayFcasIngestionResult(1, 1, 0, 1, 1, 0, 1), []
            ),
        )

        self.assertEqual(
            [receipt.report_date for receipt in registration_calls],
            [date(2026, 8, 29)],
        )
        self.assertEqual(
            (result.attempted, result.completed, result.failed, result.remaining),
            (1, 1, 0, 1),
        )

    def test_omitted_limit_selects_oldest_plus_newest_incomplete(self) -> None:
        dates = (date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 29))
        refs = tuple(
            _reference(item, str(index + 41), size=1)
            for index, item in enumerate(dates)
        )
        completed = {date(2026, 8, 28)}
        registration_calls = []
        downloaded_at = datetime(2026, 8, 31, tzinfo=UTC)

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                html = "<pre>" + "".join(
                    _index_row(
                        ref.filename,
                        ref.size_bytes,
                        "Saturday, August 29, 2026 04:10 AM",
                    )
                    for ref in refs
                ) + "</pre>"
                return _resource(url, html.encode())
            return _resource(url, b"x")

        def extract(reference, raw):
            return NextDayCurrentArtifact(
                reference,
                reference.filename.removesuffix(".zip") + ".CSV",
                b"row",
                "row",
                sha256(raw).hexdigest(),
                raw,
            )

        def parse_csv(_payload, **kwargs):
            return (_observation(
                kwargs["source_artifact_id"],
                datetime(2026, 8, 29, 18, 5, tzinfo=UTC),
                ingestion_version=kwargs["ingestion_version"],
                correction_version=kwargs["correction_version"],
                downloaded_at=kwargs["downloaded_at"],
            ),)

        result = run_nextday_current_cycle(
            "postgresql://private",
            (_asset(),),
            connect=lambda _url, *, connect_timeout: _Connection(),
            fetch=fetch,
            extract=extract,
            clock=lambda: downloaded_at,
            registrar_factory=lambda connection: _Registrar(
                connection, completed, registration_calls
            ),
            parse_csv=parse_csv,
            soc_ingestor_factory=lambda _connection: _ProjectionIngestor(
                NextDaySocIngestionResult(1, 1, 0, 1, 1, 0, 0, 1), []
            ),
            fcas_ingestor_factory=lambda _connection: _ProjectionIngestor(
                NextDayFcasIngestionResult(1, 1, 0, 1, 1, 0, 1), []
            ),
        )

        self.assertEqual(
            [receipt.report_date for receipt in registration_calls],
            [date(2026, 8, 27), date(2026, 8, 29)],
        )
        self.assertEqual((result.attempted, result.completed, result.remaining), (2, 2, 0))

    def test_limit_two_is_oldest_plus_newest_and_runs_both_projections(self) -> None:
        dates = (date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 29))
        refs = tuple(
            _reference(item, str(index + 41), size=1)
            for index, item in enumerate(dates)
        )
        payloads = {ref.url: _zip(ref.filename) for ref in refs}
        for ref in refs:
            refs = tuple(
                _reference(item.report_date, item.publication_id, size=len(payloads[item.url]))
                if item.url in payloads else item for item in refs
            )
        payloads = {ref.url: _zip(ref.filename) for ref in refs}
        completed: set[date] = set()
        registration_calls = []
        soc_calls = []
        fcas_calls = []
        registrars = []
        soc_result = NextDaySocIngestionResult(1, 1, 0, 1, 1, 0, 0, 1)
        fcas_result = NextDayFcasIngestionResult(1, 1, 0, 1, 1, 0, 1)

        def connect(_url, *, connect_timeout):
            self.assertEqual(connect_timeout, 10)
            return _Connection()

        def registrar_factory(connection):
            registrar = _Registrar(connection, completed, registration_calls)
            registrars.append(registrar)
            return registrar

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                html = "<pre>" + "".join(
                    _index_row(ref.filename, ref.size_bytes,
                               "Saturday, August 29, 2026 04:10 AM")
                    for ref in refs
                ) + "</pre>"
                return _resource(url, html.encode())
            return _resource(url, payloads[url])

        def parse_csv(payload, *, duids, source_artifact_id, downloaded_at,
                      ingestion_version, correction_version):
            self.assertEqual(duids, frozenset({"ADPBA1"}))
            return (_observation(
                source_artifact_id,
                datetime(2026, 8, 29, 18, 5, tzinfo=UTC),
                ingestion_version=ingestion_version,
                correction_version=correction_version,
                downloaded_at=downloaded_at,
            ),)

        result = run_nextday_current_cycle(
            "postgresql://private", (_asset(),), limit=2,
            ingestion_version=1, connect=connect, fetch=fetch,
            clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
            registrar_factory=registrar_factory,
            soc_ingestor_factory=lambda _connection: _ProjectionIngestor(
                soc_result, soc_calls
            ),
            fcas_ingestor_factory=lambda _connection: _ProjectionIngestor(
                fcas_result, fcas_calls
            ),
            parse_csv=parse_csv,
        )

        self.assertIsInstance(result, NextDayCurrentCollectionResult)
        self.assertEqual(
            [receipt.report_date for receipt in registration_calls],
            [date(2026, 8, 27), date(2026, 8, 29)],
        )
        self.assertEqual(len(soc_calls), 2)
        self.assertEqual(len(fcas_calls), 2)
        self.assertEqual(
            (result.attempted, result.completed, result.failed, result.remaining),
            (2, 2, 0, 1),
        )
        self.assertEqual(
            (result.min_completed_report_date, result.max_completed_report_date),
            (date(2026, 8, 27), date(2026, 8, 29)),
        )
        self.assertEqual(
            (result.soc.attempted, result.soc.completed, result.soc.failed,
             result.soc.source_rows, result.soc.raw_inserted,
             result.soc.raw_replayed, result.soc.effective_candidates,
             result.soc.effective_applied, result.soc.effective_replayed,
             result.soc.percentage_count),
            (2, 2, 0, 2, 2, 0, 2, 2, 0, 2),
        )
        self.assertEqual(
            (result.fcas.attempted, result.fcas.completed, result.fcas.failed,
             result.fcas.source_rows, result.fcas.raw_inserted,
             result.fcas.raw_replayed, result.fcas.effective_candidates,
             result.fcas.effective_applied, result.fcas.effective_replayed,
             result.fcas.reported_service_count),
            (2, 2, 0, 2, 2, 0, 2, 2, 0, 2),
        )

    def test_cycle_rejects_injected_artifact_with_wrong_declared_size(self) -> None:
        base_ref = _reference(date(2026, 8, 29), size=1)
        payload = _zip(base_ref.filename)
        ref = _reference(
            base_ref.report_date,
            base_ref.publication_id,
            size=len(payload) + 1,
        )

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                return _resource(
                    url,
                    ("<pre>" + _index_row(
                        ref.filename, ref.size_bytes,
                        "Saturday, August 29, 2026 04:10 AM",
                    ) + "</pre>").encode(),
                )
            return _resource(url, payload)

        def bad_extract(reference, raw):
            return NextDayCurrentArtifact(
                reference,
                reference.filename.removesuffix(".zip") + ".CSV",
                CSV_TEXT.encode(),
                CSV_TEXT,
                sha256(raw).hexdigest(),
                raw,
            )

        result = run_nextday_current_cycle(
            "postgresql://private", (_asset(),), limit=1,
            connect=lambda _url, *, connect_timeout: _Connection(),
            fetch=fetch,
            extract=bad_extract,
            registrar_factory=lambda connection: _Registrar(
                connection, set(), []
            ),
            parse_csv=lambda _payload, **kwargs: self.fail(
                "parser must not receive a size-mismatched artifact"
            ),
        )

        self.assertEqual((result.completed, result.failed, result.remaining), (0, 1, 1))
        self.assertEqual(result.failures[0].location, "fetch")

    def test_registrar_cannot_supply_a_future_canonical_download_clock(self) -> None:
        ref = _reference(date(2026, 8, 29), size=1)
        payload = _zip(ref.filename)
        ref = _reference(ref.report_date, ref.publication_id, size=len(payload))

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                return _resource(
                    url,
                    ("<pre>" + _index_row(
                        ref.filename, ref.size_bytes,
                        "Saturday, August 29, 2026 04:10 AM",
                    ) + "</pre>").encode(),
                )
            return _resource(url, payload)

        class FutureRegistrar:
            def __init__(self, connection) -> None:
                pass

            def read_completed_report_dates(self, references):
                return frozenset()

            def record(self, receipt):
                return NextDayCurrentArtifactResult(
                    sha256(receipt.raw_bytes).hexdigest(),
                    len(receipt.raw_bytes),
                    receipt.downloaded_at + timedelta(days=1),
                    False,
                )

        result = run_nextday_current_cycle(
            "postgresql://private", (_asset(),), limit=1,
            connect=lambda _url, *, connect_timeout: _Connection(),
            fetch=fetch,
            clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
            registrar_factory=FutureRegistrar,
            parse_csv=lambda _payload, **kwargs: self.fail(
                "parser must not receive a future canonical download clock"
            ),
        )

        self.assertEqual((result.completed, result.failed, result.remaining), (0, 1, 1))
        self.assertEqual(result.failures[0].location, "registration")

    def test_one_failed_artifact_does_not_block_the_other(self) -> None:
        # The fake registrar fails only the oldest selected date; newest must still run.
        refs = tuple(
            _reference(item, str(index + 1), size=1)
            for index, item in enumerate((date(2026, 8, 27), date(2026, 8, 29)))
        )
        payloads = {ref.url: _zip(ref.filename) for ref in refs}
        refs = tuple(
            _reference(ref.report_date, ref.publication_id, size=len(payloads[ref.url]))
            for ref in refs
        )
        completed: set[date] = set()
        registration_calls = []

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                return _resource(
                    url,
                    ("<pre>" + "".join(
                        _index_row(ref.filename, ref.size_bytes,
                                   "Saturday, August 29, 2026 04:10 AM")
                        for ref in refs
                    ) + "</pre>").encode(),
                )
            return _resource(url, payloads[url])

        registrar_factory = lambda connection: _Registrar(
            connection, completed, registration_calls, failures={date(2026, 8, 27)}
        )
        result = run_nextday_current_cycle(
                "postgresql://private", (_asset(),), limit=2,
                connect=lambda _url, *, connect_timeout: _Connection(), fetch=fetch,
                clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
                parse_csv=lambda _payload, **kwargs: (_observation(
                    kwargs["source_artifact_id"],
                    datetime(2026, 8, 29, 18, 5, tzinfo=UTC),
                    ingestion_version=kwargs["ingestion_version"],
                    correction_version=kwargs["correction_version"],
                    downloaded_at=kwargs["downloaded_at"],
                ),),
                soc_ingestor_factory=lambda _connection: _ProjectionIngestor(
                    NextDaySocIngestionResult(1, 1, 0, 1, 1, 0, 0, 1), []
                ),
                fcas_ingestor_factory=lambda _connection: _ProjectionIngestor(
                    NextDayFcasIngestionResult(1, 1, 0, 1, 1, 0, 1), []
                ),
                registrar_factory=registrar_factory,
            )

        self.assertEqual([item.report_date for item in registration_calls], [
            date(2026, 8, 27), date(2026, 8, 29)
        ])
        self.assertEqual((result.completed, result.failed), (1, 1))
        self.assertEqual((result.soc.attempted, result.soc.completed,
                          result.soc.failed), (1, 1, 0))
        self.assertEqual((result.fcas.attempted, result.fcas.completed,
                          result.fcas.failed), (1, 1, 0))
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(result.failures[0].location, "registration")
        self.assertNotIn("private", repr(result.failures[0]))
        self.assertNotIn("deliberately hidden", repr(result.failures[0]))

    def test_partial_projection_is_incomplete_and_retried_next_cycle(self) -> None:
        ref = _reference(date(2026, 8, 29), size=1)
        payload = _zip(ref.filename)
        ref = _reference(ref.report_date, ref.publication_id, size=len(payload))
        payloads = {ref.url: payload}
        state = {"soc": set(), "fcas": set()}
        registration_calls = []
        soc_calls = []
        fcas_calls = []

        def fetch(url, *, max_bytes):
            if url == NEXTDAY_CURRENT_INDEX_URL:
                return _resource(
                    url,
                    ("<pre>" + _index_row(
                        ref.filename, ref.size_bytes,
                        "Saturday, August 29, 2026 04:10 AM",
                    ) + "</pre>").encode(),
                )
            return _resource(url, payloads[url])

        class RetryRegistrar:
            def __init__(self, connection) -> None:
                pass

            def read_completed_report_dates(self, references):
                return frozenset(
                    {ref.report_date}
                    if state["soc"] & state["fcas"]
                    else set()
                )

            def record(self, receipt):
                registration_calls.append(receipt)
                return NextDayCurrentArtifactResult(
                    sha256(receipt.raw_bytes).hexdigest(),
                    len(receipt.raw_bytes),
                    registration_calls[0].downloaded_at,
                    len(registration_calls) > 1,
                )

        def parse_csv(_payload, **kwargs):
            return (_observation(
                kwargs["source_artifact_id"],
                datetime(2026, 8, 29, 18, 5, tzinfo=UTC),
                ingestion_version=kwargs["ingestion_version"],
                correction_version=kwargs["correction_version"],
                downloaded_at=kwargs["downloaded_at"],
            ),)

        def soc_factory(_connection):
            class SocRetry:
                def ingest(self, observations, assets):
                    observations = tuple(observations)
                    soc_calls.append(observations)
                    state["soc"].add(ref.report_date)
                    return NextDaySocIngestionResult(1, 1, 0, 1, 1, 0, 0, 1)

            return SocRetry()

        def fcas_factory(_connection):
            class FcasRetry:
                def ingest(self, observations):
                    observations = tuple(observations)
                    fcas_calls.append(observations)
                    if len(fcas_calls) == 1:
                        raise RuntimeError("deliberately hidden")
                    state["fcas"].add(ref.report_date)
                    return NextDayFcasIngestionResult(1, 1, 0, 1, 1, 0, 1)

            return FcasRetry()

        common: dict[str, Any] = dict(
            database_url="postgresql://private",
            assets=(_asset(),),
            limit=1,
            connect=lambda _url, *, connect_timeout: _Connection(),
            fetch=fetch,
            clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
            registrar_factory=RetryRegistrar,
            soc_ingestor_factory=soc_factory,
            fcas_ingestor_factory=fcas_factory,
            parse_csv=parse_csv,
        )
        first = run_nextday_current_cycle(**common)
        second = run_nextday_current_cycle(**common)

        self.assertEqual((first.completed, first.failed, first.remaining), (0, 1, 1))
        self.assertEqual((first.soc.attempted, first.soc.completed,
                          first.soc.failed), (1, 1, 0))
        self.assertEqual((first.fcas.attempted, first.fcas.completed,
                          first.fcas.failed), (1, 0, 1))
        self.assertEqual((second.completed, second.failed, second.remaining), (1, 0, 0))
        self.assertEqual((second.soc.attempted, second.soc.completed,
                          second.soc.failed), (1, 1, 0))
        self.assertEqual((second.fcas.attempted, second.fcas.completed,
                          second.fcas.failed), (1, 1, 0))
        self.assertEqual((second.soc.raw_inserted, second.fcas.raw_inserted), (1, 1))
        self.assertEqual(len(registration_calls), 2)
        self.assertEqual(len(soc_calls), 2)
        self.assertEqual(len(fcas_calls), 2)

    def test_rejects_invalid_asset_collection_and_limit(self) -> None:
        for bad_assets, bad_limit in (
            ((), 1),
            ((_asset(), _asset()), 1),
            ((_asset(),), 0),
            ((_asset(),), 3),
            ((_asset(),), True),
        ):
            with self.subTest(bad_assets=bad_assets, bad_limit=bad_limit):
                with self.assertRaises(ValueError):
                    run_nextday_current_cycle(
                        "postgresql://private", bad_assets, limit=bad_limit,
                        connect=lambda *_args, **_kwargs: object(),
                    )


if __name__ == "__main__":
    unittest.main()
