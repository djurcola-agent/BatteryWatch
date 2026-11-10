"""Strict discovery and retention of daily Next Day Dispatch CURRENT files."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from html.parser import HTMLParser
from io import BytesIO
import re
import struct
from stat import S_IFREG
from typing import Final
import zlib
from zipfile import (
    ZIP_DEFLATED,
    ZIP_STORED,
    BadZipFile,
    LargeZipFile,
    ZipFile,
    ZipInfo,
)

from .nemweb_http import NemwebHttpResource, fetch_nemweb_resource


NEMWEB_ORIGIN: Final = "https://www.nemweb.com.au"
NEXTDAY_CURRENT_INDEX_URL: Final = (
    NEMWEB_ORIGIN + "/REPORTS/CURRENT/Next_Day_Dispatch/"
)
NEXTDAY_CURRENT_INDEX_MAX_BYTES: Final = 2 * 1024 * 1024
NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES: Final = 16 * 1024 * 1024
NEXTDAY_CURRENT_CSV_MAX_BYTES: Final = 128 * 1024 * 1024
NEXTDAY_CURRENT_COMPRESSED_MAX_BYTES: Final = 16 * 1024 * 1024
NEXTDAY_CURRENT_EXPANDED_MAX_BYTES: Final = 128 * 1024 * 1024
NEXTDAY_CURRENT_COMPRESSION_RATIO: Final = 100

# The directory listing emitted by NEMWeb uses this spelling.  The canonical
# URL stored in PostgreSQL uses REPORTS, as do the other source adapters.
_OBSERVED_CURRENT_PATH: Final = "/Reports/CURRENT/Next_Day_Dispatch/"
_CANONICAL_CURRENT_PATH: Final = "/REPORTS/CURRENT/Next_Day_Dispatch/"
_NEM_TIMEZONE: Final = timezone(timedelta(hours=10))
_UTC: Final = timezone.utc
_FILENAME_RE: Final = re.compile(
    r"PUBLIC_NEXT_DAY_DISPATCH_(?P<date>[0-9]{8})_(?P<publication>[0-9]{1,32})\.zip",
    re.ASCII,
)
_LISTING_PREFIX_RE: Final = re.compile(
    r"\s*(?P<timestamp>[A-Z][a-z]+, [A-Z][a-z]+ [0-9]{1,2}, [0-9]{4} "
    r"[0-9]{2}:[0-9]{2} [AP]M)\s+(?P<size>[0-9]+)\s*",
    re.ASCII,
)
_END_RECORD = struct.Struct("<4s4H2LH")
_LOCAL_RECORD = struct.Struct("<4s5H3L2H")

_FORMAT_ERRORS = (
    BadZipFile,
    EOFError,
    IndexError,
    LargeZipFile,
    NotImplementedError,
    OSError,
    OverflowError,
    RuntimeError,
    UnicodeError,
    ValueError,
    struct.error,
    zlib.error,
)


class NemwebNextDayCurrentError(ValueError):
    """Raised when a CURRENT Next Day listing or ZIP is not canonical."""


@dataclass(frozen=True, slots=True)
class NextDayCurrentArtifactRef:
    """Canonical identity and listing evidence for one report date."""

    report_date: date
    filename: str
    url: str
    publication_id: str
    size_bytes: int
    listing_timestamp: datetime


@dataclass(frozen=True, slots=True)
class NextDayCurrentArtifact:
    """One retained daily ZIP and its one decoded CSV member."""

    reference: NextDayCurrentArtifactRef
    csv_member_name: str
    csv_bytes: bytes
    csv_text: str
    zip_sha256: str
    raw_zip: bytes


def _listing_error() -> NemwebNextDayCurrentError:
    return NemwebNextDayCurrentError("invalid Next Day CURRENT listing")


def _zip_error() -> NemwebNextDayCurrentError:
    return NemwebNextDayCurrentError("invalid Next Day CURRENT ZIP")


class _ListingParser(HTMLParser):
    """Collect anchor context without treating HTML as a regular expression."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._outside_text: list[str] = []
        self._href: str | None = None
        self._prefix = ""
        self._anchor_text: list[str] = []
        self._anchor_has_markup = False
        self.rows: list[tuple[str, str, str, bool]] = []
        self.malformed = False

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        lowered = tag.lower()
        if self._href is not None:
            self._anchor_has_markup = True
            return
        if lowered == "br":
            self._outside_text.clear()
            return
        if lowered != "a":
            return
        hrefs = [
            value
            for name, value in attrs
            if name.lower() == "href"
        ]
        if len(hrefs) != 1 or hrefs[0] is None:
            self.malformed = True
            href = ""
        else:
            href = hrefs[0]
        self._href = href
        self._prefix = "".join(self._outside_text)
        self._anchor_text = []
        self._anchor_has_markup = False

    def handle_data(self, data: str) -> None:
        if self._href is None:
            self._outside_text.append(data)
        else:
            self._anchor_text.append(data)

    def handle_comment(self, data: str) -> None:
        del data
        if self._href is not None:
            self._anchor_has_markup = True

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered == "a":
            if self._href is None:
                self.malformed = True
                return
            self.rows.append(
                (
                    self._href,
                    "".join(self._anchor_text),
                    self._prefix,
                    self._anchor_has_markup,
                )
            )
            self._href = None
            self._prefix = ""
            self._anchor_text = []
            self._anchor_has_markup = False
            self._outside_text.clear()
        elif self._href is not None:
            self._anchor_has_markup = True

    def close(self) -> None:
        super().close()
        if self._href is not None:
            self.malformed = True


def _listing_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%A, %B %d, %Y %I:%M %p")
        weekday = value.split(",", 1)[0]
        if parsed.strftime("%A") != weekday:
            raise _listing_error()
        return parsed.replace(tzinfo=_NEM_TIMEZONE).astimezone(_UTC)
    except (OverflowError, ValueError):
        raise _listing_error() from None


def _filename_identity(
    filename: str,
) -> tuple[date, str]:
    if type(filename) is not str:
        raise _listing_error()
    match = _FILENAME_RE.fullmatch(filename)
    if match is None:
        raise _listing_error()
    try:
        report_date = datetime.strptime(
            match.group("date"), "%Y%m%d"
        ).date()
    except (OverflowError, ValueError):
        raise _listing_error() from None
    return report_date, match.group("publication")


def _canonical_href(href: str, filename: str) -> bool:
    return href in {
        filename,
        "./" + filename,
        _OBSERVED_CURRENT_PATH + filename,
        _CANONICAL_CURRENT_PATH + filename,
        NEXTDAY_CURRENT_INDEX_URL + filename,
    }


def _validate_reference(reference: object) -> NextDayCurrentArtifactRef:
    if type(reference) is not NextDayCurrentArtifactRef:
        raise _zip_error()
    report_date, publication_id = _filename_identity(reference.filename)
    if (
        type(reference.report_date) is not date
        or reference.report_date != report_date
        or type(reference.publication_id) is not str
        or reference.publication_id != publication_id
        or reference.url != NEXTDAY_CURRENT_INDEX_URL + reference.filename
        or isinstance(reference.size_bytes, bool)
        or type(reference.size_bytes) is not int
        or not 0 < reference.size_bytes <= NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES
    ):
        raise _zip_error()
    if (
        type(reference.listing_timestamp) is not datetime
        or reference.listing_timestamp.tzinfo is None
    ):
        raise _zip_error()
    try:
        listing_offset = reference.listing_timestamp.utcoffset()
        normalized_listing = reference.listing_timestamp.astimezone(_UTC)
    except (OverflowError, TypeError, ValueError):
        raise _zip_error() from None
    if listing_offset != timedelta(0) or normalized_listing != reference.listing_timestamp:
        raise _zip_error()
    return reference


def discover_nextday_current_artifacts(
    index_html: str,
    *,
    index_url: str = NEXTDAY_CURRENT_INDEX_URL,
) -> tuple[NextDayCurrentArtifactRef, ...]:
    """Parse and validate the authoritative CURRENT directory listing."""

    if index_url != NEXTDAY_CURRENT_INDEX_URL or type(index_html) is not str:
        raise _listing_error()
    try:
        encoded_size = len(index_html.encode("utf-8"))
    except UnicodeEncodeError:
        raise _listing_error() from None
    if not 0 < encoded_size <= NEXTDAY_CURRENT_INDEX_MAX_BYTES or "\x00" in index_html:
        raise _listing_error()

    parser = _ListingParser()
    try:
        parser.feed(index_html)
        parser.close()
    except (AssertionError, AttributeError, TypeError, ValueError):
        raise _listing_error() from None
    if parser.malformed:
        raise _listing_error()

    references: dict[date, NextDayCurrentArtifactRef] = {}
    for href, anchor_text, prefix, has_markup in parser.rows:
        candidate = (
            "PUBLIC_NEXT_DAY_DISPATCH_" in href
            or "PUBLIC_NEXT_DAY_DISPATCH_" in anchor_text
        )
        if not candidate:
            continue
        if has_markup or type(anchor_text) is not str:
            raise _listing_error()
        report_date, publication_id = _filename_identity(anchor_text)
        if not _canonical_href(href, anchor_text):
            raise _listing_error()
        prefix_match = _LISTING_PREFIX_RE.fullmatch(prefix)
        if prefix_match is None:
            raise _listing_error()
        try:
            size_bytes = int(prefix_match.group("size"))
        except (OverflowError, ValueError):
            raise _listing_error() from None
        if not 0 < size_bytes <= NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES:
            raise _listing_error()
        reference = NextDayCurrentArtifactRef(
            report_date=report_date,
            filename=anchor_text,
            url=NEXTDAY_CURRENT_INDEX_URL + anchor_text,
            publication_id=publication_id,
            size_bytes=size_bytes,
            listing_timestamp=_listing_timestamp(
                prefix_match.group("timestamp")
            ),
        )
        existing = references.get(report_date)
        if existing is not None:
            # AEMO can republish a report date with a later publication id
            # and appends the replacement entry after the original row in the
            # CURRENT listing (observed live for 2026-10-28 on 2026-10-29).
            # The last listing entry per report date is the authoritative
            # CURRENT artifact identity; a second entry without a later
            # publication id remains a fail-closed conflict.
            if publication_id <= existing.publication_id:
                raise NemwebNextDayCurrentError(
                    "conflicting Next Day CURRENT report date republished "
                    "without a later publication id"
                )
        references[report_date] = reference

    if not references:
        raise NemwebNextDayCurrentError("empty Next Day CURRENT listing")
    return tuple(references[item] for item in sorted(references))


def _validate_zip_end(
    zip_payload: bytes,
    archive: ZipFile,
    members: list[ZipInfo],
) -> None:
    """Require one ordinary ZIP and no trailing container bytes."""

    if len(zip_payload) < _END_RECORD.size:
        raise _zip_error()
    end_offset = len(zip_payload) - _END_RECORD.size
    fields = _END_RECORD.unpack_from(zip_payload, end_offset)
    signature, disk, central_disk, disk_entries, total_entries, central_size, central_offset, comment_size = fields
    if (
        signature != b"PK\x05\x06"
        or disk != 0
        or central_disk != 0
        or disk_entries != total_entries
        or total_entries != len(members)
        or comment_size != 0
        or central_offset != getattr(archive, "start_dir", -1)
        or central_offset + central_size != end_offset
    ):
        raise _zip_error()


def _validate_local_header(
    zip_payload: bytes,
    member: ZipInfo,
    expected_name: str,
    central_offset: int,
) -> None:
    offset = member.header_offset
    if type(offset) is not int or offset < 0 or offset + _LOCAL_RECORD.size > len(zip_payload):
        raise _zip_error()
    fields = _LOCAL_RECORD.unpack_from(zip_payload, offset)
    (
        signature,
        _version,
        flags,
        compression,
        _modified_time,
        _modified_date,
        _crc,
        _compressed,
        _expanded,
        name_size,
        extra_size,
    ) = fields
    if signature != b"PK\x03\x04":
        raise _zip_error()
    name_start = offset + _LOCAL_RECORD.size
    name_end = name_start + name_size
    extra_end = name_end + extra_size
    try:
        encoded_name = expected_name.encode("utf-8")
    except UnicodeEncodeError:
        raise _zip_error() from None
    if (
        name_size != len(encoded_name)
        or zip_payload[name_start:name_end] != encoded_name
        or extra_size != 0
        or flags & 0x1
        or compression not in (ZIP_STORED, ZIP_DEFLATED)
        or extra_end > len(zip_payload)
        or extra_end + member.compress_size > central_offset
    ):
        raise _zip_error()


def validate_nextday_current_reference(
    reference: NextDayCurrentArtifactRef,
) -> NextDayCurrentArtifactRef:
    """Validate and return one canonical CURRENT reference."""

    return _validate_reference(reference)


def extract_nextday_current_zip(
    reference: NextDayCurrentArtifactRef,
    zip_payload: bytes,
) -> NextDayCurrentArtifact:
    """Validate exactly one canonical CSV member and retain its evidence."""

    reference = _validate_reference(reference)
    if (
        type(zip_payload) is not bytes
        or not zip_payload
        or len(zip_payload) != reference.size_bytes
        or len(zip_payload) > NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES
    ):
        raise _zip_error()

    expected_member = reference.filename.removesuffix(".zip") + ".CSV"
    try:
        with ZipFile(BytesIO(zip_payload), mode="r", allowZip64=False) as archive:
            if archive.comment:
                raise _zip_error()
            members = archive.infolist()
            _validate_zip_end(zip_payload, archive, members)
            if len(members) != 1:
                raise _zip_error()
            member = members[0]
            if (
                type(member.filename) is not str
                or type(member.orig_filename) is not str
                or member.filename != expected_member
                or member.orig_filename != expected_member
                or member.is_dir()
                or (
                    member.create_system == 3
                    and ((member.external_attr >> 16) & 0o170000)
                    not in (0, S_IFREG)
                )
                or member.extra
                or member.comment
                or member.flag_bits & 0x1
                or member.compress_type not in (ZIP_STORED, ZIP_DEFLATED)
                or type(member.file_size) is not int
                or type(member.compress_size) is not int
                or not 0 < member.file_size <= NEXTDAY_CURRENT_CSV_MAX_BYTES
                or not 0 < member.compress_size <= NEXTDAY_CURRENT_COMPRESSED_MAX_BYTES
                or member.file_size > NEXTDAY_CURRENT_EXPANDED_MAX_BYTES
                or member.file_size
                > member.compress_size * NEXTDAY_CURRENT_COMPRESSION_RATIO
            ):
                raise _zip_error()
            _validate_local_header(
                zip_payload,
                member,
                expected_member,
                archive.start_dir,
            )
            with archive.open(member, mode="r") as stream:
                csv_bytes = stream.read(NEXTDAY_CURRENT_CSV_MAX_BYTES + 1)
                has_more_data = bool(stream.read(1))
            if (
                type(csv_bytes) is not bytes
                or has_more_data
                or len(csv_bytes) != member.file_size
                or not csv_bytes
            ):
                raise _zip_error()
            csv_text = csv_bytes.decode("utf-8-sig")
            if not csv_text or "\x00" in csv_text:
                raise _zip_error()
    except NemwebNextDayCurrentError:
        raise
    except _FORMAT_ERRORS as error:
        raise _zip_error() from error

    return NextDayCurrentArtifact(
        reference=reference,
        csv_member_name=expected_member,
        csv_bytes=csv_bytes,
        csv_text=csv_text,
        zip_sha256=sha256(zip_payload).hexdigest(),
        raw_zip=zip_payload,
    )


def _validated_resource(
    resource: object,
    expected_url: str,
) -> NemwebHttpResource:
    if not isinstance(resource, NemwebHttpResource):
        raise _zip_error()
    if (
        resource.requested_url != expected_url
        or resource.resolved_url != expected_url
        or type(resource.body) is not bytes
        or not resource.body
    ):
        raise _zip_error()
    return resource


def fetch_nextday_current_artifact(
    reference: NextDayCurrentArtifactRef,
    *,
    fetch: Callable[..., NemwebHttpResource] = fetch_nemweb_resource,
) -> NextDayCurrentArtifact:
    """Fetch one selected reference through the existing HTTP resource seam."""

    reference = _validate_reference(reference)
    try:
        resource = fetch(
            reference.url,
            max_bytes=NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES,
        )
    except NemwebNextDayCurrentError:
        raise
    except Exception:
        # Transport implementations own their public transport error.  Do not
        # turn it into a source URL-bearing message at this boundary.
        raise
    resource = _validated_resource(resource, reference.url)
    return extract_nextday_current_zip(reference, resource.body)


def fetch_nextday_current_index(
    *,
    fetch: Callable[..., NemwebHttpResource] = fetch_nemweb_resource,
) -> tuple[NextDayCurrentArtifactRef, ...]:
    """Fetch and parse the bounded CURRENT index."""

    try:
        resource = fetch(
            NEXTDAY_CURRENT_INDEX_URL,
            max_bytes=NEXTDAY_CURRENT_INDEX_MAX_BYTES,
        )
    except Exception:
        raise
    if not isinstance(resource, NemwebHttpResource):
        raise NemwebNextDayCurrentError("invalid Next Day CURRENT index response")
    if (
        resource.requested_url != NEXTDAY_CURRENT_INDEX_URL
        or resource.resolved_url != NEXTDAY_CURRENT_INDEX_URL
        or type(resource.body) is not bytes
        or not 0 < len(resource.body) <= NEXTDAY_CURRENT_INDEX_MAX_BYTES
    ):
        raise NemwebNextDayCurrentError("invalid Next Day CURRENT index response")
    try:
        index_html = resource.body.decode("utf-8")
    except UnicodeDecodeError:
        raise NemwebNextDayCurrentError(
            "invalid Next Day CURRENT index response"
        ) from None
    return discover_nextday_current_artifacts(
        index_html,
        index_url=NEXTDAY_CURRENT_INDEX_URL,
    )


__all__ = [
    "NEMWEB_ORIGIN",
    "NEXTDAY_CURRENT_ARTIFACT_MAX_BYTES",
    "NEXTDAY_CURRENT_COMPRESSION_RATIO",
    "NEXTDAY_CURRENT_COMPRESSED_MAX_BYTES",
    "NEXTDAY_CURRENT_CSV_MAX_BYTES",
    "NEXTDAY_CURRENT_EXPANDED_MAX_BYTES",
    "NEXTDAY_CURRENT_INDEX_MAX_BYTES",
    "NEXTDAY_CURRENT_INDEX_URL",
    "NemwebNextDayCurrentError",
    "NextDayCurrentArtifact",
    "NextDayCurrentArtifactRef",
    "discover_nextday_current_artifacts",
    "extract_nextday_current_zip",
    "fetch_nextday_current_artifact",
    "fetch_nextday_current_index",
    "validate_nextday_current_reference",
]
