"""Strict parser for authoritative Next Day Dispatch UnitSolution SOC."""

from __future__ import annotations

import csv
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from io import StringIO
from math import isfinite
from types import MappingProxyType
import re
from typing import Literal

_NEM_TIMEZONE = timezone(timedelta(hours=10))
_METADATA_PREFIX = ("C", "NEMP.WORLD", "NEXT_DAY_DISPATCH", "AEMO", "PUBLIC")
_TABLE_PREFIX = ("DISPATCH", "UNIT_SOLUTION")
_ACCEPTED_TABLE_VERSIONS = frozenset(("5", "6"))
_REQUIRED_COLUMNS = frozenset(
    (
        "SETTLEMENTDATE",
        "RUNNO",
        "DUID",
        "DISPATCHINTERVAL",
        "INTERVENTION",
        "LASTCHANGED",
        "INITIAL_ENERGY_STORAGE",
    )
)
_DUID_RE = re.compile(r"^[A-Z0-9_]{1,32}$")
_ARTIFACT_RE = re.compile(r"^[0-9a-f]{64}$")
_DISPATCH_INTERVAL_RE = re.compile(r"^(\d{8})(\d{3})$")
_MAX_PHYSICAL_CSV_RECORDS = 1_500_000

# These are the only FCAS service identities retained by this source path.  The
# UnitSolution reports use the corresponding AEMO column names below.
FCAS_SERVICES = (
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
)
FCAS_CLEARANCE_EPSILON_MW = 0.000001

# UnitSolution flags observed in the retained v5/v6 source are the bounded
# integer status codes 0 through 4.  1 and 3 are enabled, 3 is trapped, and 4
# is stranded.  Rejecting every other value prevents an unknown source code
# from being mistaken for a valid FCAS state.
_SUPPORTED_FCAS_STATUS_FLAGS = frozenset((0, 1, 2, 3, 4))
_FCAS_COLUMNS = {
    "raise_1s": ("RAISE1SEC", "RAISE1SECFLAGS", "RAISE1SECACTUALAVAILABILITY"),
    "lower_1s": ("LOWER1SEC", "LOWER1SECFLAGS", "LOWER1SECACTUALAVAILABILITY"),
    "raise_6s": ("RAISE6SEC", "RAISE6SECFLAGS", "RAISE6SECACTUALAVAILABILITY"),
    "lower_6s": ("LOWER6SEC", "LOWER6SECFLAGS", "LOWER6SECACTUALAVAILABILITY"),
    "raise_60s": (
        "RAISE60SEC",
        "RAISE60SECFLAGS",
        "RAISE60SECACTUALAVAILABILITY",
    ),
    "lower_60s": (
        "LOWER60SEC",
        "LOWER60SECFLAGS",
        "LOWER60SECACTUALAVAILABILITY",
    ),
    "raise_5m": ("RAISE5MIN", "RAISE5MINFLAGS", "RAISE5MINACTUALAVAILABILITY"),
    "lower_5m": ("LOWER5MIN", "LOWER5MINFLAGS", "LOWER5MINACTUALAVAILABILITY"),
    "raise_reg": ("RAISEREG", "RAISEREGFLAGS", "RAISEREGACTUALAVAILABILITY"),
    "lower_reg": ("LOWERREG", "LOWERREGFLAGS", "LOWERREGACTUALAVAILABILITY"),
}


class NextDaySocParseError(ValueError):
    """Raised when Next Day SOC source data cannot be normalized safely."""


def _timestamp(value: str, *, field: str, aligned: bool) -> datetime:
    normalized = value.strip().replace("/", "-").replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(normalized)
    except (AttributeError, TypeError, ValueError) as exc:
        raise NextDaySocParseError(f"invalid {field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=_NEM_TIMEZONE)
    result = parsed.astimezone(timezone.utc)
    if aligned and (result.minute % 5 or result.second or result.microsecond):
        raise NextDaySocParseError(f"invalid {field} alignment")
    return result


def _report_timestamp(row: list[str]) -> datetime:
    if len(row) != 10 or tuple(row[:5]) != _METADATA_PREFIX:
        raise NextDaySocParseError("invalid Next Day metadata")
    return _timestamp(f"{row[5]} {row[6]}", field="report timestamp", aligned=False)


def _version(value: str, *, field: str, maximum: int) -> int:
    if not value or not value.isascii() or not value.isdecimal():
        raise NextDaySocParseError(f"invalid {field}")
    parsed = int(value)
    if parsed > maximum:
        raise NextDaySocParseError(f"invalid {field}")
    return parsed


def _soc_mwh(value: str) -> float | None:
    if value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise NextDaySocParseError("invalid INITIAL_ENERGY_STORAGE") from exc
    if not isfinite(parsed) or parsed < 0:
        raise NextDaySocParseError("invalid INITIAL_ENERGY_STORAGE")
    return parsed


def _fcas_mw(value: str | None, field: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise NextDaySocParseError(f"invalid {field}") from exc
    if not isfinite(parsed) or parsed < 0:
        raise NextDaySocParseError(f"invalid {field}")
    return parsed


def _fcas_status(value: str | None, field: str) -> int | None:
    if value is None or value == "":
        return None
    if not value.isascii() or not value.isdecimal():
        raise NextDaySocParseError(f"invalid {field}")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise NextDaySocParseError(f"invalid {field}") from exc
    if parsed not in _SUPPORTED_FCAS_STATUS_FLAGS:
        raise NextDaySocParseError(f"unsupported {field}")
    return parsed


def _fcas_service(
    values: Mapping[str, str], service: str
) -> NextDayFcasServiceObservation:
    target_column, status_column, actual_column = _FCAS_COLUMNS[service]
    return NextDayFcasServiceObservation(
        target_mw=_fcas_mw(values.get(target_column), target_column),
        enablement_status=_fcas_status(values.get(status_column), status_column),
        actual_availability_mw=_fcas_mw(values.get(actual_column), actual_column),
    )


def _dispatch_interval(value: str, interval_start: datetime) -> str:
    match = _DISPATCH_INTERVAL_RE.fullmatch(value)
    if match is None:
        raise NextDaySocParseError("invalid DISPATCHINTERVAL")
    period = int(match.group(2))
    if not 1 <= period <= 288:
        raise NextDaySocParseError("invalid DISPATCHINTERVAL")
    try:
        market_date = datetime.strptime(match.group(1), "%Y%m%d").replace(
            tzinfo=_NEM_TIMEZONE
        )
    except ValueError as exc:
        raise NextDaySocParseError("invalid DISPATCHINTERVAL") from exc
    expected = (market_date + timedelta(hours=4, minutes=period * 5)).astimezone(
        timezone.utc
    )
    if expected != interval_start:
        raise NextDaySocParseError("DISPATCHINTERVAL does not match SETTLEMENTDATE")
    return value


def _validated_downloaded_at(value: datetime, report_timestamp: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise NextDaySocParseError("invalid downloaded_at")
    result = value.astimezone(timezone.utc)
    if result < report_timestamp:
        raise NextDaySocParseError("downloaded_at precedes report timestamp")
    return result


@dataclass(frozen=True, slots=True)
class NextDayFcasServiceObservation:
    """Raw FCAS evidence for one canonical UnitSolution service."""

    target_mw: float | None = None
    enablement_status: int | None = None
    actual_availability_mw: float | None = None

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.target_mw, "target_mw"),
            (self.actual_availability_mw, "actual_availability_mw"),
        ):
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"invalid FCAS {field_name}")
            try:
                normalized = float(value)
            except (OverflowError, ValueError) as exc:
                raise ValueError(f"invalid FCAS {field_name}") from exc
            if not isfinite(normalized) or normalized < 0:
                raise ValueError(f"invalid FCAS {field_name}")
        status = self.enablement_status
        if status is not None and (
            isinstance(status, bool)
            or type(status) is not int
            or status not in _SUPPORTED_FCAS_STATUS_FLAGS
        ):
            raise ValueError("invalid FCAS enablement_status")

    @property
    def enabled(self) -> bool:
        return self.enablement_status in {1, 3}

    @property
    def trapped(self) -> bool:
        return self.enablement_status == 3

    @property
    def stranded(self) -> bool:
        return self.enablement_status == 4

    @property
    def cleared(self) -> bool:
        return (
            self.target_mw is not None
            and self.target_mw > FCAS_CLEARANCE_EPSILON_MW
        )

    @property
    def participating(self) -> bool:
        return self.enabled and self.cleared

    @property
    def response_evidence(self) -> None:
        """UnitSolution has no measured physical response evidence."""

        return None

    @property
    def reported(self) -> bool:
        return any(
            value is not None
            for value in (
                self.target_mw,
                self.enablement_status,
                self.actual_availability_mw,
            )
        )


def _empty_fcas_services() -> dict[str, NextDayFcasServiceObservation]:
    return {
        service: NextDayFcasServiceObservation()
        for service in FCAS_SERVICES
    }


@dataclass(frozen=True, slots=True)
class NextDayFcasObservation:
    """The fixed ten-service FCAS map attached to one UnitSolution row."""

    services: Mapping[str, NextDayFcasServiceObservation] = field(
        default_factory=lambda: MappingProxyType(_empty_fcas_services())
    )

    def __post_init__(self) -> None:
        if not isinstance(self.services, Mapping):
            raise ValueError("invalid FCAS service map")
        if set(self.services) != set(FCAS_SERVICES) or any(
            not isinstance(self.services.get(service), NextDayFcasServiceObservation)
            for service in FCAS_SERVICES
        ):
            raise ValueError("invalid FCAS service map")
        normalized = {
            service: self.services[service]
            for service in FCAS_SERVICES
        }
        object.__setattr__(self, "services", MappingProxyType(normalized))

    @classmethod
    def empty(cls) -> NextDayFcasObservation:
        return cls(_empty_fcas_services())

    def __getitem__(self, service: str) -> NextDayFcasServiceObservation:
        return self.services[service]

    def __iter__(self) -> Iterator[str]:
        return iter(self.services)

    def __len__(self) -> int:
        return len(self.services)

    @property
    def reported_service_count(self) -> int:
        return sum(service.reported for service in self.services.values())

    def as_dict(self) -> dict[str, dict[str, float | int | None]]:
        return {
            service: {
                "target_mw": evidence.target_mw,
                "enablement_status": evidence.enablement_status,
                "actual_availability_mw": evidence.actual_availability_mw,
            }
            for service, evidence in self.services.items()
        }

    @property
    def raise_1s(self) -> NextDayFcasServiceObservation:
        return self.services["raise_1s"]

    @property
    def lower_1s(self) -> NextDayFcasServiceObservation:
        return self.services["lower_1s"]

    @property
    def raise_6s(self) -> NextDayFcasServiceObservation:
        return self.services["raise_6s"]

    @property
    def lower_6s(self) -> NextDayFcasServiceObservation:
        return self.services["lower_6s"]

    @property
    def raise_60s(self) -> NextDayFcasServiceObservation:
        return self.services["raise_60s"]

    @property
    def lower_60s(self) -> NextDayFcasServiceObservation:
        return self.services["lower_60s"]

    @property
    def raise_5m(self) -> NextDayFcasServiceObservation:
        return self.services["raise_5m"]

    @property
    def lower_5m(self) -> NextDayFcasServiceObservation:
        return self.services["lower_5m"]

    @property
    def raise_reg(self) -> NextDayFcasServiceObservation:
        return self.services["raise_reg"]

    @property
    def lower_reg(self) -> NextDayFcasServiceObservation:
        return self.services["lower_reg"]


@dataclass(frozen=True, slots=True)
class NextDaySocObservation:
    """One authoritative per-DUID SOC and FCAS observation."""

    duid: str
    interval_start: datetime
    soc_mwh: float | None
    intervention: int
    run_number: int
    dispatch_interval: str
    last_changed: datetime
    source_artifact_id: str
    report_timestamp: datetime
    downloaded_at: datetime
    ingestion_version: int
    correction_version: int
    fcas: NextDayFcasObservation = field(
        default_factory=NextDayFcasObservation.empty
    )

    def __post_init__(self) -> None:
        if not isinstance(self.fcas, NextDayFcasObservation):
            raise ValueError("invalid Next Day FCAS observation")

    @property
    def publication_latency_seconds(self) -> int:
        return int((self.report_timestamp - self.interval_start).total_seconds())

    @property
    def publication_status(self) -> Literal["next_day"]:
        return "next_day"


def parse_nextday_unit_solution_soc(
    payload: str,
    *,
    duids: frozenset[str],
    source_artifact_id: str,
    downloaded_at: datetime,
    ingestion_version: int,
    correction_version: int = 0,
) -> tuple[NextDaySocObservation, ...]:
    """Parse public Next Day UnitSolution v5/v6 rows for reviewed DUIDs."""

    if type(payload) is not str or not payload:
        raise NextDaySocParseError("invalid payload")
    if type(duids) is not frozenset or not duids or any(
        type(duid) is not str or _DUID_RE.fullmatch(duid) is None for duid in duids
    ):
        raise NextDaySocParseError("invalid reviewed DUID set")
    if type(source_artifact_id) is not str or _ARTIFACT_RE.fullmatch(source_artifact_id) is None:
        raise NextDaySocParseError("invalid source artifact identity")
    ingestion = _version(str(ingestion_version), field="ingestion_version", maximum=2**63 - 1)
    correction = _version(str(correction_version), field="correction_version", maximum=2**63 - 1)

    reader = csv.reader(StringIO(payload))
    try:
        metadata = next(reader)
    except (StopIteration, csv.Error, TypeError, UnicodeError) as exc:
        raise NextDaySocParseError("incomplete Next Day report") from exc
    report_timestamp = _report_timestamp(metadata)
    downloaded = _validated_downloaded_at(downloaded_at, report_timestamp)

    header: list[str] | None = None
    table_identity: tuple[str, ...] | None = None
    observations: list[NextDaySocObservation] = []
    seen: set[tuple[str, datetime, int, int]] = set()
    report_record_count = 1
    expected_count: int | None = None
    try:
        for row in reader:
            report_record_count += 1
            if report_record_count > _MAX_PHYSICAL_CSV_RECORDS:
                raise NextDaySocParseError(
                    "physical CSV record limit exceeded"
                )
            if expected_count is not None:
                raise NextDaySocParseError("data follows report trailer")
            if row[:2] == ["C", "END OF REPORT"]:
                if len(row) != 3:
                    raise NextDaySocParseError("invalid report trailer")
                expected_count = _version(
                    row[2], field="report row count", maximum=10_000_000
                )
                continue

            if row[:3] == ["I", *_TABLE_PREFIX]:
                if len(row) < 4 or row[3] not in _ACCEPTED_TABLE_VERSIONS:
                    raise NextDaySocParseError("unsupported UnitSolution version")
                if header is not None or len(row) != len(set(row)):
                    raise NextDaySocParseError("invalid UnitSolution header")
                if not _REQUIRED_COLUMNS.issubset(row[4:]):
                    raise NextDaySocParseError("missing UnitSolution columns")
                header = row
                table_identity = tuple(row[1:4])
                continue
            if row[:3] != ["D", *_TABLE_PREFIX]:
                continue
            if (
                header is None
                or table_identity is None
                or tuple(row[1:4]) != table_identity
                or len(row) != len(header)
            ):
                raise NextDaySocParseError("malformed UnitSolution row")
            values = dict(zip(header[4:], row[4:]))
            duid = values["DUID"]
            if duid not in duids:
                continue
            interval_start = _timestamp(
                values["SETTLEMENTDATE"], field="SETTLEMENTDATE", aligned=True
            )
            run_number = _version(values["RUNNO"], field="RUNNO", maximum=999)
            intervention = _version(
                values["INTERVENTION"], field="INTERVENTION", maximum=1
            )
            dispatch_interval = _dispatch_interval(
                values["DISPATCHINTERVAL"], interval_start
            )
            last_changed = _timestamp(
                values["LASTCHANGED"], field="LASTCHANGED", aligned=False
            )
            if last_changed > report_timestamp or report_timestamp <= interval_start:
                raise NextDaySocParseError("invalid source timestamp ordering")
            key = (duid, interval_start, intervention, run_number)
            if key in seen:
                raise NextDaySocParseError("duplicate UnitSolution observation")
            seen.add(key)
            observations.append(
                NextDaySocObservation(
                    duid=duid,
                    interval_start=interval_start,
                    soc_mwh=_soc_mwh(values["INITIAL_ENERGY_STORAGE"]),
                    intervention=intervention,
                    run_number=run_number,
                    dispatch_interval=dispatch_interval,
                    last_changed=last_changed,
                    source_artifact_id=source_artifact_id,
                    report_timestamp=report_timestamp,
                    downloaded_at=downloaded,
                    ingestion_version=ingestion,
                    correction_version=correction,
                    fcas=NextDayFcasObservation(
                        {
                            service: _fcas_service(values, service)
                            for service in FCAS_SERVICES
                        }
                    ),
                )
            )
    except (csv.Error, TypeError, UnicodeError) as exc:
        raise NextDaySocParseError("invalid CSV") from exc
    if expected_count is None:
        raise NextDaySocParseError("missing report trailer")
    if expected_count != report_record_count:
        raise NextDaySocParseError("report row count mismatch")
    if header is None:
        raise NextDaySocParseError("missing UnitSolution v5/v6 table")
    return tuple(
        sorted(
            observations,
            key=lambda item: (
                item.duid,
                item.interval_start,
                item.intervention,
                item.run_number,
            ),
        )
    )


__all__ = [
    "FCAS_CLEARANCE_EPSILON_MW",
    "FCAS_SERVICES",
    "NextDayFcasObservation",
    "NextDayFcasServiceObservation",
    "NextDaySocObservation",
    "NextDaySocParseError",
    "parse_nextday_unit_solution_soc",
]
