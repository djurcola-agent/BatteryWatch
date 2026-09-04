"""Tests for authoritative Next Day UnitSolution SOC parsing."""

import csv
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any
import unittest

from batterywatch_api import nextday_soc as nextday_soc_module
from batterywatch_api.nextday_soc import (
    FCAS_SERVICES,
    NextDayFcasObservation,
    NextDaySocObservation,
    NextDaySocParseError,
    parse_nextday_unit_solution_soc,
)

UTC = timezone.utc
FIXTURE = Path(__file__).parent / "fixtures" / "historical" / "nextday-unit-solution-soc-20260829-reduced.csv"
V5_FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "historical"
    / "nextday-unit-solution-soc-v5-20250701-reduced.csv"
)


class NextDaySocParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = FIXTURE.read_text(encoding="utf-8")
        self.downloaded_at = datetime(2026, 8, 30, 0, 0, tzinfo=UTC)

    def parse(self, payload: str | None = None, *, duids: frozenset[str] = frozenset(("ADPBA1", "KEPBG1"))):
        return parse_nextday_unit_solution_soc(
            self.payload if payload is None else payload,
            duids=duids,
            source_artifact_id="d7a2abdd2947ed4b222166b9f60e3a8052838190027dd9ce03cb291ba2d29bc4",
            downloaded_at=self.downloaded_at,
            ingestion_version=8,
            correction_version=2,
        )

    def mutate_row(self, payload: str, **changes: str) -> str:
        rows = list(csv.reader(StringIO(payload)))
        header = rows[1]
        duid_index = header.index("DUID")
        row = next(
            row for row in rows[2:]
            if len(row) > duid_index and row[duid_index] == "ADPBA1"
        )
        for column, value in changes.items():
            row[header.index(column)] = value
        output = StringIO(newline="")
        csv.writer(output, lineterminator="\n").writerows(rows)
        return output.getvalue()

    def without_columns(self, payload: str, *columns: str) -> str:
        rows = list(csv.reader(StringIO(payload)))
        header = rows[1]
        indexes = {header.index(column) for column in columns}
        for row in rows[:2] + [row for row in rows[2:] if row and row[0] == "D"]:
            row[:] = [value for index, value in enumerate(row) if index not in indexes]
        output = StringIO(newline="")
        csv.writer(output, lineterminator="\n").writerows(rows)
        return output.getvalue()

    def test_parses_real_derived_mwh_null_and_publication_latency(self) -> None:
        observations = self.parse()

        self.assertEqual(len(observations), 3)
        first = observations[0]
        self.assertEqual(first.duid, "ADPBA1")
        self.assertEqual(first.interval_start, datetime(2026, 8, 28, 18, 5, tzinfo=UTC))
        self.assertEqual(first.soc_mwh, 3.786)
        self.assertEqual(first.intervention, 0)
        self.assertEqual(first.run_number, 1)
        self.assertEqual(first.dispatch_interval, "20260829001")
        self.assertEqual(first.last_changed, datetime(2026, 8, 28, 18, 0, 6, tzinfo=UTC))
        self.assertEqual(first.report_timestamp, datetime(2026, 8, 29, 18, 10, tzinfo=UTC))
        self.assertEqual(first.downloaded_at, self.downloaded_at)
        self.assertEqual(first.publication_latency_seconds, 86_700)
        self.assertEqual(first.publication_status, "next_day")
        self.assertEqual(first.ingestion_version, 8)
        self.assertEqual(first.correction_version, 2)
        missing = next(item for item in observations if item.duid == "KEPBG1")
        self.assertIsNone(missing.soc_mwh)
        self.assertEqual(
            [(item.duid, item.interval_start) for item in observations],
            sorted((item.duid, item.interval_start) for item in observations),
        )

    def test_v6_observations_include_all_canonical_fcas_services(self) -> None:
        observations = self.parse()

        self.assertEqual(
            tuple(observations[0].fcas.services),
            (
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
            ),
        )

    def test_retains_positive_and_enabled_zero_fcas_values(self) -> None:
        payload = self.mutate_row(self.payload, RAISE1SECFLAGS="1")
        observations = self.parse(payload)
        first = observations[0]
        positive = first.fcas.raise_6s
        self.assertEqual(positive.target_mw, 3.0)
        self.assertEqual(positive.enablement_status, 1)
        self.assertEqual(positive.actual_availability_mw, 3.0)
        self.assertTrue(positive.enabled)
        self.assertTrue(positive.cleared)
        self.assertTrue(positive.participating)

        enabled_zero = first.fcas.raise_1s
        self.assertEqual(enabled_zero.target_mw, 0.0)
        self.assertEqual(enabled_zero.enablement_status, 1)
        self.assertEqual(enabled_zero.actual_availability_mw, 0.0)
        self.assertTrue(enabled_zero.enabled)
        self.assertFalse(enabled_zero.cleared)
        self.assertFalse(enabled_zero.participating)
        self.assertEqual(first.fcas.reported_service_count, 10)

    def test_fcas_model_keeps_one_canonical_surface(self) -> None:
        service = self.parse()[0].fcas.raise_6s

        self.assertTrue(service.reported)
        self.assertIsNone(service.response_evidence)
        for redundant in (
            "is_enabled",
            "is_trapped",
            "is_stranded",
            "is_cleared",
            "is_participating",
            "response_verified",
            "response_verification",
        ):
            with self.subTest(redundant=redundant):
                self.assertFalse(hasattr(service, redundant))
        for alias in (
            "NextDayFcasService",
            "FcasServiceObservation",
            "FcasObservation",
        ):
            with self.subTest(alias=alias):
                self.assertFalse(hasattr(nextday_soc_module, alias))

    def test_derives_trapped_stranded_epsilon_and_unknown_response(self) -> None:
        payload = self.mutate_row(
            self.payload,
            RAISE6SECFLAGS="3",
            LOWER6SECFLAGS="4",
            RAISE60SEC="0.000001",
        )
        first = self.parse(payload)[0]
        trapped = first.fcas.raise_6s
        self.assertTrue(trapped.enabled)
        self.assertTrue(trapped.trapped)
        self.assertTrue(trapped.participating)
        stranded = first.fcas.lower_6s
        self.assertTrue(stranded.stranded)
        self.assertFalse(stranded.enabled)
        self.assertTrue(stranded.cleared)
        self.assertFalse(stranded.participating)
        epsilon = first.fcas.raise_60s
        self.assertFalse(epsilon.cleared)
        self.assertFalse(epsilon.participating)
        self.assertIsNone(epsilon.response_evidence)

    def test_missing_fcas_columns_are_null_without_shifting_other_services(self) -> None:
        payload = self.without_columns(
            self.payload,
            "RAISE1SEC",
            "RAISE1SECFLAGS",
            "RAISE1SECACTUALAVAILABILITY",
        )
        first = self.parse(payload)[0]
        self.assertEqual(tuple(first.fcas.services), FCAS_SERVICES)
        self.assertEqual(
            first.fcas.raise_1s,
            type(first.fcas.raise_1s)(),
        )
        self.assertEqual(first.fcas.raise_6s.target_mw, 3.0)

    def test_all_absent_fcas_columns_remain_null_in_fixed_service_map(self) -> None:
        payload = self.without_columns(
            self.payload,
            "RAISE1SEC",
            "RAISE1SECFLAGS",
            "RAISE1SECACTUALAVAILABILITY",
            "LOWER1SEC",
            "LOWER1SECFLAGS",
            "LOWER1SECACTUALAVAILABILITY",
            "RAISE6SEC",
            "RAISE6SECFLAGS",
            "RAISE6SECACTUALAVAILABILITY",
            "LOWER6SEC",
            "LOWER6SECFLAGS",
            "LOWER6SECACTUALAVAILABILITY",
            "RAISE60SEC",
            "RAISE60SECFLAGS",
            "RAISE60SECACTUALAVAILABILITY",
            "LOWER60SEC",
            "LOWER60SECFLAGS",
            "LOWER60SECACTUALAVAILABILITY",
            "RAISE5MIN",
            "RAISE5MINFLAGS",
            "RAISE5MINACTUALAVAILABILITY",
            "LOWER5MIN",
            "LOWER5MINFLAGS",
            "LOWER5MINACTUALAVAILABILITY",
            "RAISEREG",
            "RAISEREGFLAGS",
            "RAISEREGACTUALAVAILABILITY",
            "LOWERREG",
            "LOWERREGFLAGS",
            "LOWERREGACTUALAVAILABILITY",
        )
        first = self.parse(payload)[0]

        self.assertEqual(tuple(first.fcas.services), FCAS_SERVICES)
        self.assertEqual(first.fcas.reported_service_count, 0)
        self.assertTrue(
            all(service == type(service)() for service in first.fcas.services.values())
        )

    def test_soc_constructor_defaults_to_empty_fcas_group(self) -> None:
        observation = NextDaySocObservation(
            "ADPBA1",
            datetime(2026, 8, 28, 18, 5, tzinfo=UTC),
            1.0,
            0,
            1,
            "20260829001",
            datetime(2026, 8, 28, 18, tzinfo=UTC),
            "a" * 64,
            datetime(2026, 8, 29, 18, 10, tzinfo=UTC),
            datetime(2026, 8, 30, tzinfo=UTC),
            1,
            0,
        )

        self.assertEqual(observation.fcas, NextDayFcasObservation.empty())
        self.assertEqual(observation.fcas.reported_service_count, 0)

    def test_invalid_fcas_numeric_and_status_values_fail_closed(self) -> None:
        cases = (
            {"RAISE6SEC": "-1"},
            {"RAISE6SECACTUALAVAILABILITY": "-1"},
            {"RAISE6SECACTUALAVAILABILITY": "NaN"},
            {"RAISE6SECACTUALAVAILABILITY": "Infinity"},
            {"RAISE6SECFLAGS": "5"},
            {"RAISE6SECFLAGS": "1.5"},
            {"RAISE6SECFLAGS": "malformed"},
            {"RAISE6SECFLAGS": "1" * 5000},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                with self.assertRaises(NextDaySocParseError):
                    self.parse(self.mutate_row(self.payload, **changes))

    def test_filters_to_reviewed_duids_without_inventing_missing_rows(self) -> None:
        observations = self.parse(duids=frozenset(("ADPBA1",)))
        self.assertEqual(len(observations), 2)
        self.assertEqual({item.duid for item in observations}, {"ADPBA1"})

    def test_parses_real_derived_v5_initial_energy_storage(self) -> None:
        observations = parse_nextday_unit_solution_soc(
            V5_FIXTURE.read_text(encoding="utf-8"),
            duids=frozenset(("ADPBA1",)),
            source_artifact_id=(
                "5c3653d787824f5250210de11f67a6ff"
                "334433894016290e45952046d84576f4"
            ),
            downloaded_at=datetime(2026, 8, 30, tzinfo=UTC),
            ingestion_version=8,
            correction_version=47_012_9643,
        )

        self.assertEqual(len(observations), 1)
        observation = observations[0]
        self.assertEqual(observation.duid, "ADPBA1")
        self.assertEqual(observation.soc_mwh, 0.0)
        self.assertEqual(tuple(observation.fcas.services), FCAS_SERVICES)
        self.assertEqual(observation.fcas.raise_6s.target_mw, 0.0)
        self.assertEqual(observation.fcas.raise_6s.actual_availability_mw, 0.0)
        self.assertEqual(
            observation.interval_start,
            datetime(2025, 6, 30, 18, 5, tzinfo=UTC),
        )
        self.assertEqual(
            observation.report_timestamp,
            datetime(2025, 7, 1, 18, 10, tzinfo=UTC),
        )
        self.assertEqual(observation.publication_latency_seconds, 86_700)

    def test_rejects_physical_csv_records_over_hard_limit_before_trailer(self) -> None:
        maximum_records = 1_500_000
        metadata, header = self.payload.splitlines()[:2]
        payload = (
            metadata + "\n"
            + header + "\n"
            + "X\n" * (maximum_records - 2)
            + 'C,"END OF REPORT",1\n'
        )

        with self.assertRaisesRegex(
            NextDaySocParseError,
            "physical CSV record limit exceeded",
        ):
            self.parse(payload)

    def test_rejects_wrong_version_duplicate_rows_and_bad_trailer_count(self) -> None:
        duplicate = self.payload.replace(
            'C,"END OF REPORT",6',
            self.payload.splitlines()[2] + '\nC,"END OF REPORT",7',
        )
        cases = (
            self.payload.replace("I,DISPATCH,UNIT_SOLUTION,6,", "I,DISPATCH,UNIT_SOLUTION,4,", 1),
            duplicate,
            self.payload.replace('C,"END OF REPORT",6', 'C,"END OF REPORT",7'),
        )
        for payload in cases:
            with self.subTest(payload=payload[-80:]):
                with self.assertRaises(NextDaySocParseError):
                    self.parse(payload)

    def test_rejects_mixed_accepted_unit_solution_versions(self) -> None:
        mixed = self.payload.replace(
            "I,DISPATCH,UNIT_SOLUTION,6,",
            "I,DISPATCH,UNIT_SOLUTION,5,",
            1,
        )
        with self.assertRaises(NextDaySocParseError):
            self.parse(mixed)

    def test_rejects_negative_nonfinite_or_misaligned_authoritative_values(self) -> None:
        cases = (
            self.payload.replace(",3.786,3.78315,", ",-1,3.78315,", 1),
            self.payload.replace(",3.786,3.78315,", ",NaN,3.78315,", 1),
            self.payload.replace("2026/08/29 04:05:00", "2026/08/29 04:06:00", 1),
        )
        for payload in cases:
            with self.subTest(payload=payload[0:80]):
                with self.assertRaises(NextDaySocParseError):
                    self.parse(payload)

    def test_rejects_invalid_provenance_inputs_before_parsing(self) -> None:
        valid = {
            "payload": self.payload,
            "duids": frozenset(("ADPBA1",)),
            "source_artifact_id": "d7a2abdd2947ed4b222166b9f60e3a8052838190027dd9ce03cb291ba2d29bc4",
            "downloaded_at": self.downloaded_at,
            "ingestion_version": 8,
            "correction_version": 2,
        }
        invalid: tuple[dict[str, Any], ...] = (
            {"duids": {"ADPBA1"}},
            {"source_artifact_id": "not-a-digest"},
            {"downloaded_at": datetime(2026, 8, 30)},
            {"ingestion_version": True},
            {"correction_version": -1},
        )
        for override in invalid:
            with self.subTest(override=override):
                arguments = valid | override
                with self.assertRaises(NextDaySocParseError):
                    parse_nextday_unit_solution_soc(**arguments)


if __name__ == "__main__":
    unittest.main()
