"""End-to-end: every job on the synthetic fixtures, in-process, against a real DuckDB file."""

import csv
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest
from dagster import DagsterInstance, ExecuteInProcessResult, instance_for_test

from travel_pipeline.definitions import defs, settings
from travel_pipeline.partitions import FLIGHT_PARTITIONS, STAY_EVENT_PARTITIONS
from travel_pipeline.schedules import partition_range_tags

pytestmark = pytest.mark.e2e

FIXTURES = Path(__file__).parent / "fixtures" / "kaggle"
VOLATILE_COLUMNS = {"_loaded_at", "valid_from"}


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _full_range(partitions_def) -> dict[str, str]:
    keys = partitions_def.get_partition_keys()
    return partition_range_tags(keys[0], keys[-1])


def _run(
    instance: DagsterInstance, job_name: str, tags: dict[str, str] | None = None
) -> ExecuteInProcessResult:
    result = defs.resolve_job_def(job_name).execute_in_process(instance=instance, tags=tags or {})
    assert result.success, f"{job_name} failed"
    failed = [e for e in result.get_asset_check_evaluations() if not e.passed]
    assert not failed, f"{job_name}: failed checks {[(e.asset_key, e.check_name) for e in failed]}"
    return result


def _query(sql: str) -> list[tuple]:
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
        return con.execute(sql).fetchall()


def _marts_fingerprint() -> dict[str, list[tuple]]:
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'marts' ORDER BY 1"
            ).fetchall()
        ]
        fingerprint = {}
        for table in tables:
            columns = [
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = 'marts' AND table_name = ? ORDER BY ordinal_position",
                    [table],
                ).fetchall()
                if r[0] not in VOLATILE_COLUMNS
            ]
            select_list = ", ".join(f'"{c}"' for c in columns)
            fingerprint[table] = con.execute(
                f"SELECT {select_list} FROM marts.{table} ORDER BY ALL"
            ).fetchall()
    return fingerprint


@pytest.fixture(scope="module")
def instance() -> Iterator[DagsterInstance]:
    with instance_for_test() as test_instance:
        yield test_instance


@pytest.fixture(scope="module")
def pipeline(instance: DagsterInstance) -> DagsterInstance:
    _run(instance, "ingest_job")
    _run(instance, "flights_load_job", _full_range(FLIGHT_PARTITIONS))
    _run(instance, "stays_load_job", _full_range(STAY_EVENT_PARTITIONS))
    _run(instance, "transform_job")
    return instance


def test_all_fixture_rows_reach_the_facts(pipeline: DagsterInstance) -> None:
    flights = len(_csv_rows(FIXTURES / "flights" / "economy.csv")) + len(
        _csv_rows(FIXTURES / "flights" / "business.csv")
    )
    bookings = _csv_rows(FIXTURES / "hotel_bookings" / "hotel_bookings.csv")
    expected_cancellation_rate = sum(r["is_canceled"] == "1" for r in bookings) / len(bookings)

    assert _query("SELECT count(*) FROM marts.fct_flight_fare_quotes") == [(flights,)]
    [(count, rate, as_of)] = _query(
        "SELECT count(*), avg(CASE WHEN is_canceled THEN 1.0 ELSE 0.0 END), max(as_of_date) FROM marts.fct_hotel_bookings"
    )
    assert count == len(bookings)
    assert rate == pytest.approx(expected_cancellation_rate)
    assert str(as_of) == max(r["reservation_status_date"] for r in bookings)


def test_every_partition_is_recorded_as_materialized(pipeline: DagsterInstance) -> None:
    from dagster import AssetKey  # noqa: PLC0415

    flights = pipeline.get_materialized_partitions(AssetKey(["raw", "flight_fare_quotes"]))
    stays = pipeline.get_materialized_partitions(AssetKey(["raw", "hotel_booking_events"]))
    assert flights == set(FLIGHT_PARTITIONS.get_partition_keys())
    assert stays == set(STAY_EVENT_PARTITIONS.get_partition_keys())


def test_cross_subject_mart_covers_every_city(pipeline: DagsterInstance) -> None:
    rows = _query(
        "SELECT city, mid_range_listings, indicative_trip_cost_inr FROM marts.mart_city_trip_cost_benchmark ORDER BY city"
    )
    assert [r[0] for r in rows] == [
        "Bangalore",
        "Chennai",
        "Delhi",
        "Hyderabad",
        "Kolkata",
        "Mumbai",
    ]
    assert _query(
        "SELECT count(*) FROM snapshots.snap_hotel_listings WHERE dbt_valid_to IS NULL"
    ) == [(24,)]


def test_reruns_and_partial_reloads_are_idempotent(pipeline: DagsterInstance) -> None:
    before = _marts_fingerprint()

    _run(pipeline, "flights_load_job", partition_range_tags("2022-02-12", "2022-02-12"))
    _run(pipeline, "stays_load_job", partition_range_tags("2016-02-01", "2016-03-31"))
    _run(pipeline, "ingest_job")
    _run(pipeline, "transform_job")

    assert _marts_fingerprint() == before
    # The snapshot did not open new versions for unchanged listings.
    assert _query("SELECT count(*) FROM snapshots.snap_hotel_listings") == [(24,)]
