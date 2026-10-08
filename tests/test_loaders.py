import csv
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from travel_pipeline.datasets import HOTEL_LISTINGS
from travel_pipeline.ingestion import loaders
from travel_pipeline.ingestion.readers import read_text_csv
from travel_pipeline.ingestion.schemas import FLIGHT_FILE_COLUMNS, SchemaDriftError

LOADED_AT = datetime(2026, 1, 1, 6, 0)
ALL_FLIGHTS = (date(2022, 2, 11), date(2022, 4, 1))
ALL_STAYS = (date(2013, 6, 24), date(2017, 9, 15))


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def test_reader_keeps_quoted_newlines_and_numbers_rows(fixtures_dir: Path) -> None:
    table = read_text_csv(fixtures_dir / "flights" / "economy.csv", FLIGHT_FILE_COLUMNS, ())
    expected = _rows(fixtures_dir / "flights" / "economy.csv")
    assert table.num_rows == len(expected)
    assert table.column("source_row_number").to_pylist() == list(range(1, len(expected) + 1))
    assert any("\n" in value for value in table.column("stop_raw").to_pylist())


def test_reader_rejects_header_drift(tmp_path: Path) -> None:
    drifted = tmp_path / "economy.csv"
    drifted.write_text('date,airline,price\n11-02-2022,Vistara,"5,953"\n', encoding="utf-8")
    with pytest.raises(SchemaDriftError, match="missing="):
        read_text_csv(drifted, FLIGHT_FILE_COLUMNS, ())


def test_flight_window_load_is_idempotent(
    con: duckdb.DuckDBPyConnection, fixtures_dir: Path
) -> None:
    version_dir = fixtures_dir / "flights"
    total = len(_rows(version_dir / "economy.csv")) + len(_rows(version_dir / "business.csv"))

    first = loaders.load_flight_fare_quotes(
        con,
        version_dir=version_dir,
        dataset_version="v1",
        start=ALL_FLIGHTS[0],
        end=ALL_FLIGHTS[1],
        loaded_at=LOADED_AT,
    )
    snapshot = con.execute(
        "SELECT * FROM raw.flight_fare_quotes ORDER BY source_file, source_row_number"
    ).fetchall()
    one_day = loaders.load_flight_fare_quotes(
        con,
        version_dir=version_dir,
        dataset_version="v1",
        start=date(2022, 2, 12),
        end=date(2022, 2, 13),
        loaded_at=LOADED_AT,
    )

    assert first.rows_inserted == total
    assert one_day.rows_deleted == one_day.rows_inserted > 0
    assert (
        con.execute(
            "SELECT * FROM raw.flight_fare_quotes ORDER BY source_file, source_row_number"
        ).fetchall()
        == snapshot
    )


def test_flight_load_only_touches_its_window(
    con: duckdb.DuckDBPyConnection, fixtures_dir: Path
) -> None:
    result = loaders.load_flight_fare_quotes(
        con,
        version_dir=fixtures_dir / "flights",
        dataset_version="v1",
        start=date(2022, 2, 11),
        end=date(2022, 2, 12),
        loaded_at=LOADED_AT,
    )
    dates = con.execute("SELECT DISTINCT departure_date FROM raw.flight_fare_quotes").fetchall()
    assert result.rows_inserted > 0
    assert dates == [(date(2022, 2, 11),)]


def test_each_booking_yields_one_event_of_each_type(
    con: duckdb.DuckDBPyConnection, fixtures_dir: Path
) -> None:
    bookings = _rows(fixtures_dir / "hotel_bookings" / "hotel_bookings.csv")
    loaders.load_hotel_booking_events(
        con,
        version_dir=fixtures_dir / "hotel_bookings",
        dataset_version="v1",
        start=ALL_STAYS[0],
        end=ALL_STAYS[1],
        loaded_at=LOADED_AT,
    )

    counts = dict(
        con.execute(
            "SELECT event_type, count(*) FROM raw.hotel_booking_events GROUP BY 1"
        ).fetchall()
    )
    statuses = dict(
        con.execute(
            "SELECT event_status, count(*) FROM raw.hotel_booking_events WHERE event_type = 'status_changed' GROUP BY 1"
        ).fetchall()
    )
    expected_statuses: dict[str, int] = {}
    for row in bookings:
        expected_statuses[row["reservation_status"]] = (
            expected_statuses.get(row["reservation_status"], 0) + 1
        )

    assert counts == {"booking_created": len(bookings), "status_changed": len(bookings)}
    assert statuses == expected_statuses
    # NA / NULL markers become SQL NULLs under the declared types.
    assert (
        con.execute(
            "SELECT count(*) FROM raw.hotel_booking_events WHERE children IS NULL"
        ).fetchone()[0]
        == 2
    )


def test_existing_table_with_drifted_schema_fails(
    con: duckdb.DuckDBPyConnection, fixtures_dir: Path
) -> None:
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE TABLE raw.flight_fare_quotes (departure_date DATE)")
    with pytest.raises(SchemaDriftError, match="does not match its declared schema"):
        loaders.load_flight_fare_quotes(
            con,
            version_dir=fixtures_dir / "flights",
            dataset_version="v1",
            start=ALL_FLIGHTS[0],
            end=ALL_FLIGHTS[1],
            loaded_at=LOADED_AT,
        )


def test_listings_are_fully_replaced(con: duckdb.DuckDBPyConnection, fixtures_dir: Path) -> None:
    version_dir = fixtures_dir / "hotel_listings"
    expected = sum(len(_rows(version_dir / name)) for name in HOTEL_LISTINGS.files)
    first = loaders.load_hotel_listings(
        con,
        version_dir=version_dir,
        file_names=HOTEL_LISTINGS.files,
        dataset_version="v1",
        loaded_at=LOADED_AT,
    )
    second = loaders.load_hotel_listings(
        con,
        version_dir=version_dir,
        file_names=HOTEL_LISTINGS.files,
        dataset_version="v2",
        loaded_at=LOADED_AT,
    )

    assert first.rows_inserted == second.rows_inserted == second.rows_deleted == expected
    assert con.execute("SELECT DISTINCT dataset_version FROM raw.hotel_listings").fetchall() == [
        ("v2",)
    ]
    assert con.execute("SELECT count(DISTINCT city) FROM raw.hotel_listings").fetchone()[0] == 6
