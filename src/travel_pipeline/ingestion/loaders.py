"""Idempotent loads from the landing zone into the DuckDB ``raw`` schema.

Partitioned tables are written with replace-window semantics: inside one transaction,
delete every row whose partition column falls in ``[start, end)`` and insert the rows the
landed files hold for that window. Re-running any window therefore converges to the same
state, and a failed run leaves the previous contents untouched.

All SQL identifiers come from the constants in ``schemas.py``; only values are bound.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from travel_pipeline.ingestion.readers import read_text_csv
from travel_pipeline.ingestion.schemas import (
    FLIGHT_FILE_COLUMNS,
    FLIGHT_NULL_VALUES,
    HOTEL_BOOKING_FILE_COLUMNS,
    HOTEL_BOOKING_NULL_VALUES,
    HOTEL_LISTING_FILE_COLUMNS,
    HOTEL_LISTING_NULL_VALUES,
    RAW_FLIGHT_FARE_QUOTES,
    RAW_HOTEL_BOOKING_EVENTS,
    RAW_HOTEL_LISTINGS,
    Column,
    SchemaDriftError,
)

RAW_SCHEMA = "raw"
FLIGHT_FILES = {"economy.csv": "Economy", "business.csv": "Business"}


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows_deleted: int
    rows_inserted: int
    window_start: date | None
    window_end: date | None


def _base_type(dtype: str) -> str:
    return dtype.replace("NOT NULL", "").strip().upper()


def ensure_raw_table(
    con: duckdb.DuckDBPyConnection, table: str, columns: tuple[tuple[str, str], ...]
) -> None:
    """Create ``raw.<table>`` if needed and fail if an existing table has drifted."""
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    ddl = ", ".join(f'"{name}" {dtype}' for name, dtype in columns)
    con.execute(f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{table} ({ddl})")
    actual = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [RAW_SCHEMA, table],
    ).fetchall()
    expected = [(name, _base_type(dtype)) for name, dtype in columns]
    if [(n, t.upper()) for n, t in actual] != expected:
        raise SchemaDriftError(
            f"{RAW_SCHEMA}.{table} does not match its declared schema; "
            f"expected {expected}, found {actual}. Migrate or drop the table before reloading."
        )


@contextmanager
def _transaction(con: duckdb.DuckDBPyConnection) -> Iterator[None]:
    con.execute("BEGIN TRANSACTION")
    try:
        yield
    except BaseException:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")


def _typed_select(columns: tuple[Column, ...]) -> str:
    return ",\n            ".join(
        f'CAST("{c.target}" AS {c.dtype}) AS "{c.target}"' for c in columns
    )


def _register(con: duckdb.DuckDBPyConnection, name: str, table: pa.Table) -> None:
    """(Re)bind an Arrow table to a connection-local view name."""
    con.register(name, table)


def flight_quotes_select(con: duckdb.DuckDBPyConnection, version_dir: Path) -> str:
    """Register both class files and return a SELECT producing raw.flight_fare_quotes rows.

    The SELECT expects two bound parameters: dataset_version and _loaded_at.
    """
    parts = []
    for file_name, cabin_class in FLIGHT_FILES.items():
        view = f"staged_{cabin_class.lower()}"
        _register(
            con,
            view,
            read_text_csv(version_dir / file_name, FLIGHT_FILE_COLUMNS, FLIGHT_NULL_VALUES),
        )
        parts.append(
            f"""
        SELECT
            CAST(strptime(journey_date_raw, '%d-%m-%Y') AS DATE) AS departure_date,
            '{cabin_class}' AS cabin_class,
            {_typed_select(FLIGHT_FILE_COLUMNS)},
            '{file_name}' AS source_file,
            source_row_number,
            $dataset_version AS dataset_version,
            $loaded_at AS _loaded_at
        FROM {view}"""
        )
    return "\nUNION ALL\n".join(parts)


def load_flight_fare_quotes(
    con: duckdb.DuckDBPyConnection,
    *,
    version_dir: Path,
    dataset_version: str,
    start: date,
    end: date,
    loaded_at: datetime,
) -> LoadResult:
    ensure_raw_table(con, "flight_fare_quotes", RAW_FLIGHT_FARE_QUOTES)
    select_sql = flight_quotes_select(con, version_dir)
    return _replace_window(
        con,
        table="flight_fare_quotes",
        partition_column="departure_date",
        select_sql=select_sql,
        dataset_version=dataset_version,
        loaded_at=loaded_at,
        start=start,
        end=end,
    )


def hotel_events_select(con: duckdb.DuckDBPyConnection, version_dir: Path) -> str:
    """Register the bookings file and return a SELECT of booking events.

    Each booking yields two events: ``booking_created`` on its booking date (arrival date
    minus lead time) and ``status_changed`` on ``reservation_status_date``. Replaying the
    events day by day therefore only reveals what was known on that day.
    """
    file_name = "hotel_bookings.csv"
    _register(
        con,
        "staged_hotel_bookings",
        read_text_csv(
            version_dir / file_name, HOTEL_BOOKING_FILE_COLUMNS, HOTEL_BOOKING_NULL_VALUES
        ),
    )
    payload = ", ".join(f'"{c.target}"' for c in HOTEL_BOOKING_FILE_COLUMNS)
    return f"""
        WITH typed AS (
            SELECT
                {_typed_select(HOTEL_BOOKING_FILE_COLUMNS)},
                source_row_number
            FROM staged_hotel_bookings
        ),
        dated AS (
            SELECT
                *,
                'HB' || lpad(CAST(source_row_number AS VARCHAR), 6, '0') AS booking_id,
                make_date(
                    arrival_date_year,
                    month(strptime(arrival_date_month || ' 1 2000', '%B %d %Y')),
                    arrival_date_day_of_month
                ) AS arrival_date
            FROM typed
        ),
        events AS (
            SELECT
                arrival_date - lead_time AS event_date,
                'booking_created' AS event_type,
                'Booked' AS event_status,
                *
            FROM dated
            UNION ALL
            SELECT
                reservation_status_date AS event_date,
                'status_changed' AS event_type,
                reservation_status AS event_status,
                *
            FROM dated
        )
        SELECT
            event_date,
            event_type,
            event_status,
            booking_id,
            arrival_date - lead_time AS booking_date,
            arrival_date,
            {payload},
            '{file_name}' AS source_file,
            source_row_number,
            $dataset_version AS dataset_version,
            $loaded_at AS _loaded_at
        FROM events"""


def load_hotel_booking_events(
    con: duckdb.DuckDBPyConnection,
    *,
    version_dir: Path,
    dataset_version: str,
    start: date,
    end: date,
    loaded_at: datetime,
) -> LoadResult:
    ensure_raw_table(con, "hotel_booking_events", RAW_HOTEL_BOOKING_EVENTS)
    select_sql = hotel_events_select(con, version_dir)
    return _replace_window(
        con,
        table="hotel_booking_events",
        partition_column="event_date",
        select_sql=select_sql,
        dataset_version=dataset_version,
        loaded_at=loaded_at,
        start=start,
        end=end,
    )


def load_hotel_listings(
    con: duckdb.DuckDBPyConnection,
    *,
    version_dir: Path,
    file_names: tuple[str, ...],
    dataset_version: str,
    loaded_at: datetime,
) -> LoadResult:
    """Replace raw.hotel_listings with the current version (a point-in-time snapshot)."""
    ensure_raw_table(con, "hotel_listings", RAW_HOTEL_LISTINGS)
    parts = []
    for index, file_name in enumerate(file_names):
        view = f"staged_listings_{index}"
        _register(
            con,
            view,
            read_text_csv(
                version_dir / file_name, HOTEL_LISTING_FILE_COLUMNS, HOTEL_LISTING_NULL_VALUES
            ),
        )
        city = Path(file_name).stem.replace("_", " ").title()
        parts.append(
            f"""
        SELECT
            '{city}' AS city,
            {_typed_select(HOTEL_LISTING_FILE_COLUMNS)},
            '{file_name}' AS source_file,
            source_row_number,
            $dataset_version AS dataset_version,
            $loaded_at AS _loaded_at
        FROM {view}"""
        )
    select_sql = "\nUNION ALL\n".join(parts)
    with _transaction(con):
        deleted = con.execute(f"DELETE FROM {RAW_SCHEMA}.hotel_listings").fetchone()[0]
        inserted = con.execute(
            f"INSERT INTO {RAW_SCHEMA}.hotel_listings {select_sql}",
            {"dataset_version": dataset_version, "loaded_at": loaded_at},
        ).fetchone()[0]
    return LoadResult("hotel_listings", int(deleted), int(inserted), None, None)


def _replace_window(
    con: duckdb.DuckDBPyConnection,
    *,
    table: str,
    partition_column: str,
    select_sql: str,
    dataset_version: str,
    loaded_at: datetime,
    start: date,
    end: date,
) -> LoadResult:
    with _transaction(con):
        deleted = con.execute(
            f"DELETE FROM {RAW_SCHEMA}.{table} WHERE {partition_column} >= $start AND {partition_column} < $end",
            {"start": start, "end": end},
        ).fetchone()[0]
        inserted = con.execute(
            f"INSERT INTO {RAW_SCHEMA}.{table} SELECT * FROM ({select_sql}) "
            f"WHERE {partition_column} >= $start AND {partition_column} < $end",
            {
                "dataset_version": dataset_version,
                "loaded_at": loaded_at,
                "start": start,
                "end": end,
            },
        ).fetchone()[0]
    return LoadResult(table, int(deleted), int(inserted), start, end)


def partition_date_counts(
    con: duckdb.DuckDBPyConnection, select_sql: str, column: str
) -> dict[date, int]:
    """Row counts per partition date in the landed data (used by reconciliation checks)."""
    rows = con.execute(
        f"SELECT {column}, count(*) FROM ({select_sql}) GROUP BY 1",
        {"dataset_version": "", "loaded_at": datetime(1970, 1, 1)},
    ).fetchall()
    return {r[0]: int(r[1]) for r in rows}
