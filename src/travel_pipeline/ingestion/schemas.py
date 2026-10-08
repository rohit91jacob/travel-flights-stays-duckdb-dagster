"""Declared schemas for every upstream file and the raw DuckDB tables built from them.

The header of each file must match ``source`` names exactly and in order; anything else
is schema drift and fails the load instead of silently shifting columns. Values are read
as text and cast to ``dtype`` inside DuckDB with strict casts, so a value that does not
fit its declared type also fails loudly.
"""

from __future__ import annotations

from dataclasses import dataclass


class SchemaDriftError(ValueError):
    """Raised when an upstream file or raw table no longer matches its declared schema."""


@dataclass(frozen=True)
class Column:
    source: str
    target: str
    dtype: str


FLIGHT_FILE_COLUMNS: tuple[Column, ...] = (
    Column("date", "journey_date_raw", "VARCHAR"),
    Column("airline", "airline", "VARCHAR"),
    Column("ch_code", "carrier_code", "VARCHAR"),
    Column("num_code", "flight_number", "VARCHAR"),
    Column("dep_time", "dep_time_raw", "VARCHAR"),
    Column("from", "source_city", "VARCHAR"),
    Column("time_taken", "time_taken_raw", "VARCHAR"),
    Column("stop", "stop_raw", "VARCHAR"),
    Column("arr_time", "arr_time_raw", "VARCHAR"),
    Column("to", "destination_city", "VARCHAR"),
    Column("price", "price_raw", "VARCHAR"),
)

HOTEL_BOOKING_FILE_COLUMNS: tuple[Column, ...] = (
    Column("hotel", "hotel", "VARCHAR"),
    Column("is_canceled", "is_canceled", "INTEGER"),
    Column("lead_time", "lead_time", "INTEGER"),
    Column("arrival_date_year", "arrival_date_year", "INTEGER"),
    Column("arrival_date_month", "arrival_date_month", "VARCHAR"),
    Column("arrival_date_week_number", "arrival_date_week_number", "INTEGER"),
    Column("arrival_date_day_of_month", "arrival_date_day_of_month", "INTEGER"),
    Column("stays_in_weekend_nights", "stays_in_weekend_nights", "INTEGER"),
    Column("stays_in_week_nights", "stays_in_week_nights", "INTEGER"),
    Column("adults", "adults", "INTEGER"),
    Column("children", "children", "INTEGER"),
    Column("babies", "babies", "INTEGER"),
    Column("meal", "meal", "VARCHAR"),
    Column("country", "country", "VARCHAR"),
    Column("market_segment", "market_segment", "VARCHAR"),
    Column("distribution_channel", "distribution_channel", "VARCHAR"),
    Column("is_repeated_guest", "is_repeated_guest", "INTEGER"),
    Column("previous_cancellations", "previous_cancellations", "INTEGER"),
    Column("previous_bookings_not_canceled", "previous_bookings_not_canceled", "INTEGER"),
    Column("reserved_room_type", "reserved_room_type", "VARCHAR"),
    Column("assigned_room_type", "assigned_room_type", "VARCHAR"),
    Column("booking_changes", "booking_changes", "INTEGER"),
    Column("deposit_type", "deposit_type", "VARCHAR"),
    Column("agent", "agent", "VARCHAR"),
    Column("company", "company", "VARCHAR"),
    Column("days_in_waiting_list", "days_in_waiting_list", "INTEGER"),
    Column("customer_type", "customer_type", "VARCHAR"),
    Column("adr", "adr", "DECIMAL(10,2)"),
    Column("required_car_parking_spaces", "required_car_parking_spaces", "INTEGER"),
    Column("total_of_special_requests", "total_of_special_requests", "INTEGER"),
    Column("reservation_status", "reservation_status", "VARCHAR"),
    Column("reservation_status_date", "reservation_status_date", "DATE"),
)

HOTEL_LISTING_FILE_COLUMNS: tuple[Column, ...] = (
    Column("Hotel Name", "hotel_name", "VARCHAR"),
    Column("Rating", "rating", "DECIMAL(3,1)"),
    Column("Rating Description", "rating_description", "VARCHAR"),
    Column("Reviews", "reviews", "INTEGER"),
    Column("Star Rating", "star_rating", "INTEGER"),
    Column("Location", "location", "VARCHAR"),
    Column("Nearest Landmark", "nearest_landmark", "VARCHAR"),
    Column("Distance to Landmark", "distance_to_landmark_raw", "VARCHAR"),
    Column("Price", "price_raw", "VARCHAR"),
    Column("Tax", "tax_raw", "VARCHAR"),
)

# Null markers used by each upstream file (the hotel file spells missing values NA / NULL).
FLIGHT_NULL_VALUES: tuple[str, ...] = ()
HOTEL_BOOKING_NULL_VALUES: tuple[str, ...] = ("NA", "NULL")
HOTEL_LISTING_NULL_VALUES: tuple[str, ...] = ("",)

LINEAGE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("source_file", "VARCHAR NOT NULL"),
    ("source_row_number", "BIGINT NOT NULL"),
    ("dataset_version", "VARCHAR NOT NULL"),
    ("_loaded_at", "TIMESTAMP NOT NULL"),
)


def _file_columns(columns: tuple[Column, ...]) -> tuple[tuple[str, str], ...]:
    return tuple((c.target, c.dtype) for c in columns)


RAW_FLIGHT_FARE_QUOTES: tuple[tuple[str, str], ...] = (
    ("departure_date", "DATE NOT NULL"),
    ("cabin_class", "VARCHAR NOT NULL"),
    *_file_columns(FLIGHT_FILE_COLUMNS),
    *LINEAGE_COLUMNS,
)

RAW_HOTEL_BOOKING_EVENTS: tuple[tuple[str, str], ...] = (
    ("event_date", "DATE NOT NULL"),
    ("event_type", "VARCHAR NOT NULL"),
    ("event_status", "VARCHAR NOT NULL"),
    ("booking_id", "VARCHAR NOT NULL"),
    ("booking_date", "DATE NOT NULL"),
    ("arrival_date", "DATE NOT NULL"),
    *_file_columns(HOTEL_BOOKING_FILE_COLUMNS),
    *LINEAGE_COLUMNS,
)

RAW_HOTEL_LISTINGS: tuple[tuple[str, str], ...] = (
    ("city", "VARCHAR NOT NULL"),
    *_file_columns(HOTEL_LISTING_FILE_COLUMNS),
    *LINEAGE_COLUMNS,
)


def validate_header(actual: list[str], expected: tuple[Column, ...], *, file_name: str) -> None:
    wanted = [c.source for c in expected]
    if actual != wanted:
        missing = [c for c in wanted if c not in actual]
        unexpected = [c for c in actual if c not in wanted]
        raise SchemaDriftError(
            f"{file_name}: header does not match the declared schema "
            f"(missing={missing}, unexpected={unexpected}, order_changed={not missing and not unexpected})"
        )
