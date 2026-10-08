"""Raw assets: typed, partitioned copies of the landed files in DuckDB, plus their checks."""

from datetime import UTC, date, datetime

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    BackfillPolicy,
    MaterializeResult,
    MetadataValue,
    PartitionsDefinition,
    TimeWindowPartitionsDefinition,
    asset,
    asset_check,
)

from travel_pipeline.assets.landing import landing_key
from travel_pipeline.datasets import FLIGHTS, HOTEL_BOOKINGS, HOTEL_LISTINGS
from travel_pipeline.ingestion import loaders
from travel_pipeline.partitions import FLIGHT_PARTITIONS, STAY_EVENT_PARTITIONS
from travel_pipeline.policies import DAILY_FRESHNESS, DUCKDB_POOL
from travel_pipeline.resources import DuckDBWarehouse, LandingZone

RAW_FLIGHT_QUOTES = AssetKey(["raw", "flight_fare_quotes"])
RAW_BOOKING_EVENTS = AssetKey(["raw", "hotel_booking_events"])
RAW_LISTINGS = AssetKey(["raw", "hotel_listings"])
RAW_ASSET_KEYS = (RAW_FLIGHT_QUOTES, RAW_BOOKING_EVENTS, RAW_LISTINGS)

_MAX_REPORTED = 25


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _window(context: AssetExecutionContext) -> tuple[date, date]:
    window = context.partition_time_window
    return window.start.date(), window.end.date()


def _load_metadata(result: loaders.LoadResult, version_id: str) -> dict:
    return {
        "dataset_version": version_id,
        "window_start": str(result.window_start),
        "window_end_exclusive": str(result.window_end),
        "rows_inserted": result.rows_inserted,
        "rows_replaced": result.rows_deleted,
    }


@asset(
    key=RAW_FLIGHT_QUOTES,
    deps=[landing_key(FLIGHTS)],
    partitions_def=FLIGHT_PARTITIONS,
    backfill_policy=BackfillPolicy.single_run(),
    pool=DUCKDB_POOL,
    group_name="raw",
    kinds={"duckdb"},
    freshness_policy=DAILY_FRESHNESS,
    description=(
        "One row per fare quote (itinerary option x cabin class) scraped from EaseMyTrip, "
        "partitioned by departure date. Values are kept as published (price '5,953', "
        "duration '02h 10m'); parsing happens in dbt staging."
    ),
)
def raw_flight_fare_quotes(
    context: AssetExecutionContext, landing: LandingZone, warehouse: DuckDBWarehouse
) -> MaterializeResult:
    manifest, version_dir = landing.current(FLIGHTS)
    start, end = _window(context)
    with warehouse.connect() as con:
        result = loaders.load_flight_fare_quotes(
            con,
            version_dir=version_dir,
            dataset_version=manifest.version_id,
            start=start,
            end=end,
            loaded_at=_utc_now(),
        )
    context.log.info("loaded %s fare quotes for [%s, %s)", result.rows_inserted, start, end)
    return MaterializeResult(metadata=_load_metadata(result, manifest.version_id))


@asset(
    key=RAW_BOOKING_EVENTS,
    deps=[landing_key(HOTEL_BOOKINGS)],
    partitions_def=STAY_EVENT_PARTITIONS,
    backfill_policy=BackfillPolicy.single_run(),
    pool=DUCKDB_POOL,
    group_name="raw",
    kinds={"duckdb"},
    freshness_policy=DAILY_FRESHNESS,
    description=(
        "Hotel booking events partitioned by event date: `booking_created` on the booking "
        "date (arrival date - lead time) and `status_changed` on the reservation status "
        "date. Every booking contributes exactly one event of each type."
    ),
)
def raw_hotel_booking_events(
    context: AssetExecutionContext, landing: LandingZone, warehouse: DuckDBWarehouse
) -> MaterializeResult:
    manifest, version_dir = landing.current(HOTEL_BOOKINGS)
    start, end = _window(context)
    with warehouse.connect() as con:
        result = loaders.load_hotel_booking_events(
            con,
            version_dir=version_dir,
            dataset_version=manifest.version_id,
            start=start,
            end=end,
            loaded_at=_utc_now(),
        )
    context.log.info("loaded %s booking events for [%s, %s)", result.rows_inserted, start, end)
    return MaterializeResult(metadata=_load_metadata(result, manifest.version_id))


@asset(
    key=RAW_LISTINGS,
    deps=[landing_key(HOTEL_LISTINGS)],
    pool=DUCKDB_POOL,
    group_name="raw",
    kinds={"duckdb"},
    description=(
        "Point-in-time snapshot of MakeMyTrip hotel listings for the six metros the flight "
        "data covers. Fully replaced on every load; history is kept by the dbt snapshot."
    ),
)
def raw_hotel_listings(landing: LandingZone, warehouse: DuckDBWarehouse) -> MaterializeResult:
    manifest, version_dir = landing.current(HOTEL_LISTINGS)
    with warehouse.connect() as con:
        result = loaders.load_hotel_listings(
            con,
            version_dir=version_dir,
            file_names=HOTEL_LISTINGS.files,
            dataset_version=manifest.version_id,
            loaded_at=_utc_now(),
        )
    return MaterializeResult(
        metadata={
            "dataset_version": manifest.version_id,
            "rows_inserted": result.rows_inserted,
            "dagster/row_count": result.rows_inserted,
        }
    )


# --------------------------------------------------------------------------- checks


def _bounds(partitions_def: PartitionsDefinition) -> tuple[date, date]:
    assert isinstance(partitions_def, TimeWindowPartitionsDefinition)  # noqa: S101
    return partitions_def.start.date(), partitions_def.end.date()


def _reconcile(
    *,
    landed: dict[date, int],
    loaded: dict[date, int],
    partitions_def: PartitionsDefinition,
) -> AssetCheckResult:
    first, end = _bounds(partitions_def)
    outside = sorted(d for d in landed if not first <= d < end)
    mismatched = sorted(d for d, n in loaded.items() if landed.get(d, 0) != n)
    return AssetCheckResult(
        passed=not outside and not mismatched,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "landed_rows": sum(landed.values()),
            "loaded_rows": sum(loaded.values()),
            "loaded_dates": len(loaded),
            "landed_dates": len(landed),
            "dates_with_count_mismatch": MetadataValue.json(
                {
                    str(d): {"landed": landed.get(d, 0), "loaded": loaded[d]}
                    for d in mismatched[:_MAX_REPORTED]
                }
            ),
            "landed_dates_outside_partitions": MetadataValue.json(
                [str(d) for d in outside[:_MAX_REPORTED]]
            ),
        },
        description=(
            "Every loaded partition holds exactly the rows the landed files have for it, and no "
            "landed row falls outside the partition range."
        ),
    )


def _loaded_counts(con, table: str, column: str) -> dict[date, int]:
    rows = con.execute(f"SELECT {column}, count(*) FROM raw.{table} GROUP BY 1").fetchall()
    return {r[0]: int(r[1]) for r in rows}


@asset_check(
    asset=RAW_FLIGHT_QUOTES, name="reconciles_with_landing", blocking=True, pool=DUCKDB_POOL
)
def flight_quotes_reconcile(landing: LandingZone, warehouse: DuckDBWarehouse) -> AssetCheckResult:
    _, version_dir = landing.current(FLIGHTS)
    with warehouse.connect() as con:
        landed = loaders.partition_date_counts(
            con, loaders.flight_quotes_select(con, version_dir), "departure_date"
        )
        loaded = _loaded_counts(con, "flight_fare_quotes", "departure_date")
    return _reconcile(landed=landed, loaded=loaded, partitions_def=FLIGHT_PARTITIONS)


@asset_check(
    asset=RAW_BOOKING_EVENTS, name="reconciles_with_landing", blocking=True, pool=DUCKDB_POOL
)
def booking_events_reconcile(landing: LandingZone, warehouse: DuckDBWarehouse) -> AssetCheckResult:
    _, version_dir = landing.current(HOTEL_BOOKINGS)
    with warehouse.connect() as con:
        landed = loaders.partition_date_counts(
            con, loaders.hotel_events_select(con, version_dir), "event_date"
        )
        loaded = _loaded_counts(con, "hotel_booking_events", "event_date")
    return _reconcile(landed=landed, loaded=loaded, partitions_def=STAY_EVENT_PARTITIONS)


@asset_check(
    asset=RAW_BOOKING_EVENTS, name="event_pairs_consistent", blocking=True, pool=DUCKDB_POOL
)
def booking_events_consistent(warehouse: DuckDBWarehouse) -> AssetCheckResult:
    """No booking is created twice or finalised twice, and no status precedes its booking."""
    with warehouse.connect(read_only=True) as con:
        dup_created, dup_status, status_before_booking, orphan_status = con.execute(
            """
            WITH per_booking AS (
                SELECT
                    booking_id,
                    count(*) FILTER (WHERE event_type = 'booking_created') AS n_created,
                    count(*) FILTER (WHERE event_type = 'status_changed') AS n_status,
                    min(event_date) FILTER (WHERE event_type = 'status_changed') AS status_date,
                    min(booking_date) AS booking_date
                FROM raw.hotel_booking_events
                GROUP BY booking_id
            )
            SELECT
                count(*) FILTER (WHERE n_created > 1),
                count(*) FILTER (WHERE n_status > 1),
                count(*) FILTER (WHERE status_date < booking_date),
                count(*) FILTER (WHERE n_status = 1 AND n_created = 0)
            FROM per_booking
            """
        ).fetchone()
    return AssetCheckResult(
        passed=dup_created == 0 and dup_status == 0 and status_before_booking == 0,
        metadata={
            "bookings_created_twice": dup_created,
            "bookings_finalised_twice": dup_status,
            "status_before_booking_date": status_before_booking,
            # Expected to be > 0 only when windows were backfilled out of order.
            "status_events_without_creation": orphan_status,
        },
    )


@asset_check(asset=RAW_LISTINGS, name="one_row_per_city_hotel", blocking=True, pool=DUCKDB_POOL)
def listings_unique(warehouse: DuckDBWarehouse) -> AssetCheckResult:
    with warehouse.connect(read_only=True) as con:
        total, duplicates = con.execute(
            """
            SELECT count(*), count(*) - count(DISTINCT lower(city) || '|' || lower(hotel_name))
            FROM raw.hotel_listings
            """
        ).fetchone()
    return AssetCheckResult(
        passed=total > 0 and duplicates == 0, metadata={"rows": total, "duplicates": duplicates}
    )


raw_assets = [raw_flight_fare_quotes, raw_hotel_booking_events, raw_hotel_listings]
raw_checks = [
    flight_quotes_reconcile,
    booking_events_reconcile,
    booking_events_consistent,
    listings_unique,
]
