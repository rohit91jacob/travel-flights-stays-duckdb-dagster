"""Jobs: ingest (landing + listings), the two partitioned loads, and the dbt transform."""

from dagster import AssetSelection, define_asset_job

from travel_pipeline.assets.dbt import travel_dbt
from travel_pipeline.assets.raw import RAW_BOOKING_EVENTS, RAW_FLIGHT_QUOTES, RAW_LISTINGS
from travel_pipeline.policies import DUCKDB_RUN_TAGS

ingest_job = define_asset_job(
    name="ingest_job",
    description="Download new upstream versions into the landing zone and reload the listings snapshot.",
    selection=AssetSelection.groups("landing") | AssetSelection.assets(RAW_LISTINGS),
    tags=DUCKDB_RUN_TAGS,
)

flights_load_job = define_asset_job(
    name="flights_load_job",
    description="Load fare quotes for one or more departure dates (partition range = one run).",
    selection=AssetSelection.assets(RAW_FLIGHT_QUOTES),
    tags=DUCKDB_RUN_TAGS,
)

stays_load_job = define_asset_job(
    name="stays_load_job",
    description="Load hotel booking events for one or more event dates (partition range = one run).",
    selection=AssetSelection.assets(RAW_BOOKING_EVENTS),
    tags=DUCKDB_RUN_TAGS,
)

transform_job = define_asset_job(
    name="transform_job",
    description="dbt source freshness + dbt build over all models, snapshots, seeds and tests.",
    selection=AssetSelection.assets(travel_dbt),
    tags=DUCKDB_RUN_TAGS,
)

all_jobs = [ingest_job, flights_load_job, stays_load_job, transform_job]
