from dagster import AssetKey

from travel_pipeline.definitions import defs
from travel_pipeline.policies import DUCKDB_POOL


def _repository():
    return defs.get_repository_def()


def test_definitions_load_with_expected_jobs_schedules_and_sensors() -> None:
    repo = _repository()
    assert {"ingest_job", "flights_load_job", "stays_load_job", "transform_job"} <= set(
        repo.job_names
    )
    assert {s.name for s in repo.schedule_defs} == {
        "flights_replay_schedule",
        "stays_replay_schedule",
        "ingest_weekly_schedule",
    }
    assert {s.name for s in repo.sensor_defs} == {
        "transform_on_raw_update",
        "reload_on_new_dataset_version",
        "kaggle_upstream_version_sensor",
        "alert_on_run_failure",
    }


def test_dbt_sources_are_wired_to_raw_assets() -> None:
    graph = _repository().asset_graph
    for raw in ("flight_fare_quotes", "hotel_booking_events", "hotel_listings"):
        children = graph.get(AssetKey(["raw", raw])).child_keys
        assert children, f"raw/{raw} has no downstream dbt models"
    assert (
        AssetKey(["marts", "fct_flight_fare_quotes"])
        in graph.get(AssetKey(["staging", "stg_flights__fare_quotes"])).child_keys
    )


def test_every_warehouse_step_uses_the_duckdb_pool() -> None:
    for assets_def in _repository().assets_defs_by_key.values():
        keys = {k.to_user_string() for k in assets_def.keys}
        if any(k.startswith("landing/") for k in keys):
            continue
        assert assets_def.op.pool == DUCKDB_POOL, f"{keys} must claim the {DUCKDB_POOL} pool"


def test_dbt_tests_are_asset_checks_and_marts_have_freshness() -> None:
    graph = _repository().asset_graph
    check_keys = graph.asset_check_keys
    assert any(c.asset_key == AssetKey(["marts", "fct_hotel_bookings"]) for c in check_keys)
    spec = graph.get(AssetKey(["marts", "mart_stays_seasonality"]))
    assert spec.group_name == "marts_stays"
    assert spec.freshness_policy is not None
