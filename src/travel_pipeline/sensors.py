"""Sensors: chain the loads into dbt, refresh on new upstream versions, alert on failure."""

import json
import os

from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    run_failure_sensor,
    sensor,
)

from travel_pipeline.assets.landing import landing_key
from travel_pipeline.assets.raw import RAW_ASSET_KEYS
from travel_pipeline.datasets import ALL_DATASETS, FLIGHTS, HOTEL_BOOKINGS
from travel_pipeline.jobs import flights_load_job, ingest_job, stays_load_job, transform_job
from travel_pipeline.partitions import FLIGHT_PARTITIONS, STAY_EVENT_PARTITIONS
from travel_pipeline.resources import AlertNotifier, LandingZone
from travel_pipeline.schedules import partition_range_tags

_IN_PROGRESS = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]
_LOAD_JOBS = (ingest_job, flights_load_job, stays_load_job)


def _loads_in_progress(context: SensorEvaluationContext) -> list[str]:
    return [
        job.name
        for job in _LOAD_JOBS
        if context.instance.get_runs(RunsFilter(job_name=job.name, statuses=_IN_PROGRESS), limit=1)
    ]


def _latest_storage_id(context: SensorEvaluationContext, key: AssetKey) -> int | None:
    records = context.instance.fetch_materializations(key, limit=1).records
    return records[0].storage_id if records else None


@sensor(
    name="transform_on_raw_update",
    job=transform_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Runs dbt once after any raw asset gets new materializations (bursts coalesce into one "
        "run), but only once every raw source has been loaded at least once."
    ),
)
def transform_on_raw_update(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}
    latest = {
        key.to_user_string(): storage_id
        for key in RAW_ASSET_KEYS
        if (storage_id := _latest_storage_id(context, key)) is not None
    }
    missing = [k.to_user_string() for k in RAW_ASSET_KEYS if k.to_user_string() not in latest]
    if missing:
        # dbt would fail on absent source tables; wait for the first load of each.
        return SkipReason(f"waiting for the first load of {', '.join(missing)}")
    if not any(storage_id > cursor.get(key, 0) for key, storage_id in latest.items()):
        return SkipReason("no new raw materializations")
    if active := _loads_in_progress(context):
        # Cursor not advanced: one transform runs after the loads finish, not one per tick.
        return SkipReason(f"waiting for in-progress loads: {', '.join(active)}")
    return SensorResult(
        run_requests=[RunRequest(run_key=f"transform:{max(latest.values())}")],
        cursor=json.dumps(latest),
    )


_RELOADABLE = (
    (FLIGHTS, flights_load_job, FLIGHT_PARTITIONS, AssetKey(["raw", "flight_fare_quotes"])),
    (
        HOTEL_BOOKINGS,
        stays_load_job,
        STAY_EVENT_PARTITIONS,
        AssetKey(["raw", "hotel_booking_events"]),
    ),
)


@sensor(
    name="reload_on_new_dataset_version",
    jobs=[flights_load_job, stays_load_job],
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "When a landing asset lands a new upstream version, reload every partition that was "
        "already loaded from the old version (one single-run backfill per dataset)."
    ),
)
def reload_on_new_dataset_version(context: SensorEvaluationContext) -> SensorResult | SkipReason:
    cursor: dict[str, int] = json.loads(context.cursor) if context.cursor else {}
    run_requests = []
    for dataset, job, partitions_def, raw_key in _RELOADABLE:
        records = context.instance.fetch_materializations(landing_key(dataset), limit=1).records
        if not records or records[0].storage_id <= cursor.get(dataset.key, 0):
            continue
        record = records[0]
        cursor[dataset.key] = record.storage_id
        metadata = record.asset_materialization.metadata if record.asset_materialization else {}
        changed = metadata.get("changed")
        if not (changed and changed.value):
            continue
        loaded = context.instance.get_materialized_partitions(raw_key)
        if not loaded:
            continue  # nothing loaded yet: new partitions arrive via backfill or the replay clock
        keys = [k for k in partitions_def.get_partition_keys() if k in loaded]
        run_requests.append(
            RunRequest(
                job_name=job.name,
                run_key=f"reload:{dataset.key}:{metadata['version_id'].value}",
                tags=partition_range_tags(keys[0], keys[-1]),
            )
        )
    if not run_requests:
        return SensorResult(
            skip_reason="no new dataset versions to reload", cursor=json.dumps(cursor)
        )
    return SensorResult(run_requests=run_requests, cursor=json.dumps(cursor))


@sensor(
    name="kaggle_upstream_version_sensor",
    job=ingest_job,
    minimum_interval_seconds=6 * 60 * 60,
    default_status=DefaultSensorStatus.STOPPED,
    description="Polls Kaggle every 6 hours and runs ingest_job when a dataset has a new version.",
)
def kaggle_upstream_version_sensor(
    context: SensorEvaluationContext, landing: LandingZone
) -> RunRequest | SkipReason:
    source = landing.source()
    outdated = []
    for dataset in ALL_DATASETS:
        upstream = source.upstream_version(dataset)
        current = landing.current_or_none(dataset)
        if current is None or (upstream is not None and current[0].upstream_version != upstream):
            outdated.append(f"{dataset.key}@{upstream}")
    if not outdated:
        return SkipReason("every dataset is at its latest upstream version")
    context.log.info("outdated datasets: %s", outdated)
    return RunRequest(run_key="ingest:" + ",".join(sorted(outdated)))


@run_failure_sensor(
    name="alert_on_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
    description="Posts every failed run (including failed blocking checks) to ALERT_WEBHOOK_URL.",
)
def alert_on_run_failure(context: RunFailureSensorContext, alerts: AlertNotifier) -> None:
    run = context.dagster_run
    base_url = os.environ.get("DAGSTER_WEBSERVER_URL", "http://localhost:3000").rstrip("/")
    message = context.failure_event.message or "no error message"
    alerts.notify(
        f":rotating_light: Dagster run failed: job `{run.job_name}` "
        f"({run.tags.get('dagster/partition', run.tags.get('dagster/asset_partition_range_start', 'unpartitioned'))})\n"
        f"{message[:500]}\n{base_url}/runs/{run.run_id}"
    )


all_sensors = [
    transform_on_raw_update,
    reload_on_new_dataset_version,
    kaggle_upstream_version_sensor,
    alert_on_run_failure,
]
