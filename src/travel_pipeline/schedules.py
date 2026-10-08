"""Schedules: the replay clocks for the two partitioned feeds and a weekly ingest."""

import os
from datetime import UTC, date, datetime

from dagster import (
    DefaultScheduleStatus,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    TimeWindowPartitionsDefinition,
    schedule,
)
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition

from travel_pipeline.jobs import flights_load_job, ingest_job, stays_load_job
from travel_pipeline.partitions import FLIGHT_PARTITIONS, STAY_EVENT_PARTITIONS
from travel_pipeline.replay import due_partition_window

# Run tags that make a run cover a partition range (all raw assets use single-run backfills).
PARTITION_RANGE_START_TAG = "dagster/asset_partition_range_start"
PARTITION_RANGE_END_TAG = "dagster/asset_partition_range_end"


def partition_range_tags(first: str, last: str) -> dict[str, str]:
    return {PARTITION_RANGE_START_TAG: first, PARTITION_RANGE_END_TAG: last}


def build_replay_schedule(
    *,
    name: str,
    job: UnresolvedAssetJobDefinition,
    partitions_def: TimeWindowPartitionsDefinition,
    anchor_env: str,
    per_tick_env: str,
    default_per_tick: int,
) -> ScheduleDefinition:
    @schedule(
        name=name,
        job=job,
        cron_schedule="0 6 * * *",
        execution_timezone="UTC",
        default_status=DefaultScheduleStatus.STOPPED,
        description=(
            f"Replay clock: from the date in {anchor_env}, load the next {per_tick_env} "
            f"(default {default_per_tick}) partitions every day at 06:00 UTC."
        ),
    )
    def _replay(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
        anchor_raw = os.environ.get(anchor_env, "").strip()
        if not anchor_raw:
            return SkipReason(f"{anchor_env} is not set; replay is disabled")
        per_tick = int(os.environ.get(per_tick_env, str(default_per_tick)))
        tick_time = context.scheduled_execution_time or datetime.now(UTC)
        window = due_partition_window(
            partitions_def.get_partition_keys(),
            anchor=date.fromisoformat(anchor_raw),
            today=tick_time.date(),
            per_tick=per_tick,
        )
        if window is None:
            return SkipReason("no partitions due: replay has not started yet or is complete")
        first, last = window
        if first == last:
            return RunRequest(run_key=f"{name}:{first}", partition_key=first)
        return RunRequest(
            run_key=f"{name}:{first}...{last}", tags=partition_range_tags(first, last)
        )

    return _replay


flights_replay_schedule = build_replay_schedule(
    name="flights_replay_schedule",
    job=flights_load_job,
    partitions_def=FLIGHT_PARTITIONS,
    anchor_env="TRAVEL_FLIGHTS_REPLAY_START",
    per_tick_env="TRAVEL_FLIGHTS_REPLAY_PARTITIONS_PER_DAY",
    default_per_tick=1,
)

stays_replay_schedule = build_replay_schedule(
    name="stays_replay_schedule",
    job=stays_load_job,
    partitions_def=STAY_EVENT_PARTITIONS,
    anchor_env="TRAVEL_STAYS_REPLAY_START",
    per_tick_env="TRAVEL_STAYS_REPLAY_PARTITIONS_PER_DAY",
    default_per_tick=7,
)

ingest_weekly_schedule = ScheduleDefinition(
    name="ingest_weekly_schedule",
    job=ingest_job,
    cron_schedule="0 5 * * 1",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Weekly safety net: check Kaggle for new dataset versions (no-op when unchanged).",
)

all_schedules = [flights_replay_schedule, stays_replay_schedule, ingest_weekly_schedule]
