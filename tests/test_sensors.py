from dagster import AssetMaterialization, build_sensor_context, instance_for_test

from travel_pipeline.assets.raw import RAW_BOOKING_EVENTS, RAW_FLIGHT_QUOTES, RAW_LISTINGS
from travel_pipeline.sensors import transform_on_raw_update


def _materialize(instance, key, partition=None) -> None:
    instance.report_runless_asset_event(AssetMaterialization(asset_key=key, partition=partition))


def test_transform_waits_for_every_raw_source_then_coalesces_bursts() -> None:
    with instance_for_test() as instance:
        _materialize(instance, RAW_LISTINGS)
        result = transform_on_raw_update(build_sensor_context(instance=instance))
        assert "waiting for the first load" in result.skip_message

        _materialize(instance, RAW_FLIGHT_QUOTES, "2022-02-11")
        _materialize(instance, RAW_BOOKING_EVENTS, "2016-01-01")
        _materialize(instance, RAW_BOOKING_EVENTS, "2016-01-02")
        first = transform_on_raw_update(build_sensor_context(instance=instance))
        assert len(first.run_requests) == 1

        idle = transform_on_raw_update(build_sensor_context(instance=instance, cursor=first.cursor))
        assert idle.skip_message == "no new raw materializations"

        _materialize(instance, RAW_FLIGHT_QUOTES, "2022-02-12")
        again = transform_on_raw_update(
            build_sensor_context(instance=instance, cursor=first.cursor)
        )
        assert len(again.run_requests) == 1
        assert again.run_requests[0].run_key != first.run_requests[0].run_key


def test_transform_holds_while_a_load_is_running() -> None:
    from dagster import DagsterRun, DagsterRunStatus  # noqa: PLC0415

    with instance_for_test() as instance:
        for key, partition in (
            (RAW_LISTINGS, None),
            (RAW_FLIGHT_QUOTES, "2022-02-11"),
            (RAW_BOOKING_EVENTS, "2016-01-01"),
        ):
            _materialize(instance, key, partition)
        instance.add_run(
            DagsterRun(job_name="stays_load_job", run_id="a" * 36, status=DagsterRunStatus.STARTED)
        )

        held = transform_on_raw_update(build_sensor_context(instance=instance))
        assert "stays_load_job" in held.skip_message

        instance.delete_run("a" * 36)
        assert (
            len(transform_on_raw_update(build_sensor_context(instance=instance)).run_requests) == 1
        )
