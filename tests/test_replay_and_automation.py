from datetime import UTC, date, datetime

import pytest
import requests
from dagster import RunRequest, SkipReason, build_schedule_context

from travel_pipeline.definitions import defs
from travel_pipeline.partitions import FLIGHT_PARTITIONS, STAY_EVENT_PARTITIONS
from travel_pipeline.replay import due_partition_window
from travel_pipeline.resources import AlertNotifier
from travel_pipeline.schedules import (
    PARTITION_RANGE_END_TAG,
    PARTITION_RANGE_START_TAG,
    flights_replay_schedule,
    stays_replay_schedule,
)

KEYS = ["2022-02-11", "2022-02-12", "2022-02-13", "2022-02-14", "2022-02-15"]


@pytest.mark.parametrize(
    ("today", "per_tick", "expected"),
    [
        (date(2026, 1, 1), 1, ("2022-02-11", "2022-02-11")),
        (date(2026, 1, 3), 1, ("2022-02-13", "2022-02-13")),
        (date(2026, 1, 2), 2, ("2022-02-13", "2022-02-14")),
        (date(2026, 1, 3), 2, ("2022-02-15", "2022-02-15")),
        (date(2026, 1, 4), 2, None),
        (date(2025, 12, 31), 1, None),
    ],
)
def test_due_partition_window(today: date, per_tick: int, expected: tuple[str, str] | None) -> None:
    assert (
        due_partition_window(KEYS, anchor=date(2026, 1, 1), today=today, per_tick=per_tick)
        == expected
    )


def test_due_partition_window_rejects_bad_tick() -> None:
    with pytest.raises(ValueError, match="per_tick"):
        due_partition_window(KEYS, anchor=date(2026, 1, 1), today=date(2026, 1, 1), per_tick=0)


def test_partition_bounds_cover_the_datasets() -> None:
    flights = FLIGHT_PARTITIONS.get_partition_keys()
    stays = STAY_EVENT_PARTITIONS.get_partition_keys()
    assert (flights[0], flights[-1], len(flights)) == ("2022-02-11", "2022-03-31", 49)
    assert (stays[0], stays[-1]) == ("2013-06-24", "2017-09-14")


def test_replay_schedule_is_disabled_without_anchor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRAVEL_FLIGHTS_REPLAY_START", raising=False)
    result = flights_replay_schedule.evaluate_tick(
        build_schedule_context(scheduled_execution_time=datetime(2026, 1, 1, 6, tzinfo=UTC))
    )
    assert result.skip_message and "not set" in result.skip_message


def test_replay_schedule_requests_single_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAVEL_FLIGHTS_REPLAY_START", "2026-01-01")
    monkeypatch.setenv("TRAVEL_FLIGHTS_REPLAY_PARTITIONS_PER_DAY", "1")
    context = build_schedule_context(
        scheduled_execution_time=datetime(2026, 1, 3, 6, tzinfo=UTC),
        repository_def=defs.get_repository_def(),
    )
    [request] = flights_replay_schedule.evaluate_tick(context).run_requests
    assert isinstance(request, RunRequest)
    assert request.partition_key == "2022-02-13"


def test_replay_schedule_requests_a_range_as_one_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAVEL_STAYS_REPLAY_START", "2026-01-01")
    monkeypatch.setenv("TRAVEL_STAYS_REPLAY_PARTITIONS_PER_DAY", "7")
    context = build_schedule_context(scheduled_execution_time=datetime(2026, 1, 2, 6, tzinfo=UTC))
    [request] = stays_replay_schedule.evaluate_tick(context).run_requests
    assert request.tags[PARTITION_RANGE_START_TAG] == "2013-07-01"
    assert request.tags[PARTITION_RANGE_END_TAG] == "2013-07-07"


def test_replay_schedule_stops_after_history(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAVEL_FLIGHTS_REPLAY_START", "2026-01-01")
    context = build_schedule_context(scheduled_execution_time=datetime(2026, 6, 1, 6, tzinfo=UTC))
    result = flights_replay_schedule.evaluate_tick(context)
    assert not result.run_requests
    assert "complete" in (result.skip_message or "")


class _Response:
    def __init__(self, status: int) -> None:
        self.status_code = status

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


def test_alert_notifier_posts_slack_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, dict]] = []

    def fake_post(url: str, json: dict, timeout: float) -> _Response:
        sent.append((url, json))
        return _Response(200)

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.invalid/T000")
    assert AlertNotifier().notify("run failed") is True
    assert sent == [("https://hooks.example.invalid/T000", {"text": "run failed"})]


def test_alert_notifier_without_url_only_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert AlertNotifier().notify("run failed") is False


def test_alert_notifier_swallows_delivery_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(requests, "post", lambda *a, **k: _Response(500))
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.invalid/T000")
    assert AlertNotifier().notify("run failed") is False


def test_skip_reason_type_is_public() -> None:
    # Guards the public API the schedules rely on.
    assert SkipReason("x").skip_message == "x"
