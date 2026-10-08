"""Replay clock: turn a static history into a daily feed.

On ``anchor`` (a real calendar date) the first ``per_tick`` partitions are due; every day
after that releases the next ``per_tick`` partitions, until the history is exhausted.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date


def due_partition_window(
    partition_keys: Sequence[str], *, anchor: date, today: date, per_tick: int
) -> tuple[str, str] | None:
    """Return the inclusive (first, last) partition keys due on ``today``, or None."""
    if per_tick < 1:
        raise ValueError("per_tick must be >= 1")
    if today < anchor:
        return None
    start = (today - anchor).days * per_tick
    if start >= len(partition_keys):
        return None
    end = min(start + per_tick, len(partition_keys)) - 1
    return partition_keys[start], partition_keys[end]
