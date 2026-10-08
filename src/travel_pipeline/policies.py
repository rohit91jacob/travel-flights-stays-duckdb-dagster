"""Shared operational policies: concurrency, retries, freshness."""

from datetime import timedelta

from dagster import Backoff, FreshnessPolicy, Jitter, RetryPolicy

# DuckDB allows one read-write process per database file, and a writer blocks readers in
# other processes. Every step that opens the warehouse claims a slot in this pool; the
# instance config (dagster_home/dagster.yaml, docker/dagster.yaml) caps pools at 1.
DUCKDB_POOL = "duckdb"

# Run-level guard on top of the op-level pool: runs carrying this tag are de-queued one at
# a time (concurrency.runs.tag_concurrency_limits in the instance config).
DUCKDB_RUN_TAGS = {"travel/warehouse": "duckdb"}

# Network calls to Kaggle are the only flaky step; retry them with backoff.
KAGGLE_RETRY = RetryPolicy(
    max_retries=3, delay=20, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
)

# Daily feed: warn after a missed day, fail after two.
DAILY_FRESHNESS = FreshnessPolicy.time_window(
    fail_window=timedelta(hours=50), warn_window=timedelta(hours=26)
)
