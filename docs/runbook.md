# Runbook

Operational procedures for the travel pipeline. Commands assume the repository root, `uv`
installed, and `DAGSTER_HOME` pointing at a Dagster instance (`make` sets it to
`./dagster_home`). In Docker Compose, run the same `dagster` commands inside the
`code-location` container (`docker compose exec code-location ...`) or use the UI.

## How the pipeline runs

| Piece | What it does | Default |
| --- | --- | --- |
| `ingest_job` | Lands new Kaggle versions (skips when unchanged) and reloads the listings snapshot | manual / `ingest_weekly_schedule` (off) / `kaggle_upstream_version_sensor` (off) |
| `flights_load_job`, `stays_load_job` | Replace-window loads of one or more daily partitions in a single run | manual, backfill, or the replay schedules (off until a start date is set) |
| `transform_on_raw_update` sensor | Runs `transform_job` (dbt source freshness + `dbt build`) once all raw sources exist and any of them changed | on |
| `reload_on_new_dataset_version` sensor | After a new upstream version lands, reloads every partition already loaded | on |
| `alert_on_run_failure` sensor | Posts failed runs to `ALERT_WEBHOOK_URL` | on |

DuckDB allows a single writer, so every step that opens the warehouse claims the `duckdb`
pool (limit 1) and every job carries the `travel/warehouse=duckdb` tag (one such run at a
time). Runs queue instead of failing on a lock.

## Routine operations

**First load / full rebuild.** `make pipeline` (ingest, all flights partitions, all stays
partitions, transform). Or in the UI: materialize `landing/*`, then launch backfills of
`raw/flight_fare_quotes` and `raw/hotel_booking_events` (single-run backfills); the
transform follows automatically.

**Daily feed (replay).** Set `TRAVEL_FLIGHTS_REPLAY_START` / `TRAVEL_STAYS_REPLAY_START`
to the real date the replay should start, and optionally the `*_PARTITIONS_PER_DAY`
variables, then turn on `flights_replay_schedule` / `stays_replay_schedule`. Each 06:00 UTC
tick loads the next partitions; the transform follows. The schedules skip once the history
is exhausted.

**Reprocess a date range.** Re-run the load job for the range, for example
`uv run dagster job execute -m travel_pipeline.definitions -j stays_load_job --tags
'{"dagster/asset_partition_range_start": "2016-01-01", "dagster/asset_partition_range_end": "2016-01-31"}'`.
Loads delete and re-insert the window in one transaction, so reruns are safe. The
incremental fact `fct_flight_fare_quotes` rebuilds exactly the departure dates whose rows
were reloaded.

**Full dbt refresh** (after changing an incremental model or contract):
`cd dbt && uv run dbt build --profiles-dir . --full-refresh`.

## New upstream dataset version

1. `ingest_job` lands it into a new content-addressed directory and moves `CURRENT`
   (old versions are kept, never modified).
2. `reload_on_new_dataset_version` reloads every partition that was already loaded.
3. If the new version contains dates outside the partition definitions,
   `reconciles_with_landing` fails with `landed_dates_outside_partitions`. Extend the bounds
   in `src/travel_pipeline/partitions.py`, deploy, and backfill the new partitions.
4. Schema changes fail loudly with `SchemaDriftError` (header or raw-table mismatch).
   Update `ingestion/schemas.py` and the dbt staging model, then drop the affected
   `raw.*` table and reload all its partitions.

To roll back to a previous version, write its directory name into
`data/landing/<dataset>/CURRENT` and reload the partitions.

## Failure triage

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `reconciles_with_landing` failed (`dates_with_count_mismatch`) | partial load or a version changed under a partition | reload the listed dates |
| `event_pairs_consistent` failed | duplicate or out-of-order booking events | reload the whole stays range |
| `files_match_manifest` failed | landing zone modified by hand / disk corruption | restore the version directory or re-ingest |
| transform fails in `dbt source freshness` | no raw loads for more than 50 hours | load the missing partitions, or set `TRAVEL_ENFORCE_SOURCE_FRESHNESS=false` for a one-off rebuild |
| dbt test failure in `dbt build` | data quality regression | open the run's dbt log (path in the error), query the failing model |
| `IO Error: Could not set lock on file` | a process outside Dagster holds the DuckDB file | close the other client (DuckDB CLI, notebook) and re-run |
| Kaggle 401/403 | expired or missing token | rotate `KAGGLE_API_TOKEN` / `~/.kaggle/access_token` |

Warn-level dbt tests (`assert_cancellation_rate_in_expected_band`,
`assert_economy_fares_in_expected_band`) do not fail runs; they flag upstream drift on the
asset check page.

## Backups and recovery

- `data/landing/` is the system of record: everything else can be rebuilt from it with
  `make pipeline` (Docker: the `travel-data` volume).
- `data/warehouse/travel.duckdb` is derived; copy it while no run is active for a
  point-in-time backup.
- Dagster run history lives in Postgres (Compose: the `dagster-postgres` volume).
