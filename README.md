# Travel analytics: flights + stays on DuckDB, dbt and Dagster

[![CI](https://github.com/rohit91jacob/travel-flights-stays-duckdb-dagster/actions/workflows/ci.yml/badge.svg)](https://github.com/rohit91jacob/travel-flights-stays-duckdb-dagster/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](pyproject.toml)

An end-to-end, production-style data pipeline over three public Kaggle datasets: Indian
domestic **flight fares**, Portuguese **hotel booking demand**, and Indian **hotel listings**.
Dagster lands the files in an immutable, checksummed landing zone, loads them into DuckDB
as daily partitions that replay the static history as a day-by-day feed, and runs dbt to
build tested, contract-enforced marts: fare curves, route/airline comparisons, class
premiums, booking pace, cancellation drivers, ADR/LOS, revenue proxies and a cross-subject
trip-cost benchmark.

Everything runs on a laptop (no cloud account), and the same image runs under Docker
Compose with Postgres-backed Dagster.

## Contents

1. [Architecture](#architecture)
2. [Tech stack](#tech-stack)
3. [Data sources](#data-sources)
4. [Data model](#data-model)
5. [Quickstart](#quickstart)
6. [Configuration](#configuration)
7. [Testing and CI](#testing-and-ci)
8. [Operations](#operations)
9. [Project structure](#project-structure)
10. [Design rationale, limitations, roadmap](#design-rationale)
11. [License](#license)

## Architecture

```mermaid
flowchart LR
    subgraph Kaggle
        K1[flight-price-prediction<br/>economy.csv, business.csv]
        K2[hotel-booking-demand<br/>hotel_bookings.csv]
        K3[MakeMyTrip city hotels<br/>6 city CSVs]
    end

    subgraph Landing["Landing zone (immutable)"]
        L["landing/&lt;dataset&gt;/versions/sha256-…<br/>+ manifest.json + CURRENT"]
    end

    subgraph DuckDB
        R1["raw.flight_fare_quotes<br/>daily partitions: departure date"]
        R2["raw.hotel_booking_events<br/>daily partitions: event date"]
        R3[raw.hotel_listings<br/>snapshot]
        S[staging views]
        I[intermediate tables]
        SN[(snapshots.snap_hotel_listings<br/>SCD2)]
        M[marts: dims, facts, marts<br/>contracts enforced]
    end

    K1 & K2 & K3 -->|landing assets<br/>retry + checksum| L
    L -->|replace-window loads<br/>explicit schemas| R1 & R2 & R3
    R1 & R2 & R3 --> S --> I --> M
    S --> SN --> M

    D{{Dagster daemon<br/>schedules · sensors · run queue · pools}} -.-> L & R1 & R2 & M
    M --> BI[BI / notebooks / DuckDB CLI]
```

| Component | Responsibility | Where |
| --- | --- | --- |
| Landing assets | Download each Kaggle dataset (skipped when the upstream version is unchanged), store it in a content-addressed, never-modified version directory with a sha256 manifest | `src/travel_pipeline/assets/landing.py`, `ingestion/manifest.py` |
| Raw assets | Validate headers against declared schemas, cast with strict types, and replace one window of daily partitions per run inside one DuckDB transaction | `assets/raw.py`, `ingestion/loaders.py`, `ingestion/schemas.py` |
| dbt project | Staging → intermediate → marts; seeds, an SCD2 snapshot, an incremental fact, 120+ tests, unit tests, contracts, source freshness | `dbt/` |
| Dagster definitions | Jobs, replay schedules, sensors (transform chaining, version reloads, Kaggle polling, failure alerts), asset checks, freshness policies, concurrency pools | `src/travel_pipeline/` |
| Deployment | One image; Compose runs Postgres, the gRPC code location (runs execute here), webserver and daemon | `Dockerfile`, `docker-compose.yml`, `docker/` |

## Tech stack

| Tool | Version | Role |
| --- | --- | --- |
| Python | 3.12 | runtime |
| [uv](https://docs.astral.sh/uv/) | 0.12.23 | dependency management (`uv.lock`) |
| Dagster | 1.13.25 (`dagster-dbt` 0.29.25, `dagster-postgres` 0.29.25) | orchestration |
| dbt-core | 1.12.5 with `dbt-duckdb` 1.11.0 | transformations, tests, docs |
| DuckDB | 1.5.6 | warehouse (single file) |
| pyarrow | 25.0.1 | CSV reading with stable row numbers |
| kaggle | 2.2.4 | downloads (new-style `KGAT_` tokens) |
| PostgreSQL | 17 (Compose only; SQLAlchemy pinned to 2.0.x for the psycopg2 driver) | Dagster run/event storage |
| ruff 0.16.10, sqlfluff 4.4.0, pytest 9.1.1 | | quality gates |

## Data sources

| Dataset | Kaggle ref | Licence | Rows | What it is | Refresh |
| --- | --- | --- | --- | --- | --- |
| Flight fares | [`shubhambathwal/flight-price-prediction`](https://www.kaggle.com/datasets/shubhambathwal/flight-price-prediction) | CC0 1.0 | 300,261 (206,774 economy + 93,487 business) | EaseMyTrip fares for the 30 routes between 6 Indian metros, departures 2022-02-11 to 2022-03-31 | static upstream (v1); checked weekly |
| Hotel booking demand | [`jessemostipak/hotel-booking-demand`](https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand) | CC BY 4.0 | 119,390 bookings | Two Portuguese hotels (Algarve resort, Lisbon city), arrivals 2015-07-01 to 2017-08-31 | static upstream (v1); checked weekly |
| City hotel listings | [`andrewgeorgeissac/hotel-price-data-of-cities-in-india-makemytrip`](https://www.kaggle.com/datasets/andrewgeorgeissac/hotel-price-data-of-cities-in-india-makemytrip) | CDLA-Sharing 1.0 | 580 listings | MakeMyTrip listings for the same 6 metros as the flights (published 2023-08-19) | static upstream; checked weekly |

Attribution: Antonio, N., de Almeida, A. & Nunes, L. (2019), *Hotel booking demand datasets*,
Data in Brief 22, 41–49, [doi:10.1016/j.dib.2018.11.126](https://doi.org/10.1016/j.dib.2018.11.126);
Shubham Bathwal (Kaggle); Andrew George Issac (Kaggle). **No Kaggle data is committed.** CI and
the test suite use small synthetic files in `tests/fixtures/kaggle/` that mimic the real
schemas and quirks (generated by `scripts/generate_fixtures.py`).

Two facts about the data shape the design:

* **Every fare was observed on one day, 2022-02-10.** For every row, `days_left` =
  departure date − 2022-02-10, so the only time axis is the departure date. Flights are
  partitioned by departure date, and the fare curve is a cross-section of departures,
  not one flight getting dearer over time.
* **The two stays datasets do not share a geography.** The booking data is Portuguese. The
  listings cover the same six Indian cities as the flights, so only listings and flights
  are joined, through a conformed `dim_city`. Bookings stay a separate subject area that
  shares only `dim_date` and `dim_currency` (EUR vs INR, never converted).

## Data model

Full column-level documentation: **[docs/data_dictionary.md](docs/data_dictionary.md)**
(generated from the dbt YAML). Schemas in the DuckDB file: `raw`, `staging`,
`intermediate`, `snapshots`, `reference` (seeds), `marts`.

| Model | Grain | Key |
| --- | --- | --- |
| `dim_date`, `dim_currency`, `dim_city` | day / currency / metro (conformed) | `date_key`, `currency_code`, `city_name` |
| `dim_airline`, `dim_route` | carrier / directed route | `carrier_code`, `route_code` |
| `fct_flight_fare_quotes` (incremental) | one itinerary option × cabin class × departure date | `fare_quote_id` |
| `mart_flight_fare_curve` | route (or ALL) × class × days to departure | composite |
| `mart_flight_route_airline_fares`, `mart_flight_class_premium` | route × airline (× class) | composite |
| `mart_flight_stops_duration`, `mart_flight_departure_time` | class × stops × duration band / class × time band | composite |
| `dim_hotel`, `fct_hotel_bookings` | hotel / booking (status as of the replay date) | `hotel_code`, `booking_id` |
| `dim_hotel_booking_status_history` | booking status version (SCD2 from events) | `booking_status_version_id` |
| `mart_stays_booking_pace`, `_cancellation_drivers`, `_adr_los`, `_segment_channel_mix`, `_seasonality`, `_revenue_proxies` | see dictionary | composite |
| `dim_hotel_listing` (from snapshot), `mart_city_stay_prices` | listing / city × star band | `listing_id` |
| `mart_city_trip_cost_benchmark` | city | `city` |

## Quickstart

### Prerequisites

* [uv](https://docs.astral.sh/uv/getting-started/installation/) (it installs Python 3.12 for you), `make`, `git`
* For real data: a Kaggle API token in `KAGGLE_API_TOKEN` or `~/.kaggle/access_token`
* For Compose: Docker with Compose v2

### Run locally on the synthetic fixtures (no Kaggle account needed)

```bash
git clone https://github.com/rohit91jacob/travel-flights-stays-duckdb-dagster.git
cd travel-flights-stays-duckdb-dagster
make install
export TRAVEL_SOURCE_MODE=local TRAVEL_LOCAL_SOURCE_DIR=$PWD/tests/fixtures/kaggle DBT_TARGET=ci
make dbt-parse
make pipeline        # ingest -> load all flights partitions -> load all stays partitions -> transform
```

`make pipeline` ends with dbt's summary and a successful `transform_job` run:

```text
Done. PASS=159 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=159
... transform_job - ... - RUN_SUCCESS - Finished execution of run for "transform_job".
```

### Run on the real Kaggle data

```bash
export KAGGLE_API_TOKEN=...            # or save it to ~/.kaggle/access_token
make dbt-parse
make pipeline                           # downloads ~55 MB from Kaggle on the first run
make dev                                # UI + daemon on http://localhost:3000
```

Query the marts with any DuckDB client while no run is active, for example:

```bash
uv run python -c "import duckdb; print(duckdb.connect('data/warehouse/travel.duckdb', read_only=True).sql(
  'select hotel_code, round(avg(is_canceled::int), 4) as cancellation_rate from marts.fct_hotel_bookings group by 1 order by 1'))"
```

```text
┌────────────┬───────────────────┐
│ hotel_code │ cancellation_rate │
├────────────┼───────────────────┤
│ H1         │            0.2776 │
│ H2         │            0.4173 │
└────────────┴───────────────────┘
```

Results from the real data, verified locally both with `make pipeline` and through
`dagster dev`, where the daemon's sensor triggered the transform:

| Check | Result |
| --- | --- |
| Fare quotes loaded / departure dates | 300,261 / 49 (2022-02-11 to 2022-03-31); 4 malformed durations flagged, not dropped |
| Mean fare, business / economy | ₹52,540.08 / ₹6,571.19 (Kaggle's `Clean_Dataset.csv`: ₹52,540.08 / ₹6,572.34; it drops 108 economy rows) |
| Bookings / cancelled | 119,390 / 44,224 = 37.04% (City Hotel 41.73%, Resort Hotel 27.76%) |
| Non-refundable deposits | 99.36% cancel |
| Asset checks evaluated | 343, all passed |
| Rerun of ingest + one flights day + one month of stays | all 27 warehouse tables byte-identical |

### Docker Compose (verified in CI only)

The Compose stack is exercised by the `docker` CI job; it has not been run on the
author's machine (no local Docker).

```bash
cp .env.example .env      # set DAGSTER_POSTGRES_PASSWORD and KAGGLE_API_TOKEN
make docker-build
make docker-up            # http://localhost:3000
make docker-down
```

## Configuration

All configuration is environment variables (see [`.env.example`](.env.example)).

| Variable | Default | Purpose |
| --- | --- | --- |
| `TRAVEL_SOURCE_MODE` | `kaggle` | `kaggle` downloads via the API; `local` copies from `TRAVEL_LOCAL_SOURCE_DIR/<dataset>/` |
| `TRAVEL_LOCAL_SOURCE_DIR` | – | Directory for `local` mode (CI uses `tests/fixtures/kaggle`) |
| `KAGGLE_API_TOKEN` | – | Kaggle token (alternatively `~/.kaggle/access_token`) |
| `TRAVEL_DATA_DIR` | `./data` | Root for the landing zone and warehouse |
| `TRAVEL_DUCKDB_PATH` | `$TRAVEL_DATA_DIR/warehouse/travel.duckdb` | DuckDB file shared by loaders, checks and dbt |
| `TRAVEL_DUCKDB_MEMORY_LIMIT` / `TRAVEL_DUCKDB_THREADS` | `2GB` / `4` | DuckDB resource limits |
| `DBT_TARGET` | `dev` | dbt target in `dbt/profiles.yml` (`dev`, `ci`, `prod`) |
| `TRAVEL_ENFORCE_SOURCE_FRESHNESS` | `true` | Fail the transform when raw data is older than dbt's `error_after` (50 h) |
| `TRAVEL_FLIGHTS_REPLAY_START`, `TRAVEL_STAYS_REPLAY_START` | – | Real date the replay clock starts (schedules skip while unset) |
| `TRAVEL_FLIGHTS_REPLAY_PARTITIONS_PER_DAY`, `TRAVEL_STAYS_REPLAY_PARTITIONS_PER_DAY` | `1`, `7` | Partitions released per daily tick |
| `ALERT_WEBHOOK_URL` | – | Slack-compatible webhook for failed runs (only logged when unset) |
| `DAGSTER_WEBSERVER_URL` | `http://localhost:3000` | Base URL used in alert links |
| `DAGSTER_POSTGRES_USER` / `_PASSWORD` / `_DB`, `DAGSTER_WEBSERVER_PORT` | `dagster` / required / `dagster`, `3000` | Compose only |

## Testing and CI

```bash
make lint        # ruff check, ruff format --check, sqlfluff (duckdb dialect)
make test        # 33 unit tests: manifests, schemas, loaders, replay clock, schedules, sensors, alerts, definitions
make test-e2e    # 4 end-to-end tests: every job in-process on fixtures, then reruns must leave every mart identical
make validate    # load the Dagster definitions as a deployment would
```

dbt itself carries 120+ data tests (generic, custom generic, singular, two warn-level drift
detectors), 5 unit tests, model contracts on every mart, and source freshness.

| CI job | What it proves |
| --- | --- |
| `lint` | ruff + sqlfluff are clean |
| `unit` | dbt manifest builds, Dagster definitions load, unit tests pass (coverage uploaded) |
| `pipeline` | `make pipeline` materialises every asset on fixtures through the Dagster CLI, a standalone `dbt build` passes on the same warehouse, the e2e/idempotency tests pass, dbt docs are uploaded as an artifact |
| `docker` | the image builds, definitions load inside it, the Compose stack comes up healthy, and `scripts/compose_smoke_test.py` launches the ingest and both full-range loads through the webserver, then waits for the **sensor** to trigger a successful dbt transform |

Dependabot keeps uv, GitHub Actions, Docker and Compose images current; `.pre-commit-config.yaml`
runs the same linters locally.

## Operations

* **Scheduling.** `flights_replay_schedule` and `stays_replay_schedule` (06:00 UTC, off by
  default) release the next partitions each day once a replay start date is set.
  `ingest_weekly_schedule` and `kaggle_upstream_version_sensor` (both off by default) pick up
  new upstream versions.
* **Chaining.** `transform_on_raw_update` runs dbt once all raw sources exist and any of them
  changed. It waits for in-progress loads, so a burst of loads produces one transform.
  `reload_on_new_dataset_version` reloads every already-loaded partition after a new upstream version lands.
* **Backfill and reprocessing.** Raw assets use single-run backfill policies: a range of
  any size is one run and one DuckDB transaction. Re-running a window replaces exactly
  that window, and `fct_flight_fare_quotes` rebuilds only the reloaded departure dates.
* **Concurrency.** DuckDB is single-writer. Every warehouse step claims the `duckdb` pool
  (limit 1) and every job carries a tag limited to one concurrent run, so runs queue instead of failing on locks.
* **Data quality.** Blocking asset checks (landing checksums, landing↔raw reconciliation
  per partition, booking-event consistency, listing uniqueness), dbt tests as asset checks,
  freshness policies (warn 26 h, fail 50 h) on raw assets and marts, dbt source freshness,
  and run retries for crashed workers.
* **Alerting.** `alert_on_run_failure` posts every failed run, including failed blocking checks, to `ALERT_WEBHOOK_URL`.

Procedures for backfills, new upstream versions, rollbacks and failure triage are in the
**[runbook](docs/runbook.md)**.

## Project structure

```text
├── src/travel_pipeline/
│   ├── definitions.py        # Dagster code location
│   ├── assets/               # landing.py, raw.py (+ asset checks), dbt.py
│   ├── ingestion/            # manifest.py, sources.py, schemas.py, readers.py, loaders.py
│   ├── jobs.py  schedules.py  sensors.py  replay.py
│   ├── partitions.py  policies.py  resources.py  settings.py  datasets.py
├── dbt/
│   ├── models/{staging,intermediate,marts/{core,flights,stays,listings,cross}}/
│   ├── snapshots/  seeds/  macros/  tests/
│   └── dbt_project.yml  profiles.yml
├── tests/                    # pytest suite + synthetic Kaggle-shaped fixtures
├── scripts/                  # fixture + data-dictionary generators, Compose smoke test
├── docker/                   # dagster.yaml (Postgres), workspace.yaml
├── dagster_home/dagster.yaml # local instance config (same concurrency rules)
├── docs/                     # data_dictionary.md, runbook.md
├── Dockerfile  docker-compose.yml  docker-compose.ci.yml
├── Makefile  pyproject.toml  uv.lock  .env.example
└── .github/                  # CI workflow, Dependabot
```

## Design rationale

Four constraints shaped the design. The sources are real but static public snapshots, so the
pipeline replays their history as a daily feed; they are not redistributed, so CI and the
tests run on synthetic look-alikes. Everything runs on one laptop with no cloud account, and
the same image runs under Docker Compose. Backfills, reprocessing and version reloads
rewrite windows that were already loaded, so every load must converge to the same tables. And
the data fixes the time axes and the joins: every fare was observed on one day, and the two
stays datasets cover different countries (see [Data sources](#data-sources)).

### Architecture decisions

| Decision | Why | Alternatives considered | Trade-off accepted |
| --- | --- | --- | --- |
| Content-addressed, immutable landing zone (`ingestion/manifest.py`) | Version directories are named by the hash of their files and carry a sha256 manifest, so re-landing is a no-op, history is reproducible and rollback means rewriting the one-line `CURRENT` pointer; each manifest also records whether its files came from Kaggle or a local directory (`source_mode`) | Overwriting one download directory per dataset; timestamped download folders | Every upstream version is a full copy, and nothing prunes old versions |
| Daily partitions on the data's only time axes, released by a replay clock (`partitions.py`, `replay.py`) | Fares are partitioned by departure date (all were observed on 2022-02-10) and booking events by event date; replaying the real history exercises the schedules, backfills, incremental builds and freshness policies a live feed needs, without inventing data | One full load per file; flights by observation date (there is only one); bookings by arrival date (a booking would appear on its arrival day with its outcome already attached) | Partition bounds are fixed in code: a new version with dates outside them fails `reconciles_with_landing` until the bounds are extended |
| Event-sourced stays: SCD2 from events, a dbt snapshot only for listings | Each booking becomes a `booking_created` event (arrival − lead time) and a `status_changed` event (reservation status date), so a replayed day shows only what was known then: a booking is `Booked`, outcome unknown, until its status event arrives. Status history derived from the events (`int_stays__booking_status_history`) stays exact under backfills and out-of-order reruns. Listing prices, ratings and review counts change between captures, so `snap_hotel_listings` (check strategy, hard deletes invalidated) records them across upstream versions | One row per booking; a dbt snapshot of booking status, whose history would depend on when and how often dbt ran | Two raw rows per booking, with current state rebuilt in `int_stays__bookings`; windows loaded out of order leave status events without a creation event until the earlier window loads (reported by `event_pairs_consistent`, not failed) |
| Replace-window loads in one transaction, single-run backfills (`ingestion/loaders.py`) | Each load deletes the window `[start, end)` and inserts the landed rows for it in one DuckDB transaction, so any rerun converges to the same state and a failed run leaves the previous contents untouched; a range of any size is one run. `tests/test_e2e_fixtures.py` reruns loads and asserts that every mart is unchanged, timestamp columns aside | Append-only loads de-duplicated downstream, or a MERGE on a key (neither works without a natural key); one run per daily partition | A reload rewrites its whole window and re-reads the whole landed file to select it: cheap at this size (a ~55 MB download), costly at larger volumes |
| Row-number identity (`ingestion/readers.py`) | The flight and booking files have no natural key and both contain exact duplicate rows, which are kept as separate rows; `fare_quote_id` and `booking_id` derive from the source file and row number, so they are stable within an upstream version | Hashing row contents (collides on exact duplicates); de-duplicating (silently changes the source's row counts) | Ids are not stable across versions: if a new version reorders rows, every booking is reloaded with a new `booking_id` (`reload_on_new_dataset_version` runs the reload) |
| Declared schemas at the boundary, parsing in dbt (`ingestion/schemas.py`, `dbt/models/staging/`) | Headers must match the declared names and order, typed columns are cast strictly inside DuckDB, and a raw table that has drifted fails the load (`SchemaDriftError`); free text such as `'5,953'` or `'02h 10m'` stays raw and is parsed in staging, where quirks such as malformed durations are flagged, not dropped | Type inference (DuckDB's `read_csv_auto`, pandas dtypes); cleaning in Python during the load | An upstream column change needs edits to `schemas.py` and the staging model, then a drop and reload of that raw table |
| Incremental only where it pays (`fct_flight_fare_quotes`) | At this scale DuckDB rebuilds the marts in seconds, and a full rebuild cannot drift from its inputs; only the 300,261-row fare fact is incremental (delete+insert on `departure_date`, `on_schema_change='fail'`), rebuilding exactly the departure dates whose rows were reloaded | Incremental models throughout; a full rebuild of the fact as well | Changing the incremental model or its contract needs `dbt build --full-refresh` |
| Warehouse writes serialised by Dagster (`policies.py`, `dagster_home/dagster.yaml`, `docker/dagster.yaml`) | DuckDB allows one read-write process per file, and a writer blocks readers in other processes, so every warehouse step claims the `duckdb` pool (limit 1) and every job carries the `travel/warehouse=duckdb` tag (one such run at a time): runs queue instead of failing on locks | Retrying on lock errors; a warehouse server that takes concurrent writers | One writer at a time, and no concurrent BI reads while a run is active |
| Checks at every layer (`assets/landing.py`, `assets/raw.py`, the dbt YAML) | Blocking asset checks cover checksums, per-partition landing↔raw reconciliation, booking-event consistency and listing uniqueness; `dbt build` runs the 120+ dbt tests in DAG order and skips models downstream of a failing test; every mart has an enforced contract; dbt source freshness must pass before the build; failed runs are posted to `ALERT_WEBHOOK_URL` | Testing only after the build (`dbt test`); a separate data-quality framework (Great Expectations, Soda) | A blocking check stops downstream steps only within its own run: the transform sensor keys on new raw materialisations, so a failed load check surfaces as a failed run and an alert, not as a held-back transform |

### Stack choices

| Layer | Choice | Why this | Why not the alternatives |
| --- | --- | --- | --- |
| Runtime | Python 3.12 (`>=3.12,<3.13`) | One interpreter version in the lock, the Docker base image and CI, so `uv.lock` resolves one way everywhere | A version range would need a CI matrix to stay honest, for a project that ships one image |
| Dependencies | uv 0.12.23, `uv.lock` | `uv sync --frozen` builds the same environment on a laptop, in CI and in the Docker build; uv also installs Python 3.12 and is one binary copied into the image | pip with pip-tools, or Poetry, would lock dependencies too, but uv covers the interpreter, the lock and the install in one tool |
| Orchestration | Dagster 1.13.25, `dagster-dbt` 0.29.25 | Software-defined assets fit a partitioned warehouse: daily partitions, single-run backfill policies, blocking asset checks, freshness policies, concurrency pools and cursor-based sensors are built in, and `dagster-dbt` makes every dbt model an asset and every dbt test an asset check in one lineage graph | Airflow is task-centric, so partition-range backfills, per-asset checks and dbt lineage need extra plumbing (e.g. Cosmos for dbt); cron plus scripts has no lineage, run queue or retries |
| Transformations | dbt-core 1.12.5, `dbt-duckdb` 1.11.0 | Models, data tests, unit tests, contracts, the snapshot, seeds, source freshness and docs in one versioned project; another warehouse is an adapter change plus porting the DuckDB-specific SQL | SQL scripts run from Python (no tests, contracts or lineage); SQLMesh would also fit, but dbt's adapter ecosystem and `dagster-dbt`'s asset mapping make it the safer pairing with Dagster |
| Warehouse | DuckDB 1.5.6, one file | In-process columnar SQL with nothing to operate; it scans the loader's Arrow tables directly and backs up as one file | PostgreSQL (a server to run, and row storage for analytical scans); Snowflake or BigQuery (a cloud account, credentials and spend for a ~55 MB download); SQLite (row storage, thin analytical SQL) |
| CSV reading | pyarrow 25.0.1 | Reads every column as text in file order, so `source_row_number` is an explicit, stable counter; copes with the quoted newlines in the flight `stop` column | pandas (an extra dependency, with dtype inference to switch off); DuckDB's `read_csv` (an explicit file-order counter is simpler to trust than numbering rows after a parallel scan) |
| Downloads | `kaggle` 2.2.4 | The official client: new-style `KGAT_` tokens and dataset version numbers, which let landing skip unchanged downloads and `kaggle_upstream_version_sensor` (off by default) poll for new versions; `TRAVEL_SOURCE_MODE=local` swaps in the synthetic fixtures, so CI needs no token | Hand-written calls to the Kaggle API (re-implementing auth and version lookups); committing the CSVs (the repo does not redistribute Kaggle data) |
| Run and event storage | PostgreSQL 17 through `dagster-postgres` 0.29.25 (Compose only); SQLAlchemy 2.0.54, psycopg2-binary 2.9.13 | Shared instance storage for the webserver, daemon and code-location containers, while local runs keep SQLite under `dagster_home/`; SQLAlchemy stays on 2.0.x because 2.1 resolves `postgresql://` to psycopg 3, which breaks Dagster's Postgres storage | SQLite (fine for `dagster dev`; Dagster's reference Docker and Helm deployments put shared instance storage in Postgres) |
| Packaging and deployment | One Docker image (`python:3.12-slim-bookworm`), Docker Compose v2 | The same image runs the gRPC code location, webserver and daemon; runs execute inside the code location (`DefaultRunLauncher`), next to the DuckDB file on the `travel-data` volume; the dbt manifest is built into the image | Per-run containers (`DockerRunLauncher`) or Kubernetes add isolation that a single-writer warehouse cannot use, since its runs are serialised anyway |
| CI | GitHub Actions (`ubuntu-24.04`), Dependabot | The `lint`, `unit`, `pipeline` and `docker` jobs run on the synthetic fixtures with no repository secrets, and the `docker` job smoke-tests the Compose stack through its webserver; Dependabot keeps uv, Actions, Docker and Compose versions current | CI on the real Kaggle data (a token in CI and a download on every push, when the fixtures already reproduce the schemas and quirks) |
| Quality gates | ruff 0.16.10, sqlfluff 4.4.0, pytest 9.1.1, pre-commit | ruff lints and formats Python in one tool; sqlfluff lints the dbt SQL through the Jinja templater with the DuckDB dialect; pytest keeps the fast unit tests apart from the `e2e` marker; pre-commit runs the same linters locally | flake8 + black + isort (three tools for ruff's job); sqlfmt (formats SQL but does not lint it) |

### What would change in production

* **A live feed instead of a replay.** The replay clock (`replay.py`) and the fixed bounds in
  `partitions.py` would give way to open-ended daily partitions filled as data arrives. Fares
  captured every day would make `observed_on` (today the dbt var `flights_observed_on`) a real
  column and the natural flights partition key, which also separates lead time from
  seasonality in the fare curve.
* **The landing zone in object storage.** `data/landing/` (Compose: the `travel-data` volume)
  is the system of record today; the same content-addressed layout would move to a bucket
  with object versioning and a write-once retention policy (S3 or GCS), with `CURRENT` as a
  one-line pointer object.
* **A warehouse that takes concurrent writers.** Beyond the read replica on the roadmap,
  outgrowing one machine means a dbt adapter change (e.g. Snowflake or BigQuery), porting the
  DuckDB-specific SQL (`try_strptime`, `regexp_full_match`, `unnest(range(...))`) and
  relaxing the `duckdb` pool and run-tag limit so loads can run in parallel.
* **A managed Dagster deployment.** The same image would run on Kubernetes through Dagster's
  Helm chart with managed Postgres for run storage (or on Dagster+, which hosts that
  storage), with per-run containers once the warehouse is a server, and with
  `KAGGLE_API_TOKEN`, `ALERT_WEBHOOK_URL` and database credentials read from a secret store
  instead of `.env`.
* **Source-system keys and inventory.** A booking feed from a property-management system
  would carry its own booking id and room counts, so ids would survive upstream reorders and
  occupancy and RevPAR would stop being proxies.

#### Known limitations

* The upstream datasets are static snapshots; the "daily feed" is a replay.
* Fare curves mix lead time with departure-date seasonality, because there is a single observation date.
* RevPAR and occupancy are proxies (capacity assumed = peak observed occupied rooms). The
  source publishes no room inventory, so true RevPAR is not computed.
* The trip-cost benchmark combines fares observed on 2022-02-10 with listing prices
  published on 2023-08-19 (555 days apart); use it to compare cities, not as a price.
* The Compose deployment has been verified in CI, not on a workstation.

#### Roadmap

* Publish marts to a read replica (Parquet export or MotherDuck) so BI reads never contend with writes.
* Add FX conversion (ECB reference rates) to compare EUR and INR measures.
* Freshness-failure alerts (today, only run failures alert) and a lightweight dashboard.

## License

Code: [MIT](LICENSE). Data: not redistributed; each dataset keeps its own licence (see
[Data sources](#data-sources)).
