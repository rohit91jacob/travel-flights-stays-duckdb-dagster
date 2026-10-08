"""Dagster code location for the travel pipeline."""

import shutil
import sys
from pathlib import Path

from dagster import Definitions
from dagster_dbt import DbtCliResource

from travel_pipeline.assets.dbt import dbt_project, travel_dbt
from travel_pipeline.assets.landing import landing_assets, landing_checks
from travel_pipeline.assets.raw import raw_assets, raw_checks
from travel_pipeline.jobs import all_jobs
from travel_pipeline.resources import AlertNotifier, DuckDBWarehouse, LandingZone
from travel_pipeline.schedules import all_schedules
from travel_pipeline.sensors import all_sensors
from travel_pipeline.settings import export_for_subprocesses, load_settings

settings = load_settings()
export_for_subprocesses(settings)

# Prefer the dbt installed next to this interpreter, so the code location works whether or
# not its virtualenv is on PATH (e.g. when launched by absolute path or a process manager).
_venv_dbt = Path(sys.executable).with_name("dbt")
DBT_EXECUTABLE = str(_venv_dbt) if _venv_dbt.exists() else (shutil.which("dbt") or "dbt")

defs = Definitions(
    assets=[*landing_assets, *raw_assets, travel_dbt],
    asset_checks=[*landing_checks, *raw_checks],
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
    resources={
        "landing": LandingZone(
            landing_dir=str(settings.landing_dir),
            source_mode=settings.source_mode,
            local_source_dir=str(settings.local_source_dir) if settings.local_source_dir else None,
        ),
        "warehouse": DuckDBWarehouse(
            path=str(settings.duckdb_path),
            memory_limit=settings.duckdb_memory_limit,
            threads=settings.duckdb_threads,
        ),
        "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE),
        "alerts": AlertNotifier(),
    },
)
