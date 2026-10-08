"""Test configuration.

The Dagster code location reads its settings at import time, so the environment is pinned
here, before any test module imports ``travel_pipeline.definitions``: fixtures instead of
Kaggle, a throwaway data directory, and the ``ci`` dbt target.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "kaggle"
SESSION_DATA_DIR = Path(tempfile.mkdtemp(prefix="travel-tests-"))

os.environ["TRAVEL_SOURCE_MODE"] = "local"
os.environ["TRAVEL_LOCAL_SOURCE_DIR"] = str(FIXTURES)
os.environ["TRAVEL_DATA_DIR"] = str(SESSION_DATA_DIR)
os.environ["TRAVEL_DUCKDB_PATH"] = str(SESSION_DATA_DIR / "warehouse" / "travel_ci.duckdb")
os.environ["TRAVEL_DUCKDB_MEMORY_LIMIT"] = "1GB"
os.environ["TRAVEL_DUCKDB_THREADS"] = "2"
os.environ["DBT_TARGET"] = "ci"
os.environ.pop("ALERT_WEBHOOK_URL", None)


def _parse_dbt_project() -> None:
    """dagster-dbt needs target/manifest.json at import time.

    Parsing with this session's environment also seeds dbt's partial-parse cache, so every
    dbt invocation the tests trigger re-parses only what changed.
    """
    dbt = Path(sys.executable).with_name("dbt")
    subprocess.run(
        [str(dbt), "parse", "--quiet", "--profiles-dir", ".", "--target", "ci"],
        cwd=ROOT / "dbt",
        check=True,
    )


_parse_dbt_project()


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
