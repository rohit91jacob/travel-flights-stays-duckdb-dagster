"""Runtime settings resolved from environment variables.

Everything that differs between a laptop, CI and the Docker deployment is an
environment variable (documented in ``.env.example``). Defaults point at
``./data`` inside the repository so a fresh clone works without configuration.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = Path(os.environ.get("TRAVEL_DBT_PROJECT_DIR", PROJECT_ROOT / "dbt")).resolve()

SOURCE_MODES = ("kaggle", "local")


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    landing_dir: Path
    duckdb_path: Path
    source_mode: str
    local_source_dir: Path | None
    duckdb_memory_limit: str
    duckdb_threads: int


def _path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser().resolve() if raw else default.resolve()


def load_settings() -> Settings:
    data_dir = _path("TRAVEL_DATA_DIR", PROJECT_ROOT / "data")
    source_mode = os.environ.get("TRAVEL_SOURCE_MODE", "kaggle").strip().lower()
    if source_mode not in SOURCE_MODES:
        raise ValueError(f"TRAVEL_SOURCE_MODE must be one of {SOURCE_MODES}, got {source_mode!r}")
    local_dir = os.environ.get("TRAVEL_LOCAL_SOURCE_DIR")
    if source_mode == "local" and not local_dir:
        raise ValueError("TRAVEL_SOURCE_MODE=local requires TRAVEL_LOCAL_SOURCE_DIR")
    return Settings(
        data_dir=data_dir,
        landing_dir=_path("TRAVEL_LANDING_DIR", data_dir / "landing"),
        duckdb_path=_path("TRAVEL_DUCKDB_PATH", data_dir / "warehouse" / "travel.duckdb"),
        source_mode=source_mode,
        local_source_dir=Path(local_dir).expanduser().resolve() if local_dir else None,
        duckdb_memory_limit=os.environ.get("TRAVEL_DUCKDB_MEMORY_LIMIT", "2GB"),
        duckdb_threads=int(os.environ.get("TRAVEL_DUCKDB_THREADS", "4")),
    )


def export_for_subprocesses(settings: Settings) -> None:
    """Publish resolved defaults so the dbt subprocess opens the same DuckDB file.

    dbt reads ``TRAVEL_DUCKDB_PATH`` etc. from its environment (see dbt/profiles.yml).
    Only unset variables are filled in, so explicit configuration always wins.
    """
    os.environ.setdefault("TRAVEL_DUCKDB_PATH", str(settings.duckdb_path))
    os.environ.setdefault("TRAVEL_DUCKDB_MEMORY_LIMIT", settings.duckdb_memory_limit)
    os.environ.setdefault("TRAVEL_DUCKDB_THREADS", str(settings.duckdb_threads))
