"""Dagster resources. All configuration comes from the environment (see settings.py)."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import requests
from dagster import ConfigurableResource, get_dagster_logger

from travel_pipeline.datasets import SourceDataset
from travel_pipeline.ingestion.manifest import LandingError, Manifest, read_current
from travel_pipeline.ingestion.sources import DatasetSource, KaggleDatasetSource, LocalDatasetSource


class LandingZone(ConfigurableResource):
    """Immutable landing zone plus the upstream it is filled from."""

    landing_dir: str
    source_mode: str = "kaggle"
    local_source_dir: str | None = None

    @property
    def root(self) -> Path:
        return Path(self.landing_dir)

    def source(self) -> DatasetSource:
        if self.source_mode == "local":
            if not self.local_source_dir:
                raise ValueError("source_mode=local requires local_source_dir")
            return LocalDatasetSource(Path(self.local_source_dir))
        return KaggleDatasetSource()

    def current_or_none(self, dataset: SourceDataset) -> tuple[Manifest, Path] | None:
        return read_current(self.root, dataset)

    def current(self, dataset: SourceDataset) -> tuple[Manifest, Path]:
        landed = self.current_or_none(dataset)
        if landed is None:
            raise LandingError(
                f"{dataset.key} has not been landed yet; materialize landing/{dataset.key} first"
            )
        return landed


class DuckDBWarehouse(ConfigurableResource):
    """The single DuckDB file shared by the loaders, the asset checks and dbt."""

    path: str
    memory_limit: str = "2GB"
    threads: int = 4

    @contextmanager
    def connect(self, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
        db = Path(self.path)
        db.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(
            str(db),
            read_only=read_only and db.exists(),
            config={"memory_limit": self.memory_limit, "threads": self.threads},
        )
        try:
            yield con
        finally:
            con.close()


class AlertNotifier(ConfigurableResource):
    """Posts Slack-compatible ``{"text": ...}`` payloads to a webhook.

    The URL is read from the environment at send time (it embeds a secret, so it is kept out
    of resource config shown in the UI). Without a URL, alerts are only logged.
    """

    webhook_url_env_var: str = "ALERT_WEBHOOK_URL"
    timeout_seconds: float = 10.0

    def notify(self, text: str) -> bool:
        log = get_dagster_logger()
        url = os.environ.get(self.webhook_url_env_var, "").strip()
        if not url:
            log.warning("alert (no %s configured): %s", self.webhook_url_env_var, text)
            return False
        try:
            response = requests.post(url, json={"text": text}, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException:
            log.exception("failed to deliver alert to webhook")
            return False
        return True
