"""Where upstream files come from: the Kaggle API, or a local directory (CI / air-gapped)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Protocol

from travel_pipeline.datasets import SourceDataset


class DatasetSource(Protocol):
    mode: str

    def upstream_version(self, dataset: SourceDataset) -> int | None:
        """Current upstream version number, or None when the source has no versions."""

    def fetch(self, dataset: SourceDataset, dest_dir: Path) -> None:
        """Place every file in ``dataset.files`` into ``dest_dir``."""


class KaggleDatasetSource:
    """Downloads datasets with the official Kaggle client.

    Authentication is handled by the client: it reads ``KAGGLE_API_TOKEN`` or
    ``~/.kaggle/access_token`` (new-style tokens) or ``~/.kaggle/kaggle.json`` (legacy).
    The import is deferred because the client authenticates on import.
    """

    mode = "kaggle"

    def __init__(self) -> None:
        self._api = None

    def _client(self):
        if self._api is None:
            from kaggle.api.kaggle_api_extended import KaggleApi  # noqa: PLC0415

            api = KaggleApi()
            api.authenticate()
            self._api = api
        return self._api

    def upstream_version(self, dataset: SourceDataset) -> int | None:
        results = self._client().dataset_list(user=dataset.owner, search=dataset.slug)
        for item in results or []:
            if item.ref == dataset.kaggle_ref:
                return int(item.current_version_number)
        return None

    def fetch(self, dataset: SourceDataset, dest_dir: Path) -> None:
        self._client().dataset_download_files(
            dataset.kaggle_ref, path=str(dest_dir), unzip=True, quiet=True
        )


class LocalDatasetSource:
    """Copies files from ``<root>/<dataset.key>/``; used for fixtures and offline runs."""

    mode = "local"

    def __init__(self, root: Path) -> None:
        self.root = root

    def upstream_version(self, dataset: SourceDataset) -> int | None:
        return None

    def fetch(self, dataset: SourceDataset, dest_dir: Path) -> None:
        src = self.root / dataset.key
        if not src.is_dir():
            raise FileNotFoundError(f"local source directory {src} does not exist")
        for name in dataset.files:
            if (src / name).is_file():
                shutil.copy2(src / name, dest_dir / name)
