"""Content-addressed, immutable landing zone with checksum manifests.

Layout per dataset::

    <landing_dir>/<dataset_key>/
        CURRENT                      # name of the version directory in use
        versions/<version_id>/
            manifest.json            # sha256 + size of every file, upstream version, licence
            <data files>

``version_id`` is derived from the file checksums, so landing the same bytes twice is a
no-op and a version directory, once written, is never modified.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from travel_pipeline.datasets import SourceDataset

MANIFEST_FILE = "manifest.json"
CURRENT_POINTER = "CURRENT"
_CHUNK = 1024 * 1024


class LandingError(RuntimeError):
    """Raised when the landing zone is inconsistent with its manifests."""


@dataclass(frozen=True)
class LandedFile:
    name: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class Manifest:
    dataset_key: str
    kaggle_ref: str
    version_id: str
    upstream_version: int | None
    source_mode: str
    license: str
    landed_at: str
    files: tuple[LandedFile, ...]

    def to_json(self) -> str:
        payload = asdict(self)
        payload["files"] = [asdict(f) for f in self.files]
        return json.dumps(payload, indent=2, sort_keys=True) + "\n"

    @classmethod
    def from_json(cls, text: str) -> Manifest:
        payload = json.loads(text)
        payload["files"] = tuple(LandedFile(**f) for f in payload["files"])
        return cls(**payload)

    def file(self, name: str) -> LandedFile:
        for landed in self.files:
            if landed.name == name:
                return landed
        raise KeyError(name)


@dataclass(frozen=True)
class LandingResult:
    manifest: Manifest
    version_dir: Path
    changed: bool


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def content_version_id(files: list[LandedFile]) -> str:
    digest = hashlib.sha256()
    for landed in sorted(files, key=lambda f: f.name):
        digest.update(f"{landed.name}:{landed.sha256}\n".encode())
    return f"sha256-{digest.hexdigest()[:16]}"


def dataset_dir(landing_dir: Path, dataset: SourceDataset) -> Path:
    return landing_dir / dataset.key


def read_current(landing_dir: Path, dataset: SourceDataset) -> tuple[Manifest, Path] | None:
    root = dataset_dir(landing_dir, dataset)
    pointer = root / CURRENT_POINTER
    if not pointer.exists():
        return None
    version_dir = root / "versions" / pointer.read_text().strip()
    manifest_path = version_dir / MANIFEST_FILE
    if not manifest_path.exists():
        raise LandingError(f"{pointer} points at {version_dir} which has no {MANIFEST_FILE}")
    return Manifest.from_json(manifest_path.read_text()), version_dir


def verify_version(manifest: Manifest, version_dir: Path) -> list[str]:
    """Return a list of problems (empty when every file matches its checksum)."""
    problems = []
    for landed in manifest.files:
        path = version_dir / landed.name
        if not path.exists():
            problems.append(f"missing file {landed.name}")
            continue
        size = path.stat().st_size
        if size != landed.bytes:
            problems.append(f"{landed.name}: size {size} != manifest {landed.bytes}")
        elif sha256_file(path) != landed.sha256:
            problems.append(f"{landed.name}: sha256 does not match manifest")
    return problems


def _atomic_write_text(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "w") as handle:
        handle.write(text)
    Path(tmp).replace(path)


def land_files(
    *,
    landing_dir: Path,
    dataset: SourceDataset,
    staged_dir: Path,
    upstream_version: int | None,
    source_mode: str,
) -> LandingResult:
    """Promote files from ``staged_dir`` into an immutable version directory.

    Idempotent: re-landing identical bytes returns the existing version with
    ``changed=False``. Raises if a version directory exists but its contents differ
    from its own manifest (someone modified the landing zone by hand).
    """
    missing = [name for name in dataset.files if not (staged_dir / name).is_file()]
    if missing:
        raise LandingError(f"{dataset.kaggle_ref}: expected files not found upstream: {missing}")

    files = [
        LandedFile(
            name=name,
            sha256=sha256_file(staged_dir / name),
            bytes=(staged_dir / name).stat().st_size,
        )
        for name in dataset.files
    ]
    version_id = content_version_id(files)
    root = dataset_dir(landing_dir, dataset)
    versions = root / "versions"
    versions.mkdir(parents=True, exist_ok=True)
    version_dir = versions / version_id

    previous = read_current(landing_dir, dataset)
    if version_dir.exists():
        manifest = Manifest.from_json((version_dir / MANIFEST_FILE).read_text())
        problems = verify_version(manifest, version_dir)
        if problems:
            raise LandingError(f"immutable version {version_dir} was modified: {problems}")
    else:
        manifest = Manifest(
            dataset_key=dataset.key,
            kaggle_ref=dataset.kaggle_ref,
            version_id=version_id,
            upstream_version=upstream_version,
            source_mode=source_mode,
            license=dataset.license,
            landed_at=datetime.now(UTC).isoformat(timespec="seconds"),
            files=tuple(files),
        )
        tmp_dir = Path(tempfile.mkdtemp(dir=versions, prefix=f".{version_id}."))
        try:
            for landed in files:
                shutil.copy2(staged_dir / landed.name, tmp_dir / landed.name)
            (tmp_dir / MANIFEST_FILE).write_text(manifest.to_json())
            tmp_dir.replace(version_dir)
        except BaseException:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise

    _atomic_write_text(root / CURRENT_POINTER, version_id + "\n")
    changed = previous is None or previous[0].version_id != version_id
    return LandingResult(manifest=manifest, version_dir=version_dir, changed=changed)
