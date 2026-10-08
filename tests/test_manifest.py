import shutil
from pathlib import Path

import pytest

from travel_pipeline.datasets import HOTEL_BOOKINGS
from travel_pipeline.ingestion.manifest import (
    LandingError,
    land_files,
    read_current,
    verify_version,
)


def _stage(tmp_path: Path, fixtures_dir: Path) -> Path:
    staged = tmp_path / "staged"
    shutil.copytree(fixtures_dir / "hotel_bookings", staged)
    return staged


def test_landing_is_idempotent(tmp_path: Path, fixtures_dir: Path) -> None:
    staged = _stage(tmp_path, fixtures_dir)
    landing = tmp_path / "landing"

    first = land_files(
        landing_dir=landing,
        dataset=HOTEL_BOOKINGS,
        staged_dir=staged,
        upstream_version=1,
        source_mode="local",
    )
    second = land_files(
        landing_dir=landing,
        dataset=HOTEL_BOOKINGS,
        staged_dir=staged,
        upstream_version=1,
        source_mode="local",
    )

    assert first.changed is True
    assert second.changed is False
    assert first.manifest.version_id == second.manifest.version_id
    assert first.manifest.version_id.startswith("sha256-")
    assert len(list((landing / "hotel_bookings" / "versions").iterdir())) == 1
    assert verify_version(first.manifest, first.version_dir) == []


def test_new_content_creates_new_version_and_moves_pointer(
    tmp_path: Path, fixtures_dir: Path
) -> None:
    staged = _stage(tmp_path, fixtures_dir)
    landing = tmp_path / "landing"
    first = land_files(
        landing_dir=landing,
        dataset=HOTEL_BOOKINGS,
        staged_dir=staged,
        upstream_version=1,
        source_mode="local",
    )

    with (staged / "hotel_bookings.csv").open("a", encoding="utf-8") as handle:
        handle.write("\n")
    second = land_files(
        landing_dir=landing,
        dataset=HOTEL_BOOKINGS,
        staged_dir=staged,
        upstream_version=2,
        source_mode="local",
    )

    assert second.changed is True
    assert second.manifest.version_id != first.manifest.version_id
    current = read_current(landing, HOTEL_BOOKINGS)
    assert current is not None
    assert current[0].version_id == second.manifest.version_id
    assert current[0].upstream_version == 2
    # The old version is kept untouched for reproducibility.
    assert verify_version(first.manifest, first.version_dir) == []


def test_tampered_version_is_detected(tmp_path: Path, fixtures_dir: Path) -> None:
    staged = _stage(tmp_path, fixtures_dir)
    landing = tmp_path / "landing"
    result = land_files(
        landing_dir=landing,
        dataset=HOTEL_BOOKINGS,
        staged_dir=staged,
        upstream_version=1,
        source_mode="local",
    )

    (result.version_dir / "hotel_bookings.csv").write_text("tampered", encoding="utf-8")

    assert verify_version(result.manifest, result.version_dir)
    with pytest.raises(LandingError, match="was modified"):
        land_files(
            landing_dir=landing,
            dataset=HOTEL_BOOKINGS,
            staged_dir=staged,
            upstream_version=1,
            source_mode="local",
        )


def test_missing_upstream_file_fails(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(LandingError, match="expected files not found"):
        land_files(
            landing_dir=tmp_path / "landing",
            dataset=HOTEL_BOOKINGS,
            staged_dir=empty,
            upstream_version=None,
            source_mode="local",
        )
