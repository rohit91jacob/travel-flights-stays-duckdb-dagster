"""Landing assets: download each Kaggle dataset into the immutable landing zone."""

import tempfile
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    DataVersion,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
)

from travel_pipeline.datasets import ALL_DATASETS, SourceDataset
from travel_pipeline.ingestion.manifest import land_files, verify_version
from travel_pipeline.policies import KAGGLE_RETRY
from travel_pipeline.resources import LandingZone


def landing_key(dataset: SourceDataset) -> AssetKey:
    return AssetKey(["landing", dataset.key])


def _metadata(dataset: SourceDataset, manifest, version_dir: Path, *, changed: bool) -> dict:
    return {
        "kaggle_ref": dataset.kaggle_ref,
        "license": dataset.license,
        "version_id": manifest.version_id,
        "upstream_version": manifest.upstream_version or 0,
        "changed": changed,
        "path": MetadataValue.path(str(version_dir)),
        "total_bytes": sum(f.bytes for f in manifest.files),
        "files": MetadataValue.json(
            [{"name": f.name, "sha256": f.sha256, "bytes": f.bytes} for f in manifest.files]
        ),
    }


def build_landing_asset(dataset: SourceDataset) -> AssetsDefinition:
    @asset(
        key=landing_key(dataset),
        group_name="landing",
        kinds={"kaggle", "python"},
        retry_policy=KAGGLE_RETRY,
        description=(
            f"Raw files of Kaggle dataset `{dataset.kaggle_ref}` ({dataset.license}), stored "
            "byte-for-byte in a content-addressed, never-modified version directory with a "
            "sha256 manifest. Skips the download when the upstream version is unchanged."
        ),
        metadata={"kaggle_ref": dataset.kaggle_ref, "license": dataset.license},
    )
    def _landing(context: AssetExecutionContext, landing: LandingZone) -> MaterializeResult:
        source = landing.source()
        upstream_version = source.upstream_version(dataset)
        current = landing.current_or_none(dataset)
        if current is not None and upstream_version is not None:
            manifest, version_dir = current
            if manifest.upstream_version == upstream_version and not verify_version(
                manifest, version_dir
            ):
                context.log.info(
                    "%s upstream version %s already landed as %s; nothing to download",
                    dataset.kaggle_ref,
                    upstream_version,
                    manifest.version_id,
                )
                return MaterializeResult(
                    metadata=_metadata(dataset, manifest, version_dir, changed=False),
                    data_version=DataVersion(manifest.version_id),
                )

        with tempfile.TemporaryDirectory(prefix=f"travel-{dataset.key}-") as tmp:
            source.fetch(dataset, Path(tmp))
            result = land_files(
                landing_dir=landing.root,
                dataset=dataset,
                staged_dir=Path(tmp),
                upstream_version=upstream_version,
                source_mode=source.mode,
            )
        context.log.info(
            "%s landed as %s (changed=%s)",
            dataset.kaggle_ref,
            result.manifest.version_id,
            result.changed,
        )
        return MaterializeResult(
            metadata=_metadata(
                dataset, result.manifest, result.version_dir, changed=result.changed
            ),
            data_version=DataVersion(result.manifest.version_id),
        )

    return _landing


def build_landing_check(dataset: SourceDataset) -> AssetChecksDefinition:
    @asset_check(
        asset=landing_key(dataset),
        name="files_match_manifest",
        blocking=True,
        description="Every landed file still matches the size and sha256 recorded in its manifest.",
    )
    def _check(landing: LandingZone) -> AssetCheckResult:
        current = landing.current_or_none(dataset)
        if current is None:
            return AssetCheckResult(
                passed=False, severity=AssetCheckSeverity.ERROR, description="not landed yet"
            )
        manifest, version_dir = current
        problems = verify_version(manifest, version_dir)
        return AssetCheckResult(
            passed=not problems,
            metadata={"version_id": manifest.version_id, "problems": MetadataValue.json(problems)},
        )

    return _check


landing_assets = [build_landing_asset(d) for d in ALL_DATASETS]
landing_checks = [build_landing_check(d) for d in ALL_DATASETS]
