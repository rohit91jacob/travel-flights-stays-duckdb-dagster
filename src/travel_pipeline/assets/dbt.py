"""dbt models, snapshots, seeds and tests as Dagster assets and asset checks."""

import os
from collections.abc import Iterator, Mapping
from typing import Any

from dagster import AssetExecutionContext, AssetSpec, Failure
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from travel_pipeline.policies import DAILY_FRESHNESS, DUCKDB_POOL
from travel_pipeline.settings import DBT_PROJECT_DIR

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    target=os.environ.get("DBT_TARGET", "dev"),
)
# Under `dagster dev` the manifest is (re)built on load; elsewhere it must be built ahead of
# time (`make dbt-parse`, the Docker build and CI all do this).
dbt_project.prepare_if_dev()


class TravelDbtTranslator(DagsterDbtTranslator):
    """Groups assets by layer and puts freshness policies on the marts."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        resource_type = dbt_resource_props["resource_type"]
        fqn = dbt_resource_props["fqn"]
        if resource_type == "seed":
            return "reference"
        if resource_type == "snapshot":
            return "snapshots"
        if len(fqn) > 3 and fqn[1] == "marts":
            return f"marts_{fqn[2]}"
        return fqn[1] if len(fqn) > 2 else "dbt"

    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: DbtProject | None
    ) -> AssetSpec:
        spec = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        if props["resource_type"] == "model" and props["fqn"][1] == "marts":
            spec = spec.replace_attributes(freshness_policy=DAILY_FRESHNESS)
        return spec


def _enforce_source_freshness() -> bool:
    return os.environ.get("TRAVEL_ENFORCE_SOURCE_FRESHNESS", "true").strip().lower() not in {
        "0",
        "false",
        "no",
    }


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=TravelDbtTranslator(),
    pool=DUCKDB_POOL,
    name="travel_dbt",
)
def travel_dbt(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
    """`dbt source freshness`, then `dbt build` (seeds, snapshots, models and tests in DAG order)."""
    if _enforce_source_freshness():
        freshness = dbt.cli(["source", "freshness"], raise_on_error=False).wait()
        if not freshness.is_successful():
            raise Failure(
                description="dbt source freshness failed: raw data is older than error_after.",
                metadata={
                    "hint": "load the missing partitions, or set TRAVEL_ENFORCE_SOURCE_FRESHNESS=false"
                },
            )
    yield from dbt.cli(["build"], context=context).stream()
