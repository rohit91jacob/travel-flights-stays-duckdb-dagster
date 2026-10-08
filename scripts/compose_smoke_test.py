"""Smoke-test a running Docker Compose deployment through the Dagster GraphQL API.

Checks that the code location is loaded and the daemons are healthy, then launches the
ingest and both partitioned loads (full ranges) through the run queue and waits for the
`transform_on_raw_update` sensor to trigger a successful dbt transform on its own.
Standard library only, so it runs on a bare CI runner.

    python3 scripts/compose_smoke_test.py --url http://localhost:3000
"""

import argparse
import json
import sys
import time
import urllib.request

REPOSITORY = "__repository__"
FLIGHTS = {
    "dagster/asset_partition_range_start": "2022-02-11",
    "dagster/asset_partition_range_end": "2022-03-31",
}
STAYS = {
    "dagster/asset_partition_range_start": "2013-06-24",
    "dagster/asset_partition_range_end": "2017-09-14",
}
TERMINAL = {"SUCCESS", "FAILURE", "CANCELED"}


class Dagster:
    def __init__(self, url: str) -> None:
        self.endpoint = url.rstrip("/") + "/graphql"
        self.location = ""

    def query(self, query: str, variables: dict | None = None) -> dict:
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        request = urllib.request.Request(
            self.endpoint, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=180) as response:
            payload = json.load(response)
        if payload.get("errors"):
            raise RuntimeError(payload["errors"])
        return payload["data"]

    def launch(self, job: str, tags: dict[str, str] | None = None) -> str:
        data = self.query(
            """
            mutation Launch($params: ExecutionParams!) {
              launchRun(executionParams: $params) {
                __typename
                ... on LaunchRunSuccess { run { runId } }
                ... on PythonError { message }
                ... on InvalidSubsetError { message }
              }
            }
            """,
            {
                "params": {
                    "selector": {
                        "repositoryLocationName": self.location,
                        "repositoryName": REPOSITORY,
                        "jobName": job,
                    },
                    "runConfigData": {},
                    "executionMetadata": {
                        "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()]
                    },
                }
            },
        )["launchRun"]
        if data["__typename"] != "LaunchRunSuccess":
            raise RuntimeError(f"could not launch {job}: {data}")
        return data["run"]["runId"]

    def status(self, run_id: str) -> str:
        return self.query(
            "query($id: ID!) { runOrError(runId: $id) { ... on Run { status } } }", {"id": run_id}
        )["runOrError"]["status"]

    def wait(self, run_id: str, label: str, timeout: float = 900) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.status(run_id)
            if status in TERMINAL:
                print(f"{label}: {status}")
                if status != "SUCCESS":
                    raise SystemExit(f"{label} finished with {status}")
                return
            time.sleep(5)
        raise SystemExit(f"{label} did not finish within {timeout}s")

    def runs(self, job: str) -> list[dict]:
        return self.query(
            "query($job: String!) { runsOrError(filter: {pipelineName: $job}) { ... on Runs { results { runId status } } } }",
            {"job": job},
        )["runsOrError"]["results"]


def wait_until_ready(dagster: Dagster, timeout: float = 300) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            data = dagster.query(
                """
                {
                  workspaceOrError { ... on Workspace { locationEntries { name loadStatus
                    locationOrLoadError { __typename ... on PythonError { message } } } } }
                  instance { daemonHealth { allDaemonStatuses { daemonType healthy required } } }
                }
                """
            )
            entries = data["workspaceOrError"]["locationEntries"]
            loaded_names = [
                e["name"]
                for e in entries
                if e["loadStatus"] == "LOADED"
                and e["locationOrLoadError"]["__typename"] == "RepositoryLocation"
            ]
            loaded = len(loaded_names) == 1
            if loaded:
                dagster.location = loaded_names[0]
            unhealthy = [
                d["daemonType"]
                for d in data["instance"]["daemonHealth"]["allDaemonStatuses"]
                if d["required"] and not d["healthy"]
            ]
            last = {"loaded": loaded, "unhealthy_daemons": unhealthy, "entries": entries}
            if loaded and not unhealthy:
                print(f"code location {dagster.location!r} loaded; daemons healthy")
                return
        except (OSError, RuntimeError) as error:
            last = str(error)
        time.sleep(5)
    raise SystemExit(f"deployment not ready: {last}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://localhost:3000")
    args = parser.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    dagster = Dagster(args.url)

    wait_until_ready(dagster)
    dagster.wait(dagster.launch("ingest_job"), "ingest_job")
    dagster.wait(dagster.launch("flights_load_job", FLIGHTS), "flights_load_job (all partitions)")
    dagster.wait(dagster.launch("stays_load_job", STAYS), "stays_load_job (all partitions)")

    # No manual launch: the transform must be triggered by the sensor via the daemon.
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        runs = dagster.runs("transform_job")
        if any(r["status"] == "SUCCESS" for r in runs):
            print(f"transform_job triggered by sensor and succeeded ({len(runs)} run(s))")
            return
        if runs and all(r["status"] in {"FAILURE", "CANCELED"} for r in runs):
            raise SystemExit(f"transform_job failed: {runs}")
        time.sleep(10)
    raise SystemExit("transform_job was not triggered/completed by the sensor in time")


if __name__ == "__main__":
    sys.exit(main())
