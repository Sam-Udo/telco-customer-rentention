"""
Integration Test Runner — Executes validation notebooks on Databricks
Called by: CD pipeline after DEV deployment
Verifies: Bronze and Silver pipeline runs without errors on DEV cluster
"""

import json
import os
import sys
import time
import urllib.request

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
TARGET_ENV = os.environ.get("TARGET_ENV", "dev")
WORKSPACE_PATH = f"/Workspace/telco-churn-{TARGET_ENV}"

VALIDATION_NOTEBOOKS = [
    f"{WORKSPACE_PATH}/notebooks/01_bronze_ingestion",
    f"{WORKSPACE_PATH}/notebooks/02_silver_cleaning",
]

CLUSTER_SPEC = {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_D4s_v3",
    "num_workers": 1,
    "spark_conf": {"spark.sql.adaptive.enabled": "true"},
    "custom_tags": {"purpose": "integration-test", "environment": TARGET_ENV},
}

TIMEOUT_SECONDS = 3600


def _api(method, path, body=None):
    """Call Databricks REST API."""
    url = f"{DATABRICKS_HOST}/api/2.0{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {DATABRICKS_TOKEN}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def run_notebook(notebook_path: str) -> bool:
    """Submit a notebook as a one-time run and poll for completion."""
    print(f"\n  Running: {notebook_path}")

    result = _api("POST", "/jobs/runs/submit", {
        "run_name": f"integration-test-{os.path.basename(notebook_path)}",
        "tasks": [
            {
                "task_key": "test_task",
                "new_cluster": CLUSTER_SPEC,
                "notebook_task": {"notebook_path": notebook_path},
            }
        ],
    })
    run_id = result["run_id"]
    print(f"  Run ID: {run_id}")

    start = time.time()
    while time.time() - start < TIMEOUT_SECONDS:
        status = _api("GET", f"/jobs/runs/get?run_id={run_id}")
        state = status["state"]["life_cycle_state"]

        if state in ("TERMINATED", "SKIPPED"):
            result_state = status["state"].get("result_state", "UNKNOWN")
            if result_state == "SUCCESS":
                elapsed = int(time.time() - start)
                print(f"  PASS ({elapsed}s)")
                return True
            else:
                print(f"  FAIL — Result: {result_state}")
                msg = status["state"].get("state_message", "")
                if msg:
                    print(f"  Message: {msg}")
                return False

        if state == "INTERNAL_ERROR":
            print(f"  FAIL — Internal error: {status['state'].get('state_message', '')}")
            return False

        time.sleep(15)

    print(f"  FAIL — Timeout after {TIMEOUT_SECONDS}s")
    return False


def main():
    print("=" * 60)
    print(f"  Integration Tests — {TARGET_ENV.upper()}")
    print(f"  Workspace: {DATABRICKS_HOST}")
    print("=" * 60)

    results = {}
    all_passed = True

    for notebook in VALIDATION_NOTEBOOKS:
        passed = run_notebook(notebook)
        results[notebook] = passed
        if not passed:
            all_passed = False
            print(f"\n  Stopping — {notebook} failed")
            break

    print("\n" + "=" * 60)
    print("  Integration Test Results:")
    print("=" * 60)
    for nb, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {os.path.basename(nb)}")

    if not all_passed:
        print("\nINTEGRATION TESTS FAILED — blocking deployment to next stage")
        sys.exit(1)
    else:
        print("\nAll integration tests passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
