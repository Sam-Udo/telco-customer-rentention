"""
Integration Test Runner — Executes a validation notebook on Databricks
Called by: CD pipeline after DEV deployment
Verifies: Bronze → Silver → Gold pipeline runs without errors on DEV cluster
"""

import os
import sys
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, RunLifeCycleState

# ── Config ──
DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
TARGET_ENV = os.environ.get("TARGET_ENV", "dev")
WORKSPACE_PATH = f"/Repos/telco-churn-{TARGET_ENV}"

# ── Notebooks to validate (run in order) ──
VALIDATION_NOTEBOOKS = [
    f"{WORKSPACE_PATH}/notebooks/01_bronze_ingestion",
    f"{WORKSPACE_PATH}/notebooks/02_silver_cleaning",
]

# ── Cluster config for integration tests (small, ephemeral) ──
CLUSTER_SPEC = {
    "spark_version": "14.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 1,
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
    },
    "custom_tags": {
        "purpose": "integration-test",
        "environment": TARGET_ENV,
    },
}

TIMEOUT_SECONDS = 1800  # 30 min max per notebook


def run_notebook(w: WorkspaceClient, notebook_path: str) -> bool:
    """Run a notebook as a one-time job and wait for completion."""
    print(f"\n  Running: {notebook_path}")

    run = w.jobs.submit(
        run_name=f"integration-test-{os.path.basename(notebook_path)}",
        tasks=[
            {
                "task_key": "test_task",
                "new_cluster": CLUSTER_SPEC,
                "notebook_task": NotebookTask(
                    notebook_path=notebook_path,
                ),
            }
        ],
    )

    run_id = run.bind()["run_id"]
    print(f"  Run ID: {run_id}")

    # Poll for completion
    start = time.time()
    while time.time() - start < TIMEOUT_SECONDS:
        status = w.jobs.get_run(run_id)
        state = status.state.life_cycle_state

        if state in (RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED):
            result = status.state.result_state
            if result and result.value == "SUCCESS":
                elapsed = int(time.time() - start)
                print(f"  PASS ({elapsed}s)")
                return True
            else:
                print(f"  FAIL — Result: {result}")
                if status.state.state_message:
                    print(f"  Message: {status.state.state_message}")
                return False

        if state == RunLifeCycleState.INTERNAL_ERROR:
            print(f"  FAIL — Internal error: {status.state.state_message}")
            return False

        time.sleep(15)

    print(f"  FAIL — Timeout after {TIMEOUT_SECONDS}s")
    return False


def main():
    print("=" * 60)
    print(f"  Integration Tests — {TARGET_ENV.upper()}")
    print(f"  Workspace: {DATABRICKS_HOST}")
    print("=" * 60)

    w = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
    )

    results = {}
    all_passed = True

    for notebook in VALIDATION_NOTEBOOKS:
        passed = run_notebook(w, notebook)
        results[notebook] = passed
        if not passed:
            all_passed = False
            # Stop on first failure
            print(f"\n  Stopping — {notebook} failed")
            break

    # Summary
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
