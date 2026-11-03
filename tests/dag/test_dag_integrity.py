"""DAG integrity: imports cleanly, has the expected shape, no cycles."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_task_order(bag):
    dag = bag.get_dag("ridestream_elt")
    order = [
        "stream_health_gate",
        "dbt_source_freshness",
        "dbt_seed",
        "dbt_build_staging",
        "dbt_build_marts",
        "record_reconciliation",
    ]
    assert set(dag.task_ids) == set(order)
    for up, down in zip(order, order[1:], strict=False):
        assert down in dag.get_task(up).downstream_task_ids
    assert dag.max_active_runs == 1
    assert dag.catchup is False
