"""Batch ELT over the streaming landing zone.

Every 15 minutes:
  stream health gate -> source freshness -> seeds -> staging -> marts (+tests)
  -> record stream-vs-batch reconciliation

dbt tests run inside `dbt build`, so a failing data test stops downstream
models from being published, and the DAG run goes red.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import psycopg2
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DBT = os.environ.get("DBT_BIN", "/opt/dbt-venv/bin/dbt")
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_FLAGS = (
    f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target-path /tmp/dbt-target --log-path /tmp/dbt-logs"
)
PG_DSN = os.environ.get("PG_DSN", "postgresql://platform:platform@localhost:5432/warehouse")
MAX_CONSUMER_STALENESS = timedelta(minutes=5)


def _query(sql: str, params: tuple = ()) -> list[tuple]:
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else []


@dag(
    dag_id="ridestream_elt",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["elt", "dbt"],
)
def ridestream_elt():
    @task
    def stream_health_gate() -> dict:
        """Refuse to publish marts from a stalled stream."""
        rows = _query(
            "SELECT count(*), max(updated_at), now() - max(updated_at) FROM ops.consumer_offsets "
            "WHERE topic = 'rides.events'"
        )
        n, last, lag = rows[0]
        if n == 0:
            raise RuntimeError("consumer has never committed; is it running?")
        if lag > MAX_CONSUMER_STALENESS:
            raise RuntimeError(f"consumer last committed {lag} ago (limit {MAX_CONSUMER_STALENESS})")
        return {"partitions": n, "last_commit": last.isoformat()}

    freshness = BashOperator(
        task_id="dbt_source_freshness", bash_command=f"{DBT} source freshness {DBT_FLAGS}"
    )
    seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT} seed {DBT_FLAGS}")
    staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=f"{DBT} build --select staging intermediate {DBT_FLAGS}",
    )
    marts = BashOperator(task_id="dbt_build_marts", bash_command=f"{DBT} build --select marts {DBT_FLAGS}")

    @task
    def record_reconciliation() -> dict:
        (checked, matched, late, max_diff) = _query(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE diff_events = 0 AND diff_fare_cents = 0),
                   coalesce(sum(late_events), 0),
                   coalesce(max(abs(diff_events)), 0)
            FROM marts.recon_stream_vs_batch
            """
        )[0]
        passed = checked == matched
        _query(
            "INSERT INTO ops.reconciliation_runs "
            "(windows_checked, windows_matched, late_events, max_abs_diff, passed) VALUES (%s,%s,%s,%s,%s)",
            (checked, matched, late, max_diff, passed),
        )
        if not passed:
            raise RuntimeError(f"{checked - matched} windows disagree between stream and batch")
        return {"windows_checked": checked, "late_events": late}

    stream_health_gate() >> freshness >> seed >> staging >> marts >> record_reconciliation()


ridestream_elt()
