"""End-to-end: producer -> Redpanda -> consumer -> Postgres -> dbt.

Needs Redpanda on $KAFKA_BOOTSTRAP and Postgres on $PG_ADMIN_DSN
(`docker compose up -d redpanda topics postgres`). Run: pytest -m integration
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import uuid
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from ridestream.config import Settings
from ridestream.consumer import StreamProcessor
from ridestream.producer import run_backfill

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
ADMIN_DSN = os.environ.get("PG_ADMIN_DSN", "postgresql://platform:platform@localhost:5432/warehouse")
DB = "it_pipeline"


@pytest.fixture(scope="module")
def env():
    try:
        psycopg.connect(ADMIN_DSN, connect_timeout=3).close()
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=5)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"infrastructure not running: {exc}")
    with psycopg.connect(ADMIN_DSN, autocommit=True) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{DB}" WITH (FORCE)')
        c.execute(f'CREATE DATABASE "{DB}"')
    dsn = ADMIN_DSN.rsplit("/", 1)[0] + "/" + DB
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute((ROOT / "sql" / "init.sql").read_text())
    topic = f"it.rides.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, 3, 1)])[topic].result()
    cfg = replace(
        Settings.from_env(),
        kafka_bootstrap=BOOTSTRAP,
        topic=topic,
        dlq_topic=topic + ".dlq",
        group_id="it-" + topic,
        pg_dsn=dsn,
        batch_size=500,
    )
    manifest = run_backfill(cfg, seed=5, minutes=15, rate=3.0, manifest=None)
    StreamProcessor(cfg).run(max_idle_s=3)
    yield cfg, manifest
    admin.delete_topics([topic])


def q(dsn: str, sql: str):
    with psycopg.connect(dsn) as c:
        return c.execute(sql).fetchone()


def test_every_valid_event_lands_exactly_once(env):
    cfg, m = env
    assert m["by_kind"]["duplicate"] > 0 and m["by_kind"]["malformed"] > 0
    assert q(cfg.pg_dsn, "SELECT count(*) FROM raw.ride_events")[0] == m["unique_valid_events"]
    assert q(cfg.pg_dsn, "SELECT count(*) FROM ops.dead_letters")[0] == m["by_kind"]["malformed"]
    assert q(cfg.pg_dsn, "SELECT sum(next_offset) FROM ops.consumer_offsets")[0] == m["messages_sent"]


def window_totals(dsn: str):
    return q(
        dsn,
        "SELECT sum(requests+assignments+starts+completions+cancellations), sum(gross_fare_cents) "
        "FROM stream.zone_minute_metrics",
    )


def test_windows_equal_on_time_raw_events(env):
    cfg, _ = env
    events, fare = window_totals(cfg.pg_dsn)
    on_time, on_time_fare = q(
        cfg.pg_dsn,
        "SELECT count(*), coalesce(sum(fare_cents) FILTER "
        "(WHERE event_type='ride_completed'),0) FROM raw.ride_events WHERE NOT is_late",
    )
    assert (events, fare) == (on_time, on_time_fare)


def test_full_replay_is_idempotent(env):
    cfg, _ = env
    before = window_totals(cfg.pg_dsn)
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as c:
        c.execute("UPDATE ops.consumer_offsets SET next_offset = 0")  # rewind everything
    StreamProcessor(cfg).run(max_idle_s=3)
    assert q(cfg.pg_dsn, "SELECT sum(next_offset) FROM ops.consumer_offsets")[0] > 0
    assert window_totals(cfg.pg_dsn) == before


@pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt not installed")
def test_dbt_build_passes_and_reconciliation_catches_drift(env):
    cfg, _ = env
    dbt_dir = ROOT / "dbt" / "ridestream"
    host, port = ADMIN_DSN.split("@")[1].split("/")[0].split(":")
    e = {**os.environ, "PG_DB": DB, "PG_HOST": host, "PG_PORT": port}
    args = [
        "--project-dir",
        str(dbt_dir),
        "--profiles-dir",
        str(dbt_dir),
        "--target-path",
        "/tmp/it-dbt-target",
        "--log-path",
        "/tmp/it-dbt-logs",
        "--vars",
        f"{{stream_topic: '{cfg.topic}'}}",
    ]
    ok = subprocess.run(["dbt", "build", *args], env=e, capture_output=True, text=True)
    assert ok.returncode == 0, ok.stdout[-3000:]
    # Second run is incremental and selects marts only, exactly like the Airflow
    # DAG. A missing `depends_on` hint in fct_rides used to fail only here.
    again = subprocess.run(
        ["dbt", "build", "--select", "marts", *args], env=e, capture_output=True, text=True
    )
    assert again.returncode == 0, again.stdout[-3000:]
    checked = q(cfg.pg_dsn, "SELECT count(*) FROM marts.recon_stream_vs_batch")[0]
    assert checked > 100  # the reconciliation actually compared windows
    with psycopg.connect(cfg.pg_dsn, autocommit=True) as c:
        c.execute(
            "UPDATE stream.zone_minute_metrics SET requests = requests + 1 "
            "WHERE window_start = (SELECT min(window_start) FROM stream.zone_minute_metrics)"
        )
    time.sleep(0.1)
    bad = subprocess.run(
        ["dbt", "build", "--select", "recon_stream_vs_batch+", *args], env=e, capture_output=True, text=True
    )
    assert bad.returncode != 0
    assert "assert_stream_matches_batch" in bad.stdout
