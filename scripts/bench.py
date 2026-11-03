"""Reproducible measurements for the README.

    python scripts/bench.py throughput   # consumer msg/s vs batch size
    python scripts/bench.py chaos        # kill -9 the consumer repeatedly, verify exact results
    python scripts/bench.py latency      # produce->queryable latency from the live stack

Each benchmark uses its own topic and database so it never touches the demo data.
Results are appended as JSON lines to results/bench.jsonl.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
ADMIN_DSN = os.environ.get("PG_ADMIN_DSN", "postgresql://platform:platform@localhost:5432/warehouse")
RESULTS = ROOT / "results" / "bench.jsonl"


def dsn_for(db: str) -> str:
    return ADMIN_DSN.rsplit("/", 1)[0] + "/" + db


def fresh_db(db: str) -> str:
    with psycopg.connect(ADMIN_DSN, autocommit=True) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
        c.execute(f'CREATE DATABASE "{db}"')
    sql = (ROOT / "sql" / "init.sql").read_text()
    with psycopg.connect(dsn_for(db), autocommit=True) as c:
        c.execute(sql)
    return dsn_for(db)


def fresh_topic(name: str, partitions: int = 6) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if name in admin.list_topics(timeout=10).topics:
        admin.delete_topics([name])[name].result()
        time.sleep(2)
    admin.create_topics([NewTopic(name, partitions, 1)])[name].result()


def env(topic: str, dsn: str, **extra: str) -> dict:
    e = {
        **os.environ,
        "KAFKA_BOOTSTRAP": BOOTSTRAP,
        "TOPIC": topic,
        "PG_DSN": dsn,
        "DLQ_TOPIC": topic + ".dlq",
        "GROUP_ID": f"bench-{topic}-{time.time_ns()}",
    }
    e.update(extra)
    return e


def produce(topic: str, dsn: str, minutes: float, rate: float, seed: int = 11) -> dict:
    mf = ROOT / "results" / f"manifest-{topic}.json"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ridestream.producer",
            "backfill",
            "--seed",
            str(seed),
            "--minutes",
            str(minutes),
            "--rides-per-sec",
            str(rate),
            "--manifest",
            str(mf),
        ],
        env=env(topic, dsn),
        check=True,
        capture_output=True,
    )
    return json.loads(mf.read_text())


def counts(dsn: str) -> dict:
    with psycopg.connect(dsn) as c:
        raw, late = c.execute(
            "SELECT count(*), count(*) FILTER (WHERE is_late) FROM raw.ride_events"
        ).fetchone()
        dlq = c.execute("SELECT count(*) FROM ops.dead_letters").fetchone()[0]
        win = c.execute(
            "SELECT coalesce(sum(requests+assignments+starts+completions+cancellations),0) "
            "FROM stream.zone_minute_metrics"
        ).fetchone()[0]
        fare_s = c.execute(
            "SELECT coalesce(sum(gross_fare_cents),0) FROM stream.zone_minute_metrics"
        ).fetchone()[0]
        fare_b = c.execute(
            "SELECT coalesce(sum(fare_cents),0) FROM raw.ride_events "
            "WHERE NOT is_late AND event_type='ride_completed'"
        ).fetchone()[0]
    return {
        "raw_events": raw,
        "late": late,
        "dead_letters": dlq,
        "window_event_sum": int(win),
        "stream_fare": int(fare_s),
        "batch_on_time_fare": int(fare_b),
    }


def record(entry: dict) -> None:
    entry["at"] = datetime.now(UTC).isoformat(timespec="seconds")
    RESULTS.parent.mkdir(exist_ok=True)
    with RESULTS.open("a") as f:
        f.write(json.dumps(entry) + "\n")
    print(json.dumps(entry, indent=2))


def consume_until_done(topic: str, dsn: str, expected_msgs: int, batch: int) -> float:
    """Run the consumer and return seconds from first to last committed batch."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "ridestream.consumer", "--exit-when-idle", "3"],
        env=env(topic, dsn, BATCH_SIZE=str(batch)),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    proc.wait(timeout=1800)
    with psycopg.connect(dsn) as c:
        first, last = c.execute("SELECT min(ingested_at), max(ingested_at) FROM raw.ride_events").fetchone()
        n = c.execute("SELECT sum(next_offset) FROM ops.consumer_offsets").fetchone()[0]
    assert n == expected_msgs, f"consumed {n} of {expected_msgs}"
    return (last - first).total_seconds()


def throughput(a) -> None:
    for batch in a.batches:
        topic = f"bench_tp_{batch}"
        db = f"bench_tp_{batch}"
        dsn = fresh_db(db)
        fresh_topic(topic)
        minutes = a.minutes if batch >= 50 else a.minutes / 10  # per-message txns are slow
        m = produce(topic, dsn, minutes, a.rate)
        secs = consume_until_done(topic, dsn, m["messages_sent"], batch)
        c = counts(dsn)
        record(
            {
                "bench": "throughput",
                "batch_size": batch,
                "messages": m["messages_sent"],
                "consume_seconds": round(secs, 2),
                "msgs_per_sec": round(m["messages_sent"] / secs),
                "producer_msgs_per_sec": m["publish_rate_msg_s"],
                "exact": c["raw_events"] == m["unique_valid_events"]
                and c["dead_letters"] == m["by_kind"].get("malformed", 0),
            }
        )


def chaos(a) -> None:
    topic, db = "bench_chaos", "bench_chaos"
    dsn = fresh_db(db)
    fresh_topic(topic)
    m = produce(topic, dsn, a.minutes, a.rate)
    e = env(topic, dsn, BATCH_SIZE="200")
    kills = 0
    for _ in range(a.kills):
        p = subprocess.Popen(
            [sys.executable, "-m", "ridestream.consumer"],
            env=e,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(a.kill_after)
        if p.poll() is None:
            p.send_signal(signal.SIGKILL)  # no cleanup, no offset flush
            p.wait()
            kills += 1
        with psycopg.connect(dsn) as c:
            done = c.execute("SELECT coalesce(sum(next_offset),0) FROM ops.consumer_offsets").fetchone()[0]
        print(f"after kill {kills}: {done}/{m['messages_sent']} messages committed", flush=True)
    subprocess.run(
        [sys.executable, "-m", "ridestream.consumer", "--exit-when-idle", "3"],
        env=e,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    c = counts(dsn)
    record(
        {
            "bench": "chaos",
            "sigkills": kills,
            "messages": m["messages_sent"],
            "expected_unique_valid": m["unique_valid_events"],
            "expected_malformed": m["by_kind"].get("malformed", 0),
            "duplicates_injected": m["by_kind"].get("duplicate", 0),
            **c,
            "raw_exact": c["raw_events"] == m["unique_valid_events"],
            "dlq_exact": c["dead_letters"] == m["by_kind"].get("malformed", 0),
            "windows_exact": c["window_event_sum"] == c["raw_events"] - c["late"]
            and c["stream_fare"] == c["batch_on_time_fare"],
        }
    )


def latency(a) -> None:
    """Reads the live demo stack (compose producer + consumer)."""
    with psycopg.connect(ADMIN_DSN) as c:
        rows = c.execute(
            "SELECT extract(epoch FROM ingested_at - kafka_ts) * 1000 FROM raw.ride_events "
            "WHERE ingested_at > now() - make_interval(secs => %s)",
            (a.window_s,),
        ).fetchall()
        rate = c.execute(
            "SELECT count(*) / %s::float FROM raw.ride_events "
            "WHERE ingested_at > now() - make_interval(secs => %s)",
            (a.window_s, a.window_s),
        ).fetchone()[0]
    ms = sorted(float(r[0]) for r in rows)
    q = statistics.quantiles(ms, n=100)
    record(
        {
            "bench": "latency",
            "label": a.label,
            "events": len(ms),
            "events_per_sec": round(rate, 1),
            "window_s": a.window_s,
            "p50_ms": round(q[49], 1),
            "p95_ms": round(q[94], 1),
            "p99_ms": round(q[98], 1),
            "max_ms": round(ms[-1], 1),
        }
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("throughput")
    t.add_argument("--batches", type=int, nargs="+", default=[1, 100, 500, 2000, 5000])
    t.add_argument("--minutes", type=float, default=60)
    t.add_argument("--rate", type=float, default=10)
    c = sub.add_parser("chaos")
    c.add_argument("--minutes", type=float, default=60)
    c.add_argument("--rate", type=float, default=10)
    c.add_argument("--kills", type=int, default=5)
    c.add_argument("--kill-after", type=float, default=4)
    lt = sub.add_parser("latency")
    lt.add_argument("--window-s", type=int, default=300)
    lt.add_argument("--label", default="")
    a = ap.parse_args()
    {"throughput": throughput, "chaos": chaos, "latency": latency}[a.cmd](a)


if __name__ == "__main__":
    main()
