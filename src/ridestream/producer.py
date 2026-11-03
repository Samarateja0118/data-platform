"""Publish simulated ride events to Redpanda.

Two modes:

* ``live``      event time follows the wall clock; use for the demo and for
                end-to-end latency measurements.
* ``backfill``  simulate N minutes of history and publish it as fast as the
                broker accepts; use for throughput benchmarks and CI.

Both write a manifest (ground-truth counts) so tests can check that Postgres
ended up with exactly the unique valid events that were sent.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Producer

from .config import Settings
from .generator import Emission, FaultInjector, RideSimulator

log = logging.getLogger("ridestream.producer")


class Publisher:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.p = Producer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap,
                "enable.idempotence": True,  # broker drops producer retries
                "acks": "all",
                "linger.ms": 20,
                "batch.size": 262144,
                "compression.type": "lz4",
            }
        )
        self.sent = 0
        self.failed = 0
        self.valid_ids: set[str] = set()

    def _cb(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)

    def send(self, ems: list[Emission]) -> None:
        for em in ems:
            while True:
                try:
                    self.p.produce(self.cfg.topic, em.value, key=em.key, on_delivery=self._cb)
                    break
                except BufferError:
                    self.p.poll(0.05)
            if em.event is not None:
                self.valid_ids.add(em.event.event_id)
            self.sent += 1
        self.p.poll(0)

    def flush(self) -> None:
        self.p.flush(30)


def write_manifest(path: str | None, pub: Publisher, inj: FaultInjector, extra: dict) -> dict:
    manifest = {
        "messages_sent": pub.sent,
        "delivery_failures": pub.failed,
        "unique_valid_events": len(pub.valid_ids),
        "by_kind": dict(inj.counts),
        **extra,
    }
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(manifest, indent=2))
    return manifest


def run_backfill(cfg: Settings, seed: int, minutes: float, rate: float, manifest: str | None) -> dict:
    start = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
    sim = RideSimulator(seed, start, rides_per_sec=rate)
    inj = FaultInjector(seed, cfg.allowed_lateness.total_seconds())
    pub = Publisher(cfg)
    t0 = time.perf_counter()
    step = timedelta(seconds=10)
    clock = start
    end = start + timedelta(minutes=minutes)
    while clock < end:
        clock += step
        inj.feed(sim.advance(clock))
        pub.send(inj.release(clock))
    pub.send(inj.release(None))  # drain held late/out-of-order events
    pub.flush()
    secs = time.perf_counter() - t0
    m = write_manifest(
        manifest,
        pub,
        inj,
        {
            "mode": "backfill",
            "seed": seed,
            "sim_minutes": minutes,
            "publish_seconds": round(secs, 2),
            "publish_rate_msg_s": round(pub.sent / secs),
        },
    )
    log.info("backfill done %s", m)
    return m


def run_live(cfg: Settings, seed: int, rate: float, duration: float | None, manifest: str | None) -> dict:
    sim = RideSimulator(seed, datetime.now(UTC), rides_per_sec=rate, run_id=uuid.uuid4().hex[:8])
    inj = FaultInjector(seed, cfg.allowed_lateness.total_seconds())
    pub = Publisher(cfg)
    stop = {"flag": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    t_end = None if duration is None else time.monotonic() + duration
    last_log = time.monotonic()
    while not stop["flag"] and (t_end is None or time.monotonic() < t_end):
        now = datetime.now(UTC)
        inj.feed(sim.advance(now))
        pub.send(inj.release(now))
        if time.monotonic() - last_log > 10:
            log.info("sent=%d kinds=%s", pub.sent, dict(inj.counts))
            last_log = time.monotonic()
        time.sleep(0.05)
    pub.flush()
    return write_manifest(manifest, pub, inj, {"mode": "live", "seed": seed, "rides_per_sec": rate})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["live", "backfill"])
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--rides-per-sec", type=float, default=5.0)
    ap.add_argument("--minutes", type=float, default=60, help="backfill: simulated minutes")
    ap.add_argument("--duration", type=float, default=None, help="live: wall seconds (default forever)")
    ap.add_argument("--manifest", default=None)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Settings.from_env()
    if a.mode == "backfill":
        run_backfill(cfg, a.seed, a.minutes, a.rides_per_sec, a.manifest)
    else:
        run_live(cfg, a.seed, a.rides_per_sec, a.duration, a.manifest)


if __name__ == "__main__":
    main()
