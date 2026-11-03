"""Stream processor: Redpanda -> validation -> windowed aggregates in Postgres.

Run: ``python -m ridestream.consumer``  (settings come from env, see config.py)
"""

from __future__ import annotations

import logging
import signal
import time
from datetime import UTC, datetime

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, Producer, TopicPartition

from . import sink
from .config import Settings
from .schema import InvalidEvent, parse_event
from .windows import WatermarkTracker, is_late

log = logging.getLogger("ridestream.consumer")


class StreamProcessor:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.conn = psycopg.connect(cfg.pg_dsn, autocommit=True)
        sink.prepare(self.conn)
        self.tracker = WatermarkTracker(cfg.allowed_lateness)
        self.owned: set[int] = set()
        self.n_partitions: int | None = None
        self.running = True
        self.consumer = Consumer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap,
                "group.id": cfg.group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
                "fetch.min.bytes": 1,
                "fetch.wait.max.ms": 100,
                "session.timeout.ms": cfg.session_timeout_ms,
            }
        )
        self.dlq = Producer({"bootstrap.servers": cfg.kafka_bootstrap, "linger.ms": 50})
        self.totals = {"received": 0, "inserted": 0, "duplicates": 0, "late": 0, "dead_letters": 0}

    # Offsets come from Postgres, not Kafka: seek to what the last committed
    # transaction recorded so a crash can never skip or double-apply a batch.
    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        md = consumer.list_topics(self.cfg.topic, timeout=10)
        self.n_partitions = len(md.topics[self.cfg.topic].partitions)
        stored = sink.load_offsets(self.conn, self.cfg.group_id, self.cfg.topic)
        for tp in partitions:
            nxt, wm = stored.get(tp.partition, (OFFSET_BEGINNING, None))
            tp.offset = nxt
            self.tracker.restore(tp.partition, wm)
            self.owned.add(tp.partition)
        log.info("assigned %s", {tp.partition: tp.offset for tp in partitions})
        consumer.incremental_assign(partitions)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        for tp in partitions:
            self.owned.discard(tp.partition)
            self.tracker.forget(tp.partition)
        log.info("revoked %s", [tp.partition for tp in partitions])
        consumer.incremental_unassign(partitions)

    def process(self, msgs) -> sink.BatchResult | None:
        others = sink.other_watermarks(self.conn, self.cfg.group_id, self.cfg.topic, self.owned)
        mine = {p: self.tracker.watermark(p) for p in self.owned}
        gwm = WatermarkTracker.global_watermark({**others, **mine}, self.n_partitions)

        rows, dead, offsets = [], [], {}
        for m in msgs:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka error: %s", m.error())
                continue
            p, off = m.partition(), m.offset()
            offsets[p] = off + 1
            kafka_ts = datetime.fromtimestamp(m.timestamp()[1] / 1000, tz=UTC)
            try:
                e = parse_event(m.value())
            except InvalidEvent as exc:
                payload = (m.value() or b"").decode("utf-8", "replace")
                dead.append((p, off, payload, str(exc)))
                self.dlq.produce(self.cfg.dlq_topic, m.value(), key=m.key(), headers={"error": str(exc)})
                continue
            self.tracker.observe(p, e.event_time)
            rows.append(
                (
                    e.event_id,
                    e.event_type,
                    e.ride_id,
                    e.event_time,
                    e.zone_id,
                    e.rider_id,
                    e.driver_id,
                    e.fare_cents,
                    e.distance_m,
                    e.cancel_reason,
                    is_late(e.event_time, gwm),
                    p,
                    off,
                    kafka_ts,
                )
            )
        if not offsets:
            return None
        offs = {p: (nxt, self.tracker.watermark(p)) for p, nxt in offsets.items()}
        result = sink.write_batch(self.conn, self.cfg.group_id, self.cfg.topic, rows, dead, offs)
        self.dlq.poll(0)
        # Mirror progress to Kafka only so `rpk group describe` shows lag.
        self.consumer.commit(
            offsets=[TopicPartition(self.cfg.topic, p, n) for p, n in offsets.items()],
            asynchronous=True,
        )
        return result

    def run(self, max_idle_s: float | None = None) -> dict:
        self.consumer.subscribe([self.cfg.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
        last_log = time.monotonic()
        idle_since: float | None = None  # idle timer starts once partitions are assigned
        window = {"received": 0}
        while self.running:
            msgs = self.consumer.consume(num_messages=self.cfg.batch_size, timeout=self.cfg.poll_timeout_s)
            res = self.process(msgs) if msgs else None
            now = time.monotonic()
            if idle_since is None and self.owned:
                idle_since = now
            if res:
                idle_since = now
                for k in self.totals:
                    self.totals[k] += getattr(res, k)
                window["received"] += res.received
            elif max_idle_s is not None and idle_since is not None and now - idle_since > max_idle_s:
                log.info("idle for %.0fs, exiting", max_idle_s)
                break
            if now - last_log >= 5:
                log.info("rate=%.0f msg/s totals=%s", window["received"] / (now - last_log), self.totals)
                window["received"] = 0
                last_log = now
        self.close()
        return self.totals

    def stop(self, *_: object) -> None:
        self.running = False

    def close(self) -> None:
        self.dlq.flush(5)
        self.consumer.close()
        self.conn.close()


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--exit-when-idle",
        type=float,
        default=None,
        help="stop after N seconds without messages (benchmarks, CI)",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    proc = StreamProcessor(Settings.from_env())
    signal.signal(signal.SIGTERM, proc.stop)
    signal.signal(signal.SIGINT, proc.stop)
    totals = proc.run(max_idle_s=args.exit_when_idle)
    log.info("final totals %s", totals)


if __name__ == "__main__":
    main()
