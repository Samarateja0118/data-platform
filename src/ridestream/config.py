"""Runtime settings, all overridable through environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:19092"
    topic: str = "rides.events"
    dlq_topic: str = "rides.events.dlq"
    group_id: str = "ridestream-windows"
    pg_dsn: str = "postgresql://platform:platform@localhost:5432/warehouse"
    allowed_lateness: timedelta = timedelta(seconds=30)
    batch_size: int = 2000
    poll_timeout_s: float = 0.2
    session_timeout_ms: int = 10000

    @classmethod
    def from_env(cls) -> Settings:
        e = os.environ.get
        return cls(
            kafka_bootstrap=e("KAFKA_BOOTSTRAP", cls.kafka_bootstrap),
            topic=e("TOPIC", cls.topic),
            dlq_topic=e("DLQ_TOPIC", cls.dlq_topic),
            group_id=e("GROUP_ID", cls.group_id),
            pg_dsn=e("PG_DSN", cls.pg_dsn),
            allowed_lateness=timedelta(seconds=float(e("ALLOWED_LATENESS_S", "30"))),
            batch_size=int(e("BATCH_SIZE", str(cls.batch_size))),
            poll_timeout_s=float(e("POLL_TIMEOUT_S", str(cls.poll_timeout_s))),
            session_timeout_ms=int(e("SESSION_TIMEOUT_MS", str(cls.session_timeout_ms))),
        )
