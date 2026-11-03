"""Event-time windowing rules shared by the consumer, tests and docs.

* Tumbling 1-minute windows per zone, keyed by event time (not arrival time).
* Each partition's watermark is ``max(event_time seen) - allowed_lateness``.
* The global watermark is the minimum over all partitions, so one slow (or
  not-yet-seen) partition holds windows open instead of letting them close early.
* A window is closed once ``window_end <= global watermark``. An event that
  lands in a closed window is *late*: it is still stored in raw (so the batch
  layer counts it), but it is not added to the streaming aggregate.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

WINDOW = timedelta(minutes=1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def window_start(ts: datetime) -> datetime:
    return ts - (ts - EPOCH) % WINDOW


def is_late(event_time: datetime, global_watermark: datetime | None) -> bool:
    if global_watermark is None:
        return False
    return window_start(event_time) + WINDOW <= global_watermark


class WatermarkTracker:
    """Per-partition event-time high-water marks."""

    def __init__(self, allowed_lateness: timedelta) -> None:
        self.lateness = allowed_lateness
        self.max_event_time: dict[int, datetime] = {}

    def restore(self, partition: int, watermark: datetime | None) -> None:
        if watermark is not None:
            self.max_event_time[partition] = watermark + self.lateness

    def forget(self, partition: int) -> None:
        self.max_event_time.pop(partition, None)

    def observe(self, partition: int, event_time: datetime) -> None:
        cur = self.max_event_time.get(partition)
        if cur is None or event_time > cur:
            self.max_event_time[partition] = event_time

    def watermark(self, partition: int) -> datetime | None:
        m = self.max_event_time.get(partition)
        return None if m is None else m - self.lateness

    @staticmethod
    def global_watermark(watermarks: dict[int, datetime | None], n_partitions: int | None) -> datetime | None:
        """Minimum watermark over *all* partitions of the topic.

        A partition that has not reported yet holds every window open (returns
        None). Otherwise a consumer that happens to read partition 0 first
        would race its watermark ahead and wrongly flag the other partitions'
        events as late. The cost is that an idle partition stalls window
        closing; see the README's limits section.
        """
        if n_partitions is None or len(watermarks) < n_partitions:
            return None
        if any(w is None for w in watermarks.values()):
            return None
        return min(watermarks.values())  # type: ignore[type-var]
