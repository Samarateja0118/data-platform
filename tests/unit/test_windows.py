from datetime import UTC, datetime, timedelta

from ridestream.windows import WatermarkTracker, is_late, window_start


def t(m, s=0):
    return datetime(2026, 11, 1, 8, m, s, tzinfo=UTC)


def test_window_start_floors_to_minute():
    assert window_start(t(3, 59)) == t(3)
    assert window_start(t(3, 0)) == t(3)


def test_late_only_once_window_end_passes_watermark():
    # event in [08:03, 08:04); window closes when watermark reaches 08:04
    assert not is_late(t(3, 30), t(3, 59))
    assert is_late(t(3, 30), t(4, 0))
    assert not is_late(t(3, 30), None)


def test_partition_watermark_is_max_minus_lateness_and_monotonic():
    w = WatermarkTracker(timedelta(seconds=30))
    w.observe(0, t(5))
    w.observe(0, t(4))  # out-of-order event does not move it back
    assert w.watermark(0) == t(4, 30)


def test_global_watermark_waits_for_every_partition():
    g = WatermarkTracker.global_watermark
    assert g({0: t(5), 1: t(3)}, 3) is None  # partition 2 unseen
    assert g({0: t(5), 1: t(3), 2: None}, 3) is None  # partition 2 has no data
    assert g({0: t(5), 1: t(3), 2: t(9)}, 3) == t(3)
    assert g({0: t(5)}, None) is None


def test_restore_round_trips_through_stored_watermark():
    w = WatermarkTracker(timedelta(seconds=30))
    w.restore(2, t(4, 30))
    assert w.watermark(2) == t(4, 30)
    w.forget(2)
    assert w.watermark(2) is None
