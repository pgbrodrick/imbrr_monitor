"""Tests for hourly external statistics generation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from custom_components.imbrr.coordinator import DeviceLedger
from custom_components.imbrr.statistics import async_import_readings, statistic_id

from .conftest import make_device, make_reading

# Two fully elapsed hours, well in the past.
HOUR_1 = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
HOUR_2 = datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc)


@pytest.fixture
def recorder_loaded(hass):
    hass.config.components.add("recorder")
    return hass


def sample_rows():
    return [
        make_reading(1, HOUR_1 + timedelta(minutes=5), gallons=1.0, flow=4.0, depth=140.0),
        make_reading(2, HOUR_1 + timedelta(minutes=10), gallons=2.0, flow=6.0, depth=130.0),
        make_reading(3, HOUR_1 + timedelta(minutes=15), gallons=1.0, flow=5.0, depth=135.0, hidden=True),
        make_reading(4, HOUR_2 + timedelta(minutes=1), gallons=0.5, flow=3.0, depth=137.0),
    ]


async def test_noop_without_recorder(hass) -> None:
    ledger = DeviceLedger()
    with patch(
        "custom_components.imbrr.statistics.async_add_external_statistics"
    ) as add_stats:
        await async_import_readings(hass, make_device(), sample_rows(), ledger)
    add_stats.assert_not_called()
    assert ledger.stats_gallons_sum == 0.0


async def test_measurement_statistics_hourly_buckets(recorder_loaded) -> None:
    hass = recorder_loaded
    ledger = DeviceLedger()
    with patch(
        "custom_components.imbrr.statistics.async_add_external_statistics"
    ) as add_stats:
        await async_import_readings(hass, make_device(), sample_rows(), ledger)

    by_id = {call.args[1]["statistic_id"]: call for call in add_stats.call_args_list}
    flow_stats = by_id[statistic_id("AABBCCDDEEFF", "flow")].args[2]

    # Hidden rows are excluded from measurement series: hour 1 has rows 1 and 2.
    hour1 = next(s for s in flow_stats if s["start"] == HOUR_1)
    assert hour1["mean"] == pytest.approx(5.0)
    assert hour1["min"] == 4.0
    assert hour1["max"] == 6.0

    depth_stats = by_id[statistic_id("AABBCCDDEEFF", "depth_to_water")].args[2]
    hour1_depth = next(s for s in depth_stats if s["start"] == HOUR_1)
    assert hour1_depth["min"] == 130.0
    assert hour1_depth["max"] == 140.0


async def test_gallons_sum_monotonic_and_no_refold(recorder_loaded) -> None:
    """Gallon sums accumulate across imports and never re-fold an hour."""
    hass = recorder_loaded
    ledger = DeviceLedger()
    device = make_device()

    with patch(
        "custom_components.imbrr.statistics.async_add_external_statistics"
    ) as add_stats:
        await async_import_readings(hass, device, sample_rows(), ledger)

    gallons_call = next(
        c
        for c in add_stats.call_args_list
        if c.args[1]["statistic_id"] == statistic_id("AABBCCDDEEFF", "gallons")
    )
    stats = gallons_call.args[2]
    # Hidden rows ARE included in gallons (matches the server's accounting).
    assert stats[0]["start"] == HOUR_1
    assert stats[0]["state"] == pytest.approx(4.0)
    assert stats[0]["sum"] == pytest.approx(4.0)
    assert stats[1]["start"] == HOUR_2
    assert stats[1]["state"] == pytest.approx(0.5)
    assert stats[1]["sum"] == pytest.approx(4.5)
    assert ledger.stats_gallons_sum == pytest.approx(4.5)
    assert ledger.stats_last_imported_hour == HOUR_2

    # Re-import the same window (as the day-granular fetch will do): the
    # cumulative sum must not grow and no gallons stats are re-emitted.
    with patch(
        "custom_components.imbrr.statistics.async_add_external_statistics"
    ) as add_stats2:
        await async_import_readings(hass, device, sample_rows(), ledger)
    gallons_calls = [
        c
        for c in add_stats2.call_args_list
        if c.args[1]["statistic_id"] == statistic_id("AABBCCDDEEFF", "gallons")
    ]
    assert not gallons_calls
    assert ledger.stats_gallons_sum == pytest.approx(4.5)


async def test_current_hour_not_summed_yet(recorder_loaded) -> None:
    """Gallons in the still-elapsing hour wait for the hour to finish."""
    from homeassistant.util import dt as dt_util

    hass = recorder_loaded
    ledger = DeviceLedger()
    now = dt_util.utcnow()
    rows = [make_reading(1, now, gallons=9.0)]

    with patch(
        "custom_components.imbrr.statistics.async_add_external_statistics"
    ) as add_stats:
        await async_import_readings(hass, make_device(), rows, ledger)

    gallons_calls = [
        c
        for c in add_stats.call_args_list
        if c.args[1]["statistic_id"] == statistic_id("AABBCCDDEEFF", "gallons")
    ]
    assert not gallons_calls
    assert ledger.stats_gallons_sum == 0.0


async def test_empty_rows_noop(recorder_loaded) -> None:
    with patch(
        "custom_components.imbrr.statistics.async_add_external_statistics"
    ) as add_stats:
        await async_import_readings(recorder_loaded, make_device(), [], DeviceLedger())
    add_stats.assert_not_called()
