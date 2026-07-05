"""Long-term statistics import for imbrr raw readings.

Raw readings arrive every ~5 seconds during flow events — far denser than
any reasonable poll interval. To preserve the full profile through time,
committed readings are aggregated into hourly external statistics
(``imbrr:<serial>_<metric>``) that Home Assistant stores forever:

* depth_to_water / flow / psi / temp: hourly mean, min, and max.
  Hours are recomputed idempotently — re-importing an hour overwrites it —
  so partially filled hours simply get corrected on the next import.
* gallons: an hourly monotonic ``sum`` series (usable on the Water
  dashboard). Sums are cumulative, so an hour is emitted exactly once and
  only after it has fully elapsed, guarded by the ledger's
  ``stats_last_imported_hour``.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
)
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .const import DOMAIN

if TYPE_CHECKING:
    from .api import FlowReading, ImbrrDevice
    from .coordinator import DeviceLedger

_LOGGER = logging.getLogger(__name__)

try:  # HA 2025.4+ replaced has_mean with mean_type
    from homeassistant.components.recorder.models import StatisticMeanType

    _MEAN_KWARGS: dict[str, Any] = {"mean_type": StatisticMeanType.ARITHMETIC}
    _NO_MEAN_KWARGS: dict[str, Any] = {"mean_type": StatisticMeanType.NONE}
except ImportError:  # pragma: no cover - older cores
    _MEAN_KWARGS = {"has_mean": True}
    _NO_MEAN_KWARGS = {"has_mean": False}

# metric key -> (FlowReading attribute, display suffix, unit)
MEASUREMENT_METRICS: dict[str, tuple[str, str, str]] = {
    "depth_to_water": ("depth_to_water", "Depth to water", "ft"),
    "flow": ("flow", "Flow rate", "gal/min"),
    "psi": ("psi", "Pressure", "psi"),
    "temp": ("temp", "Water temperature", "°F"),
}


def statistic_id(serial: str, key: str) -> str:
    """Return the external statistic id for a device metric."""
    return f"{DOMAIN}:{serial.lower()}_{key}"


def _hour_bucket(when: datetime) -> datetime:
    return dt_util.as_utc(when).replace(minute=0, second=0, microsecond=0)


async def async_import_readings(
    hass: HomeAssistant,
    device: ImbrrDevice,
    rows: list[FlowReading],
    ledger: DeviceLedger,
) -> None:
    """Aggregate raw readings into hourly external statistics.

    ``rows`` should cover complete local days (the raw download endpoint is
    day-granular), so every hour present is complete. Mutates the ledger's
    gallons running sum and import watermark.
    """
    if "recorder" not in hass.config.components:
        _LOGGER.debug("Recorder not loaded; skipping statistics import")
        return
    if not rows:
        return

    _import_measurements(hass, device, rows)
    _import_gallons(hass, device, rows, ledger)


def _import_measurements(
    hass: HomeAssistant, device: ImbrrDevice, rows: list[FlowReading]
) -> None:
    visible = [r for r in rows if not r.hide_from_graph]
    for key, (attr, label, unit) in MEASUREMENT_METRICS.items():
        buckets: dict[datetime, list[float]] = defaultdict(list)
        for row in visible:
            value = getattr(row, attr)
            if value is not None:
                buckets[_hour_bucket(row.timestamp)].append(value)
        if not buckets:
            continue
        stats = [
            {
                "start": hour,
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
            }
            for hour, values in sorted(buckets.items())
        ]
        metadata = {
            "source": DOMAIN,
            "statistic_id": statistic_id(device.serial, key),
            "name": f"{device.name} {label}",
            "unit_of_measurement": unit,
            "has_sum": False,
            **_MEAN_KWARGS,
        }
        async_add_external_statistics(hass, metadata, stats)


def _import_gallons(
    hass: HomeAssistant,
    device: ImbrrDevice,
    rows: list[FlowReading],
    ledger: DeviceLedger,
) -> None:
    # Hidden rows are included: the server's own accumulated_gallons counts
    # them, and per-row gallons summed this way match it exactly.
    buckets: dict[datetime, float] = defaultdict(float)
    for row in rows:
        if row.gallons:
            buckets[_hour_bucket(row.timestamp)] += row.gallons
    if not buckets:
        return

    current_hour = _hour_bucket(dt_util.utcnow())
    last_imported = ledger.stats_last_imported_hour

    stats = []
    running_sum = ledger.stats_gallons_sum
    for hour in sorted(buckets):
        if hour >= current_hour:
            continue  # hour not over yet; emit once it has fully elapsed
        if last_imported is not None and hour <= last_imported:
            continue  # already folded into the cumulative sum
        running_sum += buckets[hour]
        stats.append({"start": hour, "state": buckets[hour], "sum": running_sum})

    if not stats:
        return

    metadata = {
        "source": DOMAIN,
        "statistic_id": statistic_id(device.serial, "gallons"),
        "name": f"{device.name} Water usage",
        "unit_of_measurement": "gal",
        "has_sum": True,
        **_NO_MEAN_KWARGS,
    }
    async_add_external_statistics(hass, metadata, stats)

    ledger.stats_gallons_sum = running_sum
    ledger.stats_last_imported_hour = stats[-1]["start"]


def backfill_start(days: int) -> datetime:
    """Return the aware start datetime for an N-day backfill window."""
    return dt_util.utcnow() - timedelta(days=days)
