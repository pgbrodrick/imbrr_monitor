"""Long-term statistics import for imbrr raw readings.

Raw readings arrive every ~5 seconds during flow events — far denser than
any reasonable poll interval, and they also predate the integration being
installed. To preserve the full profile through time *on the entities
themselves*, committed readings are aggregated into hourly statistics that
Home Assistant stores forever, keyed by each sensor's own statistic id
(``sensor.<name>``). The sensor's History card then shows backfilled and
live data as one continuous series.

Values are converted into whatever unit Home Assistant currently uses for
the entity (e.g. feet -> meters on a metric system) so the imported
statistics line up with the ones the recorder compiles from live states.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from homeassistant.components.recorder.statistics import async_import_statistics
from homeassistant.const import (
    UnitOfLength,
    UnitOfPressure,
    UnitOfTemperature,
    UnitOfVolumeFlowRate,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import (
    DistanceConverter,
    PressureConverter,
    TemperatureConverter,
    VolumeFlowRateConverter,
)

from .const import DOMAIN

if TYPE_CHECKING:
    from .api import FlowReading, ImbrrDevice
    from .coordinator import DeviceLedger

_LOGGER = logging.getLogger(__name__)

try:  # HA 2025.4+ replaced has_mean with mean_type
    from homeassistant.components.recorder.models import StatisticMeanType

    _MEAN_KWARGS: dict[str, Any] = {"mean_type": StatisticMeanType.ARITHMETIC}
except ImportError:  # pragma: no cover - older cores
    _MEAN_KWARGS = {"has_mean": True}

# metric key -> (FlowReading attribute, entity unique_id suffix, raw unit,
# unit converter). The entity suffixes match the sensor descriptions in
# sensor.py; the unique_id is f"{serial}_{suffix}".
MEASUREMENT_METRICS: dict[str, tuple[str, str, str, Any]] = {
    "depth_to_water": (
        "depth_to_water",
        "depth_to_water",
        UnitOfLength.FEET,
        DistanceConverter,
    ),
    "flow": (
        "flow",
        "flow_rate",
        UnitOfVolumeFlowRate.GALLONS_PER_MINUTE,
        VolumeFlowRateConverter,
    ),
    "psi": ("psi", "pressure", UnitOfPressure.PSI, PressureConverter),
    "temp": (
        "temp",
        "water_temperature",
        UnitOfTemperature.FAHRENHEIT,
        TemperatureConverter,
    ),
}


def _hour_bucket(when: datetime) -> datetime:
    return dt_util.as_utc(when).replace(minute=0, second=0, microsecond=0)


def _entity_id(hass: HomeAssistant, serial: str, suffix: str) -> str | None:
    """Resolve a metric's entity id from the registry via its unique id."""
    registry = er.async_get(hass)
    return registry.async_get_entity_id("sensor", DOMAIN, f"{serial}_{suffix}")


def _target_unit(hass: HomeAssistant, entity_id: str, raw_unit: str) -> str:
    """The unit HA currently uses for this entity (falls back to the raw unit)."""
    state = hass.states.get(entity_id)
    if state is not None:
        unit = state.attributes.get("unit_of_measurement")
        if unit:
            return unit
    return raw_unit


def _convert(converter: Any, value: float, from_unit: str, to_unit: str) -> float | None:
    if from_unit == to_unit:
        return value
    try:
        return converter.convert(value, from_unit, to_unit)
    except Exception as err:  # noqa: BLE001 - unsupported unit pairing
        _LOGGER.debug("imbrr statistics: cannot convert %s->%s: %s", from_unit, to_unit, err)
        return None


async def async_import_readings(
    hass: HomeAssistant,
    device: ImbrrDevice,
    rows: list[FlowReading],
    ledger: DeviceLedger,
) -> None:
    """Aggregate raw readings into hourly statistics on each sensor entity.

    ``rows`` should cover complete local days (the raw download endpoint is
    day-granular), so every hour present is complete.
    """
    if "recorder" not in hass.config.components:
        _LOGGER.debug("Recorder not loaded; skipping statistics import")
        return
    if not rows:
        return

    visible = [r for r in rows if not r.hide_from_graph]
    for key, (attr, suffix, raw_unit, converter) in MEASUREMENT_METRICS.items():
        entity_id = _entity_id(hass, device.serial, suffix)
        if entity_id is None:
            # Entities are created after the first refresh; the background
            # initial-ingest re-runs once they exist.
            _LOGGER.debug(
                "imbrr statistics: no entity yet for %s %s; skipping", device.serial, key
            )
            continue

        target_unit = _target_unit(hass, entity_id, raw_unit)
        buckets: dict[datetime, list[float]] = defaultdict(list)
        for row in visible:
            value = getattr(row, attr)
            if value is not None:
                buckets[_hour_bucket(row.timestamp)].append(value)
        if not buckets:
            continue

        stats: list[dict[str, Any]] = []
        for hour, values in sorted(buckets.items()):
            mean = _convert(converter, sum(values) / len(values), raw_unit, target_unit)
            low = _convert(converter, min(values), raw_unit, target_unit)
            high = _convert(converter, max(values), raw_unit, target_unit)
            if mean is None or low is None or high is None:
                stats = []
                break
            stats.append({"start": hour, "mean": mean, "min": low, "max": high})
        if not stats:
            continue

        metadata = {
            "has_sum": False,
            "name": None,
            "source": "recorder",
            "statistic_id": entity_id,
            "unit_of_measurement": target_unit,
            **_MEAN_KWARGS,
        }
        async_import_statistics(hass, metadata, stats)
        _LOGGER.debug(
            "imbrr statistics from readings API: imported %d hourly points for %s",
            len(stats),
            entity_id,
        )


def async_import_daily_k(
    hass: HomeAssistant, device: ImbrrDevice, series: dict[date, float]
) -> None:
    """Import a per-day k series onto the Outflow model k sensor's statistics."""
    if "recorder" not in hass.config.components or not series:
        return
    entity_id = _entity_id(hass, device.serial, "outflow_k")
    if entity_id is None:
        return
    stats = [
        {
            "start": datetime(d.year, d.month, d.day, tzinfo=timezone.utc),
            "mean": series[d],
            "min": series[d],
            "max": series[d],
        }
        for d in sorted(series)
    ]
    metadata = {
        "has_sum": False,
        "name": None,
        "source": "recorder",
        "statistic_id": entity_id,
        "unit_of_measurement": None,
        **_MEAN_KWARGS,
    }
    async_import_statistics(hass, metadata, stats)
