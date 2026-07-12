"""Tests for hourly statistics imported onto the sensor entities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from custom_components.imbrr.const import DOMAIN
from custom_components.imbrr.coordinator import DeviceLedger
from custom_components.imbrr.statistics import async_import_readings

from .conftest import TEST_SERIAL, make_device, make_reading

# Two fully elapsed hours, well in the past.
HOUR_1 = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
HOUR_2 = datetime(2026, 7, 1, 11, 0, tzinfo=timezone.utc)

# unique_id suffix -> (entity_id, unit) registered for the device.
ENTITY_SUFFIXES = {
    "depth_to_water": ("sensor.imbrr_depth_to_water", "ft"),
    "flow_rate": ("sensor.imbrr_flow_rate", "gal/min"),
    "pressure": ("sensor.imbrr_pressure", "psi"),
    "water_temperature": ("sensor.imbrr_water_temperature", "°F"),
}


@pytest.fixture
def registered_entities(hass, entity_registry):
    """Register the device's measurement sensors with US units (no conversion)."""
    hass.config.components.add("recorder")
    ids: dict[str, str] = {}
    for suffix, (entity_id, unit) in ENTITY_SUFFIXES.items():
        entry = entity_registry.async_get_or_create(
            "sensor", DOMAIN, f"{TEST_SERIAL}_{suffix}",
            suggested_object_id=entity_id.split(".", 1)[1],
        )
        hass.states.async_set(entry.entity_id, "1.0", {"unit_of_measurement": unit})
        ids[suffix] = entry.entity_id
    return ids


def sample_rows():
    return [
        make_reading(1, HOUR_1 + timedelta(minutes=5), flow=4.0, psi=44.0, temp=57.0, depth=140.0),
        make_reading(2, HOUR_1 + timedelta(minutes=10), flow=6.0, psi=46.0, temp=58.0, depth=130.0),
        make_reading(3, HOUR_1 + timedelta(minutes=15), flow=5.0, psi=45.0, temp=57.5, depth=135.0, hidden=True),
        make_reading(4, HOUR_2 + timedelta(minutes=1), flow=3.0, psi=43.0, temp=56.0, depth=137.0),
    ]


async def test_noop_without_recorder(hass, entity_registry) -> None:
    ledger = DeviceLedger()
    with patch(
        "custom_components.imbrr.statistics.async_import_statistics"
    ) as import_stats:
        await async_import_readings(hass, make_device(), sample_rows(), ledger)
    import_stats.assert_not_called()


async def test_noop_when_entities_missing(registered_entities, hass) -> None:
    """A device with no registered entities imports nothing (no crash)."""
    other = make_device(serial="FFFFFFFFFFFF")
    with patch(
        "custom_components.imbrr.statistics.async_import_statistics"
    ) as import_stats:
        await async_import_readings(hass, other, sample_rows(), DeviceLedger())
    import_stats.assert_not_called()


async def test_imports_onto_entity_statistic_ids(registered_entities, hass) -> None:
    with patch(
        "custom_components.imbrr.statistics.async_import_statistics"
    ) as import_stats:
        await async_import_readings(hass, make_device(), sample_rows(), DeviceLedger())

    by_id = {c.args[1]["statistic_id"]: c for c in import_stats.call_args_list}
    # One import per measurement entity, keyed by the entity id (not imbrr:*).
    assert set(by_id) == set(registered_entities.values())

    flow = by_id["sensor.imbrr_flow_rate"]
    assert flow.args[1]["source"] == "recorder"
    assert flow.args[1]["unit_of_measurement"] == "gal/min"
    stats = flow.args[2]
    # Hidden rows excluded from measurements: hour 1 has rows 1 and 2 only.
    hour1 = next(s for s in stats if s["start"] == HOUR_1)
    assert hour1["mean"] == pytest.approx(5.0)
    assert hour1["min"] == 4.0
    assert hour1["max"] == 6.0


async def test_unit_conversion_to_entity_unit(registered_entities, hass) -> None:
    """When the entity displays metric, imported values are converted."""
    depth_id = registered_entities["depth_to_water"]
    hass.states.async_set(depth_id, "42.7", {"unit_of_measurement": "m"})

    with patch(
        "custom_components.imbrr.statistics.async_import_statistics"
    ) as import_stats:
        await async_import_readings(hass, make_device(), sample_rows(), DeviceLedger())

    depth = next(
        c for c in import_stats.call_args_list if c.args[1]["statistic_id"] == depth_id
    )
    assert depth.args[1]["unit_of_measurement"] == "m"
    hour1 = next(s for s in depth.args[2] if s["start"] == HOUR_1)
    # 140 ft and 130 ft -> meters
    assert hour1["max"] == pytest.approx(140 * 0.3048, rel=1e-3)
    assert hour1["min"] == pytest.approx(130 * 0.3048, rel=1e-3)


async def test_empty_rows_noop(registered_entities, hass) -> None:
    with patch(
        "custom_components.imbrr.statistics.async_import_statistics"
    ) as import_stats:
        await async_import_readings(hass, make_device(), [], DeviceLedger())
    import_stats.assert_not_called()
