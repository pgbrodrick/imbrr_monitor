"""Tests for imbrr sensor and binary sensor entities."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from homeassistant.const import STATE_UNAVAILABLE
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.imbrr.api import ImbrrConnectionError, PumpCycle
from custom_components.imbrr.const import CONF_DEVICES, DOMAIN, TYPE_CISTERN

from .conftest import (
    TEST_EMAIL,
    TEST_PASSWORD,
    make_latest_depth,
    make_mock_api,
)

NOW = datetime.now(timezone.utc)


@pytest.fixture
def mock_api():
    api = make_mock_api()
    with patch("custom_components.imbrr.ImbrrApiClient", return_value=api):
        yield api


async def setup_entry(hass, entry) -> None:
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def test_well_sensor_values(hass, mock_config_entry, mock_api) -> None:
    mock_api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="in_progress", accumulated=3.21
    )
    mock_api.async_get_latest_flow_event.return_value = []
    mock_api.async_get_pump_cycles.return_value = [
        PumpCycle(
            time=NOW,
            gpm=4.7,
            trimmed_gpm=4.8,
            gallons=8.6,
            duration_seconds=110,
            start_psi=44.1,
            stop_psi=66.6,
        )
    ]
    await setup_entry(hass, mock_config_entry)

    assert hass.states.get("binary_sensor.test_well_site_flow_active").state == "on"
    assert float(
        hass.states.get("sensor.test_well_site_current_event_water").state
    ) == pytest.approx(3.21)
    assert float(
        hass.states.get("sensor.test_well_site_last_pump_cycle_rate").state
    ) == pytest.approx(4.7)
    assert (
        hass.states.get("sensor.test_well_site_last_pump_cycle_duration").state
        == "110"
    )
    # Timestamp sensor renders the latest reading time as an ISO string.
    last_reading = hass.states.get("sensor.test_well_site_last_reading")
    assert last_reading.state.startswith("2026-07-03T22:43:10")


async def test_cistern_sensors(hass, mock_api) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=TEST_EMAIL,
        unique_id=TEST_EMAIL,
        data={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            CONF_DEVICES: [
                {
                    "serial": "112233445566",
                    "name": "Test Cistern Site",
                    "device_type": TYPE_CISTERN,
                }
            ],
        },
    )
    entry.add_to_hass(hass)
    mock_api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, serial="112233445566"
    )
    mock_api.async_get_cistern_stats.return_value = {
        "status": "success",
        "id": "112233445566",
        "storage_gallons": 1250.5,
        "storage_percent": 75,
        "24_hour_usage": 45.2,
        "31_day_usage": 1250.8,
        "last_temp": 68.5,
        "last_pressure": 45.2,
        "last_connected": "2026-07-03 14:30:25",
    }
    await setup_entry(hass, entry)

    assert float(
        hass.states.get("sensor.test_cistern_site_storage").state
    ) == pytest.approx(1250.5)
    assert hass.states.get("sensor.test_cistern_site_storage_percentage").state == "75"
    assert float(
        hass.states.get("sensor.test_cistern_site_usage_last_24_hours").state
    ) == pytest.approx(45.2)
    # No pump-cycle entities for cistern devices.
    assert hass.states.get("sensor.test_cistern_site_last_pump_cycle_rate") is None


async def test_entities_unavailable_when_device_fails(
    hass, mock_config_entry, mock_api
) -> None:
    await setup_entry(hass, mock_config_entry)
    assert (
        hass.states.get("sensor.test_well_site_total_water").state
        != STATE_UNAVAILABLE
    )

    mock_api.async_get_latest_depth.side_effect = ImbrrConnectionError("offline")
    coordinator = mock_config_entry.runtime_data
    await coordinator.async_refresh()
    await hass.async_block_till_done()

    assert (
        hass.states.get("sensor.test_well_site_total_water").state
        == STATE_UNAVAILABLE
    )


async def test_total_water_restores_when_ledger_lost(
    hass, mock_config_entry, mock_api
) -> None:
    """If the store is lost, the lifetime total recovers from the entity state."""
    from pytest_homeassistant_custom_component.common import (
        mock_restore_cache_with_extra_data,
    )
    from homeassistant.core import State

    mock_restore_cache_with_extra_data(
        hass,
        [
            (
                State("sensor.test_well_site_total_water", "1234.5"),
                {"native_value": 1234.5, "native_unit_of_measurement": "gal"},
            )
        ],
    )
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=0)
    await setup_entry(hass, mock_config_entry)

    coordinator = mock_config_entry.runtime_data
    assert coordinator.ledgers[
        "AABBCCDDEEFF"
    ].lifetime_gallons == pytest.approx(1234.5)
    # The test hass runs metric, so the water total renders in liters.
    assert float(
        hass.states.get("sensor.test_well_site_total_water").state
    ) == pytest.approx(1234.5 * 3.78541, rel=1e-3)
