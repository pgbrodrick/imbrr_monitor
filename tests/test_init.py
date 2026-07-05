"""Tests for integration setup, backfill seeding, unload, and MQTT wiring."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.util import dt as dt_util

from custom_components.imbrr.const import (
    CONF_BACKFILL_DAYS,
    CONF_MQTT_ENABLED,
    CONF_MQTT_TOPIC,
    DEFAULT_BACKFILL_DAYS,
)

from .conftest import TEST_SERIAL, make_latest_depth, make_mock_api, make_reading

NOW = datetime.now(timezone.utc)


@pytest.fixture
def mock_api():
    api = make_mock_api()
    with patch("custom_components.imbrr.ImbrrApiClient", return_value=api):
        yield api


async def setup_entry(hass, entry) -> bool:
    ok = await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return ok


async def test_setup_and_unload(hass, mock_config_entry, mock_api) -> None:
    assert await setup_entry(hass, mock_config_entry)
    assert mock_config_entry.state is ConfigEntryState.LOADED

    # Entities exist for the well device. The test hass runs metric, so the
    # 136.416 ft depth is auto-converted to meters by the distance class.
    state = hass.states.get("sensor.test_well_site_depth_to_water")
    assert state is not None
    assert float(state.state) == pytest.approx(136.416 * 0.3048, rel=1e-3)
    assert hass.states.get("binary_sensor.test_well_site_flow_active").state == "off"
    assert hass.states.get("sensor.test_well_site_total_water") is not None

    assert await hass.config_entries.async_unload(mock_config_entry.entry_id)
    assert mock_config_entry.state is ConfigEntryState.NOT_LOADED


async def test_first_setup_seeds_backfill_window(
    hass, mock_config_entry, mock_api
) -> None:
    """On first setup the fetch window starts backfill_days in the past."""
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    mock_api.async_download_readings.return_value = [
        make_reading(50, NOW, gallons=2.5)
    ]

    assert await setup_entry(hass, mock_config_entry)

    call = mock_api.async_download_readings.await_args
    start = call.args[1]
    expected = (dt_util.utcnow() - timedelta(days=DEFAULT_BACKFILL_DAYS)).date()
    assert abs((start - expected).days) <= 1

    coordinator = mock_config_entry.runtime_data
    assert coordinator.ledgers[TEST_SERIAL].backfill_done is True
    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(2.5)


async def test_backfill_respects_options(hass, mock_config_entry, mock_api) -> None:
    hass.config_entries.async_update_entry(
        mock_config_entry, options={CONF_BACKFILL_DAYS: 7}
    )
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    mock_api.async_download_readings.return_value = []

    assert await setup_entry(hass, mock_config_entry)

    start = mock_api.async_download_readings.await_args.args[1]
    expected = (dt_util.utcnow() - timedelta(days=7)).date()
    assert abs((start - expected).days) <= 1


async def test_backfill_not_repeated_after_reload(
    hass, mock_config_entry, mock_api
) -> None:
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    mock_api.async_download_readings.return_value = [
        make_reading(50, NOW, gallons=2.5)
    ]
    assert await setup_entry(hass, mock_config_entry)
    assert await hass.config_entries.async_unload(mock_config_entry.entry_id)

    mock_api.async_download_readings.reset_mock()
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    assert await setup_entry(hass, mock_config_entry)

    coordinator = mock_config_entry.runtime_data
    # Watermark already at 50: no re-download, and the total carried over.
    mock_api.async_download_readings.assert_not_awaited()
    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(2.5)


async def test_mqtt_subscribes_when_enabled_and_available(
    hass, mock_config_entry, mock_api
) -> None:
    hass.config_entries.async_update_entry(
        mock_config_entry,
        options={CONF_MQTT_ENABLED: True, CONF_MQTT_TOPIC: "imbrr/#"},
    )
    hass.config.components.add("mqtt")
    unsub = MagicMock()
    with patch(
        "homeassistant.components.mqtt.async_subscribe",
        AsyncMock(return_value=unsub),
    ) as subscribe:
        assert await setup_entry(hass, mock_config_entry)

    subscribe.assert_awaited_once()
    assert subscribe.await_args.args[1] == "imbrr/#"

    # The subscription callback feeds the coordinator overlay.
    callback = subscribe.await_args.args[2]
    msg = MagicMock(topic=f"imbrr/{TEST_SERIAL}/flow", payload="4.5")
    await callback(msg)
    coordinator = mock_config_entry.runtime_data
    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 4.5


async def test_mqtt_enabled_but_not_setup_degrades_gracefully(
    hass, mock_config_entry, mock_api, caplog
) -> None:
    hass.config_entries.async_update_entry(
        mock_config_entry, options={CONF_MQTT_ENABLED: True}
    )
    assert await setup_entry(hass, mock_config_entry)
    assert mock_config_entry.state is ConfigEntryState.LOADED
    assert "MQTT integration is not set up" in caplog.text
