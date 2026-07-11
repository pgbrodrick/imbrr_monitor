"""Tests for integration setup, backfill seeding, unload, and MQTT wiring."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.imbrr.const import (
    CONF_BACKFILL_DAYS,
    CONF_DEVICES,
    CONF_MQTT_ENABLED,
    CONF_MQTT_TOPIC,
    DEFAULT_BACKFILL_DAYS,
    DOMAIN,
    TYPE_WELL,
)

from .conftest import (
    TEST_EMAIL,
    TEST_PASSWORD,
    TEST_SERIAL,
    make_latest_depth,
    make_mock_api,
    make_reading,
)

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


async def test_setup_tolerates_legacy_numeric_id(hass, mock_api) -> None:
    """Entries persisted before numeric_id was dropped must still load.

    Older versions stored a ``numeric_id`` field on each device; rebuilding
    ImbrrDevice from that data must ignore the unknown key rather than raise
    TypeError and fail setup on upgrade.
    """
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=TEST_EMAIL,
        unique_id=TEST_EMAIL,
        data={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            CONF_DEVICES: [
                {
                    "serial": TEST_SERIAL,
                    "name": "Test Well Site",
                    "numeric_id": "115",
                    "device_type": TYPE_WELL,
                }
            ],
        },
        options={},
    )
    entry.add_to_hass(hass)

    assert await setup_entry(hass, entry)
    assert entry.state is ConfigEntryState.LOADED
    coordinator = entry.runtime_data
    assert [d.serial for d in coordinator.devices] == [TEST_SERIAL]


async def test_migration_strips_legacy_numeric_id(hass, mock_api) -> None:
    """A v1.1 entry is migrated to v1.2, dropping numeric_id from storage."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=TEST_EMAIL,
        unique_id=TEST_EMAIL,
        version=1,
        minor_version=1,
        data={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            CONF_DEVICES: [
                {
                    "serial": TEST_SERIAL,
                    "name": "Test Well Site",
                    "numeric_id": "115",
                    "device_type": TYPE_WELL,
                }
            ],
        },
        options={},
    )
    entry.add_to_hass(hass)

    assert await setup_entry(hass, entry)
    assert entry.minor_version == 2
    assert entry.data[CONF_DEVICES] == [
        {
            "serial": TEST_SERIAL,
            "name": "Test Well Site",
            "device_type": TYPE_WELL,
        }
    ]


async def test_import_history_service(hass, mock_config_entry, mock_api) -> None:
    """The import_history service re-imports the requested window."""
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    mock_api.async_get_readings_since_date.return_value = (
        [make_reading(50, NOW, gallons=2.5)],
        False,
    )
    assert await setup_entry(hass, mock_config_entry)
    assert hass.services.has_service(DOMAIN, "import_history")

    mock_api.async_get_readings_since_date.reset_mock()
    await hass.services.async_call(
        DOMAIN, "import_history", {"days": 14}, blocking=True
    )

    start = mock_api.async_get_readings_since_date.await_args.args[1]
    assert (dt_util.utcnow().date() - start).days == 14
    # Re-import doesn't change the running total.
    coordinator = mock_config_entry.runtime_data
    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(2.5)

    # Service is removed when the last entry unloads.
    assert await hass.config_entries.async_unload(mock_config_entry.entry_id)
    assert not hass.services.has_service(DOMAIN, "import_history")


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
    mock_api.async_get_readings_since_date.return_value = (
        [make_reading(50, NOW, gallons=2.5)],
        False,
    )

    assert await setup_entry(hass, mock_config_entry)

    call = mock_api.async_get_readings_since_date.await_args
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
    mock_api.async_get_readings_since_date.return_value = ([], False)

    assert await setup_entry(hass, mock_config_entry)

    start = mock_api.async_get_readings_since_date.await_args.args[1]
    expected = (dt_util.utcnow() - timedelta(days=7)).date()
    assert abs((start - expected).days) <= 1


async def test_backfill_not_double_counted_after_reload(
    hass, mock_config_entry, mock_api
) -> None:
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    mock_api.async_get_readings_since_date.return_value = (
        [make_reading(50, NOW, gallons=2.5)],
        False,
    )
    assert await setup_entry(hass, mock_config_entry)
    assert await hass.config_entries.async_unload(mock_config_entry.entry_id)

    mock_api.async_get_readings_since_id.return_value = ([], False)
    mock_api.async_get_latest_depth.return_value = make_latest_depth(reading_id=50)
    assert await setup_entry(hass, mock_config_entry)

    coordinator = mock_config_entry.runtime_data
    # On reload, the persisted watermark (reading_id 50) is already >0, so
    # catch-up resumes via since_id instead of since_date; the server
    # naturally returns nothing new, so the total does not grow.
    mock_api.async_get_readings_since_id.assert_awaited()
    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(2.5)
    assert coordinator.ledgers[TEST_SERIAL].backfill_done is True


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

    # The subscription callback feeds the coordinator overlay (pressure is
    # always live; flow is suppressed unless the well is actively pumping).
    callback = subscribe.await_args.args[2]
    msg = MagicMock(topic=f"imbrr/{TEST_SERIAL}/pressure", payload="57.4")
    await callback(msg)
    coordinator = mock_config_entry.runtime_data
    assert coordinator.get_live_value(TEST_SERIAL, "psi") == 57.4


async def test_mqtt_enabled_but_not_setup_degrades_gracefully(
    hass, mock_config_entry, mock_api, caplog
) -> None:
    hass.config_entries.async_update_entry(
        mock_config_entry, options={CONF_MQTT_ENABLED: True}
    )
    assert await setup_entry(hass, mock_config_entry)
    assert mock_config_entry.state is ConfigEntryState.LOADED
    assert "MQTT integration is not set up" in caplog.text
