"""Tests for the imbrr coordinator's ingestion and accounting pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.imbrr.api import ImbrrAuthError, ImbrrConnectionError
from custom_components.imbrr.const import (
    DEFAULT_FAST_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
)
from custom_components.imbrr.coordinator import ImbrrCoordinator

from .conftest import (
    TEST_SERIAL,
    make_device,
    make_latest_depth,
    make_mock_api,
    make_reading,
)

NOW = datetime.now(timezone.utc)


async def make_coordinator(
    hass, mock_config_entry, api=None, devices=None
) -> ImbrrCoordinator:
    api = api or make_mock_api()
    devices = devices or [make_device()]
    coordinator = ImbrrCoordinator(hass, mock_config_entry, api, devices)
    await coordinator.async_load_ledgers()
    return coordinator


async def test_first_ingest_accounts_all_rows(hass, mock_config_entry) -> None:
    api = make_mock_api()
    rows = [
        make_reading(1, NOW - timedelta(minutes=30), gallons=1.0),
        make_reading(2, NOW - timedelta(minutes=29), gallons=2.0, hidden=True),
        make_reading(3, NOW - timedelta(minutes=28), gallons=0.5),
    ]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=3)
    api.async_download_readings.return_value = rows

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    data = await coordinator._async_update_data()

    ledger = coordinator.ledgers[TEST_SERIAL]
    # Hidden rows count toward the total (matches the server's accounting).
    assert ledger.lifetime_gallons == pytest.approx(3.5)
    assert ledger.last_processed_reading_id == 3
    assert data[TEST_SERIAL].lifetime_gallons == pytest.approx(3.5)
    assert data[TEST_SERIAL].available is True


async def test_no_double_count_on_repeat_polls(hass, mock_config_entry) -> None:
    api = make_mock_api()
    rows = [make_reading(1, NOW, gallons=1.0), make_reading(2, NOW, gallons=1.0)]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=2)
    api.async_download_readings.return_value = rows

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()
    await coordinator._async_update_data()
    await coordinator._async_update_data()

    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(2.0)
    # Watermark unchanged => the download endpoint was hit exactly once.
    assert api.async_download_readings.await_count == 1


async def test_incremental_rows_only_counted_once(hass, mock_config_entry) -> None:
    """A later poll re-fetching overlapping days only counts rows past the watermark."""
    api = make_mock_api()
    first_batch = [make_reading(i, NOW - timedelta(minutes=10 - i), gallons=1.0) for i in (1, 2)]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=2)
    api.async_download_readings.return_value = first_batch

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()

    # New readings appear; the download returns the overlapping full day.
    second_batch = first_batch + [
        make_reading(3, NOW, gallons=0.25),
        make_reading(4, NOW, gallons=0.25),
    ]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=4)
    api.async_download_readings.return_value = second_batch
    await coordinator._async_update_data()

    ledger = coordinator.ledgers[TEST_SERIAL]
    assert ledger.lifetime_gallons == pytest.approx(2.5)
    assert ledger.last_processed_reading_id == 4


async def test_gap_fill_uses_watermark_date(hass, mock_config_entry) -> None:
    """After downtime, the fetch window starts at the last processed timestamp."""
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=10)
    api.async_download_readings.return_value = []

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    ledger = coordinator.ledgers[TEST_SERIAL]
    ledger.last_processed_reading_id = 5
    ledger.last_processed_ts = NOW - timedelta(days=3)

    await coordinator._async_update_data()

    call = api.async_download_readings.await_args
    start, end = call.args[1], call.args[2]
    assert start == (NOW - timedelta(days=3)).date()
    assert end >= NOW.date() - timedelta(days=1)


async def test_live_values_during_flow_event(hass, mock_config_entry) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="in_progress"
    )
    api.async_get_latest_flow_event.return_value = [
        make_reading(1, NOW, flow=4.0, psi=44.0, temp=57.0, hidden=True),
        make_reading(2, NOW, flow=5.5, psi=45.5, temp=57.5),
    ]

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    data = await coordinator._async_update_data()

    device_data = data[TEST_SERIAL]
    assert device_data.flow_in_progress is True
    # Latest visible row wins.
    assert device_data.live["flow"] == 5.5
    assert device_data.live["psi"] == 45.5
    # Fast polling kicks in while water is flowing.
    assert coordinator.update_interval == timedelta(seconds=DEFAULT_FAST_SCAN_INTERVAL)


async def test_flow_idle_zeroes_flow_and_uses_base_interval(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="completed"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    data = await coordinator._async_update_data()

    assert data[TEST_SERIAL].live["flow"] == 0.0
    assert coordinator.update_interval == timedelta(seconds=DEFAULT_SCAN_INTERVAL)


async def test_auth_error_raises_config_entry_auth_failed(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.side_effect = ImbrrAuthError("bad password")
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    with pytest.raises(ConfigEntryAuthFailed):
        await coordinator._async_update_data()


async def test_per_device_failure_isolation(hass, mock_config_entry) -> None:
    """One failing device does not take down the other."""
    well = make_device()
    other = make_device(serial="112233445566", name="Other", numeric_id=None)
    api = make_mock_api()

    def latest_depth(serial):
        if serial == TEST_SERIAL:
            raise ImbrrConnectionError("offline")
        return make_latest_depth(reading_id=0, serial=serial)

    api.async_get_latest_depth.side_effect = latest_depth
    coordinator = await make_coordinator(
        hass, mock_config_entry, api, devices=[well, other]
    )
    data = await coordinator._async_update_data()

    assert data[TEST_SERIAL].available is False
    assert data["112233445566"].available is True


async def test_all_devices_failing_raises_update_failed(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.side_effect = ImbrrConnectionError("offline")
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    with pytest.raises(UpdateFailed):
        await coordinator._async_update_data()


async def test_ledger_persists_via_store(hass, mock_config_entry) -> None:
    """Ledgers survive a coordinator rebuild via the persisted store."""
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=2)
    api.async_download_readings.return_value = [
        make_reading(1, NOW, gallons=3.0),
        make_reading(2, NOW, gallons=4.0),
    ]
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()
    await coordinator.async_flush_store()

    rebuilt = await make_coordinator(hass, mock_config_entry, make_mock_api())
    ledger = rebuilt.ledgers[TEST_SERIAL]
    assert ledger.lifetime_gallons == pytest.approx(7.0)
    assert ledger.last_processed_reading_id == 2


# ----------------------------------------------------------------------
# MQTT overlay
# ----------------------------------------------------------------------

# The device's real JSON state blob, captured live from imbrr/<serial>/state.
STATE_TOPIC = f"imbrr/{TEST_SERIAL}/state"
STATE_PAYLOAD_IDLE = (
    '{"depth_ft":91.56,"temp_f":61.03,"pressure_psi":48.32,'
    '"flow_gpm":0.00,"event_gallons":0.000,"flow_event_status":"completed"}'
)
STATE_PAYLOAD_FLOWING = (
    '{"depth_ft":120.4,"temp_f":57.6,"pressure_psi":45.1,'
    '"flow_gpm":5.20,"event_gallons":3.140,"flow_event_status":"in_progress"}'
)


async def test_mqtt_json_state_blob_overlays_all_metrics(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=0)
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    coordinator.handle_mqtt_message(STATE_TOPIC, STATE_PAYLOAD_IDLE)

    assert coordinator.get_live_value(TEST_SERIAL, "depth_to_water") == 91.56
    assert coordinator.get_live_value(TEST_SERIAL, "temp") == 61.03
    assert coordinator.get_live_value(TEST_SERIAL, "psi") == 48.32
    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 0.0
    assert coordinator.get_live_value(TEST_SERIAL, "event_gallons") == 0.0
    # flow_event_status "completed" => not active
    assert coordinator.is_flow_active(TEST_SERIAL) is False


async def test_mqtt_json_state_sets_flow_active_and_refreshes(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="completed"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())
    assert coordinator.is_flow_active(TEST_SERIAL) is False
    api.async_get_latest_depth.reset_mock()

    coordinator.handle_mqtt_message(STATE_TOPIC, STATE_PAYLOAD_FLOWING)
    await hass.async_block_till_done()

    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 5.2
    assert coordinator.get_live_value(TEST_SERIAL, "event_gallons") == pytest.approx(3.14)
    # MQTT status flips flow-active instantly, and a just-started event refreshes.
    assert coordinator.is_flow_active(TEST_SERIAL) is True
    assert api.async_get_latest_depth.await_count >= 1


async def test_mqtt_overlay_updates_live_value(hass, mock_config_entry) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=0)
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 0.0
    coordinator.handle_mqtt_message(f"imbrr/{TEST_SERIAL}/flow", "6.25")
    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 6.25

    # JSON payloads work too.
    coordinator.handle_mqtt_message(f"imbrr/{TEST_SERIAL}/pressure", '{"value": 48.5}')
    assert coordinator.get_live_value(TEST_SERIAL, "psi") == 48.5


async def test_mqtt_overlay_matches_sole_device_without_serial(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=0)
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    coordinator.handle_mqtt_message("imbrr/temperature", "58.1")
    assert coordinator.get_live_value(TEST_SERIAL, "temp") == 58.1


async def test_mqtt_overlay_ignores_unknown_topics_and_payloads(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=0)
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    coordinator.handle_mqtt_message("imbrr/unknown_metric", "1.0")
    coordinator.handle_mqtt_message(f"imbrr/{TEST_SERIAL}/flow", "not-a-number")
    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 0.0


async def test_mqtt_overlay_goes_stale(hass, mock_config_entry) -> None:
    """Old MQTT values yield to polled cloud values."""
    from homeassistant.util import dt as dt_util

    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=0)
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    coordinator.handle_mqtt_message(f"imbrr/{TEST_SERIAL}/flow", "6.25")
    data = coordinator.data[TEST_SERIAL]
    value, _ = data.mqtt["flow"]
    stale_time = dt_util.utcnow() - timedelta(seconds=DEFAULT_SCAN_INTERVAL * 3)
    data.mqtt["flow"] = (value, stale_time)

    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 0.0


async def test_mqtt_flow_push_triggers_refresh_when_idle(
    hass, mock_config_entry
) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="completed"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())
    api.async_get_latest_depth.reset_mock()

    coordinator.handle_mqtt_message(f"imbrr/{TEST_SERIAL}/flow", "5.0")
    await hass.async_block_till_done()

    assert api.async_get_latest_depth.await_count >= 1
