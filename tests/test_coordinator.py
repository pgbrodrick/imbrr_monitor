"""Tests for the imbrr coordinator's ingestion and accounting pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed
from homeassistant.util import dt as dt_util

from custom_components.imbrr.api import (
    ImbrrAuthError,
    ImbrrConnectionError,
    PumpCycle,
)
from custom_components.imbrr.outflow import OutflowModel
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
    # These tests exercise ingestion directly; in production it is enabled once
    # the platforms are set up.
    coordinator.enable_ingest()
    return coordinator


async def test_first_ingest_accounts_all_rows(hass, mock_config_entry) -> None:
    api = make_mock_api()
    rows = [
        make_reading(1, NOW - timedelta(minutes=30), gallons=1.0),
        make_reading(2, NOW - timedelta(minutes=29), gallons=2.0, hidden=True),
        make_reading(3, NOW - timedelta(minutes=28), gallons=0.5),
    ]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=3)
    api.async_get_readings_since_date.return_value = (rows, False)

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
    api.async_get_readings_since_date.return_value = (rows, False)

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()
    await coordinator._async_update_data()
    await coordinator._async_update_data()

    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(2.0)
    # Watermark unchanged => the readings endpoint was hit exactly once.
    assert api.async_get_readings_since_date.await_count == 1
    assert api.async_get_readings_since_id.await_count == 0


async def test_incremental_rows_only_counted_once(hass, mock_config_entry) -> None:
    """A second poll fetches by watermark and only counts the new rows."""
    api = make_mock_api()
    first_batch = [make_reading(i, NOW - timedelta(minutes=10 - i), gallons=1.0) for i in (1, 2)]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=2)
    api.async_get_readings_since_date.return_value = (first_batch, False)

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()

    # New readings appear; since_id (watermark=2) returns only the new rows.
    new_rows = [
        make_reading(3, NOW, gallons=0.25),
        make_reading(4, NOW, gallons=0.25),
    ]
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=4)
    api.async_get_readings_since_id.return_value = (new_rows, False)
    await coordinator._async_update_data()

    ledger = coordinator.ledgers[TEST_SERIAL]
    assert ledger.lifetime_gallons == pytest.approx(2.5)
    assert ledger.last_processed_reading_id == 4
    api.async_get_readings_since_id.assert_awaited_once_with(TEST_SERIAL, 2)


async def test_gap_fill_uses_watermark_reading_id(hass, mock_config_entry) -> None:
    """After downtime, catch-up resumes from the last processed reading_id."""
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=10)

    coordinator = await make_coordinator(hass, mock_config_entry, api)
    ledger = coordinator.ledgers[TEST_SERIAL]
    ledger.last_processed_reading_id = 5
    ledger.last_processed_ts = NOW - timedelta(days=3)

    await coordinator._async_update_data()

    api.async_get_readings_since_id.assert_awaited_once_with(TEST_SERIAL, 5)
    api.async_get_readings_since_date.assert_not_awaited()


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
    other = make_device(serial="112233445566", name="Other")
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


async def test_reimport_history_reimports_without_double_count(
    hass, mock_config_entry
) -> None:
    """Re-importing statistics re-downloads but never grows the total."""
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=2)
    api.async_get_readings_since_date.return_value = (
        [
            make_reading(1, NOW, gallons=3.0),
            make_reading(2, NOW, gallons=4.0),
        ],
        False,
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()
    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(7.0)

    api.async_get_readings_since_date.reset_mock()
    await coordinator.async_reimport_history(30)

    # It re-downloaded the window but did not touch the running total.
    api.async_get_readings_since_date.assert_awaited_once()
    start, end = api.async_get_readings_since_date.await_args.args[1:3]
    assert (dt_util.utcnow().date() - start).days == 30
    assert coordinator.ledgers[TEST_SERIAL].lifetime_gallons == pytest.approx(7.0)


async def test_ledger_persists_via_store(hass, mock_config_entry) -> None:
    """Ledgers survive a coordinator rebuild via the persisted store."""
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(reading_id=2)
    api.async_get_readings_since_date.return_value = (
        [
            make_reading(1, NOW, gallons=3.0),
            make_reading(2, NOW, gallons=4.0),
        ],
        False,
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    await coordinator._async_update_data()
    await coordinator.async_flush_store()

    rebuilt = await make_coordinator(hass, mock_config_entry, make_mock_api())
    ledger = rebuilt.ledgers[TEST_SERIAL]
    assert ledger.lifetime_gallons == pytest.approx(7.0)
    assert ledger.last_processed_reading_id == 2


def _cycle(minutes_ago: int) -> PumpCycle:
    return PumpCycle(
        time=NOW - timedelta(minutes=minutes_ago),
        gpm=5.0,
        trimmed_gpm=5.0,
        gallons=8.0,
        duration_seconds=110,
        start_psi=44.0,
        stop_psi=66.0,
    )


async def test_pump_cycle_counter(hass, mock_config_entry) -> None:
    """Pump cycles are counted monotonically from install, no double count."""
    coordinator = await make_coordinator(hass, mock_config_entry)
    device = coordinator.devices[0]
    data = coordinator._device_data[device.serial]
    ledger = coordinator.ledgers[device.serial]

    # First observation only sets the watermark (pre-existing history isn't counted).
    coordinator._count_new_pump_cycles(device, data, [_cycle(60), _cycle(120)])
    assert ledger.pump_cycles_total == 0
    assert data.pump_cycles_total == 0

    # Two cycles newer than the watermark appear -> +2.
    coordinator._count_new_pump_cycles(
        device, data, [_cycle(5), _cycle(30), _cycle(60), _cycle(120)]
    )
    assert ledger.pump_cycles_total == 2
    assert data.pump_cycles_total == 2

    # Re-observing the same window does not double count.
    coordinator._count_new_pump_cycles(
        device, data, [_cycle(5), _cycle(30), _cycle(60), _cycle(120)]
    )
    assert ledger.pump_cycles_total == 2

    # No cycles is a no-op.
    coordinator._count_new_pump_cycles(device, data, [])
    assert ledger.pump_cycles_total == 2


def _refill_readings(uid: int, n: int, start_psi: float = 45.0):
    rows = []
    psi = start_psi
    t = NOW
    for i in range(n):
        rows.append(make_reading(uid * 1000 + i, t, flow=6.0, psi=psi, unique_id=uid))
        psi += 0.25
        t += timedelta(seconds=5)
    return rows


async def test_build_outflow_model(hass, mock_config_entry) -> None:
    api = make_mock_api()
    rows = _refill_readings(1, 30) + _refill_readings(2, 30) + _refill_readings(3, 30)
    api.async_get_readings_since_date.return_value = (rows, False)
    coordinator = await make_coordinator(hass, mock_config_entry, api)

    summary = await coordinator.async_build_outflow_model(30)

    model = coordinator.ledgers[TEST_SERIAL].outflow_model
    assert model is not None and model.k > 0
    assert summary[TEST_SERIAL]["fitted"] is True
    assert summary[TEST_SERIAL]["samples"] >= 40


async def test_build_outflow_model_insufficient_data(hass, mock_config_entry) -> None:
    api = make_mock_api()
    api.async_get_readings_since_date.return_value = (_refill_readings(1, 5), False)
    coordinator = await make_coordinator(hass, mock_config_entry, api)

    summary = await coordinator.async_build_outflow_model(30)

    assert coordinator.ledgers[TEST_SERIAL].outflow_model is None
    assert summary[TEST_SERIAL]["fitted"] is False


async def test_get_outflow(hass, mock_config_entry) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="completed"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    # No model yet -> None.
    assert coordinator.get_outflow(TEST_SERIAL) is None

    coordinator.ledgers[TEST_SERIAL].outflow_model = OutflowModel(k=88000.0, samples=100)
    # Pump off, current pressure 55, falling ~0.3 psi/s (a draw).
    coordinator.handle_mqtt_message(
        f"imbrr/{TEST_SERIAL}/state",
        '{"pressure_psi":55.0,"flow_gpm":0.0,"flow_event_status":"completed"}',
    )
    now = dt_util.utcnow()
    coordinator._psi_buffer[TEST_SERIAL] = [
        (now - timedelta(seconds=s), 55.0 + 0.3 * s) for s in (20, 15, 10, 5, 0)
    ]

    out = coordinator.get_outflow(TEST_SERIAL)
    assert out is not None and out > 0
    # Matches C(55) * 0.3.
    model = coordinator.ledgers[TEST_SERIAL].outflow_model
    assert out == pytest.approx(model.capacitance(55.0) * 0.3, rel=0.1)


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
# The device keeps publishing the pre-shutoff flow as a residual value even
# after the event has completed (its model lags the physical shutoff).
STATE_PAYLOAD_RESIDUAL = (
    '{"depth_ft":94.9,"temp_f":66.6,"pressure_psi":57.4,'
    '"flow_gpm":6.35,"event_gallons":0.000,"flow_event_status":"completed"}'
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


async def test_residual_flow_reads_zero_when_not_active(
    hass, mock_config_entry
) -> None:
    """A residual non-zero flow_gpm on a completed event must not stick.

    Regression: the device publishes the last flow rate over MQTT even after
    the event completes; flow_rate must read 0 (consistent with the
    flow_active binary sensor) rather than the stale value.
    """
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="completed"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

    coordinator.handle_mqtt_message(STATE_TOPIC, STATE_PAYLOAD_RESIDUAL)

    assert coordinator.is_flow_active(TEST_SERIAL) is False
    # Residual flow is suppressed while idle...
    assert coordinator.get_live_value(TEST_SERIAL, "flow") == 0.0
    # ...but the other live metrics still reflect the fresh overlay.
    assert coordinator.get_live_value(TEST_SERIAL, "psi") == 57.4
    assert coordinator.get_live_value(TEST_SERIAL, "depth_to_water") == 94.9


async def test_mqtt_overlay_updates_live_value(hass, mock_config_entry) -> None:
    api = make_mock_api()
    # in_progress so live flow is reported (it is suppressed while idle).
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="in_progress"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())

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
