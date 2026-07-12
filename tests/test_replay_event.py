"""End-to-end replay of a real captured pump cycle through the coordinator.

The fixture is a genuine event's (dt, psi, flow) rows at the device's ~5 s
cadence, replayed as the MQTT state blob would deliver them — with
``flow_event_status`` lying as "completed" throughout, which is what the
device's firmware has been observed to do mid-event. This is the regression
test for the dead flow_rate/flow_active bug and for the pump-on outflow
estimate.
"""

from __future__ import annotations

import json
import pathlib
from datetime import timedelta
from unittest.mock import patch

from homeassistant.util import dt as dt_util

from custom_components.imbrr.outflow import OutflowModel

from .conftest import TEST_SERIAL, make_latest_depth, make_mock_api
from .test_coordinator import make_coordinator

EVENT = json.loads(
    (pathlib.Path(__file__).parent / "fixtures" / "pump_event_replay.json").read_text()
)
# The idle draw simulated before the event: psi falling at 0.017 psi/s from
# 44.6, which for the fitted k is a ~0.44 gpm draw.
IDLE_DPDT = 0.017
IDLE_PSI0 = 44.6
K = 89546.0  # the live-fit tank constant at capture time


async def test_replay_real_pump_event(hass, mock_config_entry, freezer) -> None:
    api = make_mock_api()
    api.async_get_latest_depth.return_value = make_latest_depth(
        reading_id=0, status="completed"
    )
    coordinator = await make_coordinator(hass, mock_config_entry, api)
    coordinator.async_set_updated_data(await coordinator._async_update_data())
    coordinator.ledgers[TEST_SERIAL].outflow_model = OutflowModel(k=K, samples=500)
    model = coordinator.ledgers[TEST_SERIAL].outflow_model

    t0 = dt_util.utcnow()
    topic = f"imbrr/{TEST_SERIAL}/state"

    def feed(dt_s: float, psi: float, flow: float):
        freezer.move_to(t0 + timedelta(seconds=dt_s))
        blob = json.dumps(
            {
                "pressure_psi": psi,
                "flow_gpm": flow,
                "flow_event_status": "completed",  # the status field lies
            }
        )
        with patch.object(coordinator, "async_request_refresh"):
            coordinator.handle_mqtt_message(topic, blob)
        return (
            coordinator.is_flow_active(TEST_SERIAL),
            coordinator.get_live_value(TEST_SERIAL, "flow"),
            coordinator.get_outflow(TEST_SERIAL),
        )

    # --- 60 s of pump-off draw leading up to the event ------------------
    idle = []
    for i in range(12):
        s = i * 5.0
        idle.append(feed(s - 60.0, IDLE_PSI0 - IDLE_DPDT * s, 0.0))
    settled = idle[3:]
    assert all(not active for active, _, _ in settled)
    assert all(flow == 0.0 for _, flow, _ in settled)
    expected_draw = model.capacitance(IDLE_PSI0) * IDLE_DPDT
    assert all(
        out is not None and abs(out - expected_draw) < 0.15 for _, _, out in settled
    ), "pump-off draw must be visible and match C(P)*dP/dt"

    # --- the real event, blob status lying the whole way ----------------
    during = []
    for r in EVENT:
        during.append((r["dt"], *feed(r["dt"], r["psi"], r["flow"])))
    body = [row for row in during if 15 < row[0] < 130]
    assert all(active for _, active, _, _ in body), (
        "event must read active despite the lying status field"
    )
    assert all(flow > 4 for _, _, flow, _ in body), (
        "flow_rate must show the metered inflow"
    )
    # This event had a genuine concurrent draw (the refill's shortfall):
    # the estimate must be present and positive through the body of it.
    outs = [out for _, _, _, out in body]
    assert all(out is not None for out in outs)
    assert sum(outs) / len(outs) > 1.0

    # --- 40 s idle after shutoff -----------------------------------------
    end = EVENT[-1]["dt"]
    post = []
    for i in range(1, 9):
        s = end + i * 5.0
        post.append((s, *feed(s, EVENT[-1]["psi"], 0.0)))
    # The slope signal may lag shutoff by up to its window; after that the
    # device must read idle with flow forced to 0.
    late = [row for row in post if row[0] > end + 30]
    assert all(not active for _, active, _, _ in late)
    assert all(flow == 0.0 for _, _, flow, _ in late)
