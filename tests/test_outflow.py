"""Tests for the proxy outflow model (pure functions, no HA)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from custom_components.imbrr.api import FlowReading
from custom_components.imbrr.outflow import (
    ATM_PSI,
    OutflowModel,
    draw_level,
    estimate_outflow,
    fit_outflow_k,
    pressure_slope,
)

T0 = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)


def _refill_event(
    uid: int, k: float, flow: float, n: int, start_psi: float, dt: float = 5.0
):
    """Synthetic refill where flow == (k/P_abs**2)*dP/dt at the pair midpoint.

    The next pressure is solved by fixed-point iteration so the discrete
    midpoint relation the fitter uses holds exactly. ``k`` here sets the
    pressure trajectory: a larger ``k`` = slower rise (as if water is being
    drawn while the pump runs).
    """
    rows = []
    psi = start_psi
    t = T0 + timedelta(minutes=uid * 10)
    for i in range(n):
        rows.append(
            FlowReading(
                reading_id=uid * 1000 + i,
                timestamp=t,
                unique_id=uid,
                gallons=0.5,
                flow=flow,
                psi=psi,
                temp=57.0,
                depth_to_water=100.0,
            )
        )
        nxt = psi + dt * flow * (psi + ATM_PSI) ** 2 / k
        for _ in range(50):
            pm = (psi + nxt) / 2.0
            nxt = psi + dt * flow * (pm + ATM_PSI) ** 2 / k
        psi = nxt
        t += timedelta(seconds=dt)
    return rows


def test_fit_recovers_k() -> None:
    readings = []
    for uid in range(6):
        readings += _refill_event(uid, k=88000.0, flow=6.0, n=30, start_psi=45.0)
    model = fit_outflow_k(readings)
    assert model is not None
    assert model.k == pytest.approx(88000.0, rel=0.02)
    assert model.samples >= 40


def test_fit_returns_none_without_enough_data() -> None:
    readings = _refill_event(1, k=88000.0, flow=6.0, n=10, start_psi=45.0)
    assert fit_outflow_k(readings) is None


def test_fit_ignores_draw_contaminated_slow_rises() -> None:
    """Slow-rise (simultaneous-draw) samples must not drag the fit up.

    A draw during pumping makes pressure rise slower for the same reported
    inflow, so those pairs show a much larger apparent k. They are slow rises,
    so the fast-refill selection excludes them.
    """
    clean = []
    for uid in range(6):
        clean += _refill_event(uid, k=88000.0, flow=6.0, n=30, start_psi=45.0)
    # Contaminated: half the rise rate (k trajectory 2x) -> apparent k ~2x.
    dirty = []
    for uid in range(6, 10):
        dirty += _refill_event(uid, k=176000.0, flow=6.0, n=30, start_psi=45.0)
    model = fit_outflow_k(clean + dirty)
    assert model is not None
    assert model.k == pytest.approx(88000.0, rel=0.05)


def test_capacitance_falls_with_pressure() -> None:
    model = OutflowModel(k=88000.0, samples=100)
    assert model.capacitance(45) > model.capacitance(65)
    assert model.capacitance(55) == pytest.approx(88000 / (55 + ATM_PSI) ** 2)


def test_estimate_outflow_pump_off_draw() -> None:
    model = OutflowModel(k=88000.0, samples=100)
    # Pump off (flow_in 0), pressure falling 0.3 psi/s at 55 psi -> positive draw.
    out = estimate_outflow(model, psi=55.0, flow_in=0.0, dpdt=-0.3)
    assert out == pytest.approx(model.capacitance(55) * 0.3, rel=1e-6)
    assert out > 0


def test_estimate_outflow_pump_on_net_and_clamp() -> None:
    model = OutflowModel(k=88000.0, samples=100)
    # Pump on filling with no draw: inflow == C*dP/dt -> net outflow clamps to 0.
    c = model.capacitance(55)
    assert estimate_outflow(model, 55.0, flow_in=c * 0.2, dpdt=0.2) == 0.0
    # Pump on but pressure barely rising -> some draw.
    assert estimate_outflow(model, 55.0, flow_in=6.0, dpdt=0.05) > 0


def test_pressure_slope() -> None:
    now = T0 + timedelta(seconds=30)
    samples = [(T0 + timedelta(seconds=s), 50.0 - 0.2 * s) for s in range(0, 31, 5)]
    slope = pressure_slope(samples, now)
    assert slope == pytest.approx(-0.2, rel=1e-6)


def test_pressure_slope_insufficient() -> None:
    now = T0 + timedelta(seconds=30)
    assert pressure_slope([(T0, 50.0)], now) is None
    # Two samples too close in time -> not enough span.
    close = [(T0, 50.0), (T0 + timedelta(seconds=2), 50.4)]
    assert pressure_slope(close, T0 + timedelta(seconds=2)) is None


def test_draw_level_buckets() -> None:
    assert draw_level(0.0) == "none"
    assert draw_level(1.5) == "low"
    assert draw_level(5.0) == "moderate"
    assert draw_level(9.0) == "high"


def test_model_round_trip() -> None:
    model = OutflowModel(k=88000.0, samples=120, fitted_at=T0)
    restored = OutflowModel.from_dict(model.as_dict())
    assert restored == model
    assert OutflowModel.from_dict(None) is None
    assert OutflowModel.from_dict({"k": None}) is None
