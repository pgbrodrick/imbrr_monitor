"""Proxy outflow model: estimate flow OUT of the pressure tank from pressure.

The imbrr meter measures flow *into* the tank. Household draw (out of the
tank) is invisible to it — especially while the pump is off, when it reads 0.
A pressure tank couples the two: ``flow_in - flow_out = C(P) * dP/dt`` where
``C(P)`` is the tank's water capacitance (gallons per psi). From an ideal
air charge (Boyle's law) ``C(P) = k / P_abs**2`` with a single constant ``k``.

``k`` is fit from historical readings, using only *clean* fast refills (no
simultaneous draw) — a slow pressure rise means water is being drawn while the
pump runs, which inflates the apparent capacitance, so those samples are
excluded. Validated against live data: after that cleaning, ``k`` is flat
across the operating band, confirming the 1/P**2 form.

These are pure functions with no Home Assistant or network dependencies so
they can be unit-tested directly.
"""

from __future__ import annotations

import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .api import FlowReading

# Atmospheric offset to convert gauge psi to absolute (the physics needs
# absolute pressure).
ATM_PSI = 14.7

# Fit filters.
_MIN_DT = 2.0  # seconds between samples
_MAX_DT = 12.0  # seconds; larger gaps are not a continuous rise
_MIN_FLOW = 2.0  # gpm; ignore near-zero flow
_CLEAN_PERCENTILE = 0.6  # keep only the faster (no-draw) refills
_MIN_SAMPLES = 40  # below this the fit is too weak to trust

# Real-time slope window.
SLOPE_WINDOW_S = 30.0
_MIN_SLOPE_SAMPLES = 3
_MIN_SLOPE_SPAN_S = 8.0

# Draw-level thresholds (gpm).
_LEVEL_CUTOFFS = ((0.5, "none"), (3.0, "low"), (7.0, "moderate"))


@dataclass(frozen=True)
class OutflowModel:
    """A fitted tank model for one device."""

    k: float
    samples: int
    fitted_at: datetime | None = None

    def capacitance(self, psi: float) -> float:
        """Tank capacitance C(P) = k / P_abs**2 (gallons per psi)."""
        p_abs = psi + ATM_PSI
        return self.k / (p_abs * p_abs)

    def as_dict(self) -> dict:
        return {
            "k": self.k,
            "samples": self.samples,
            "fitted_at": self.fitted_at.isoformat() if self.fitted_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict | None) -> OutflowModel | None:
        if not data or data.get("k") is None:
            return None
        fitted = data.get("fitted_at")
        return cls(
            k=float(data["k"]),
            samples=int(data.get("samples", 0)),
            fitted_at=datetime.fromisoformat(fitted) if fitted else None,
        )


def fit_outflow_k(
    readings: Sequence[FlowReading], fitted_at: datetime | None = None
) -> OutflowModel | None:
    """Fit ``k`` for ``C(P) = k / P_abs**2`` from historical readings.

    Groups readings by flow event, differentiates pressure between consecutive
    samples, keeps only clean fast refills (the top ``1-_CLEAN_PERCENTILE`` of
    positive dP/dt, where there is little or no simultaneous draw), and takes
    the median of ``flow * P_abs**2 / (dP/dt)``. Returns ``None`` if there is
    not enough clean data.
    """
    by_event: dict[int, list[FlowReading]] = {}
    for r in readings:
        if r.unique_id is None or r.psi is None or r.flow is None:
            continue
        by_event.setdefault(r.unique_id, []).append(r)

    # (pressure-midpoint, dP/dt, flow) for every rising-pressure sample pair.
    pairs: list[tuple[float, float, float]] = []
    for event in by_event.values():
        event.sort(key=lambda r: r.reading_id)
        for a, b in zip(event, event[1:]):
            dt = (b.timestamp - a.timestamp).total_seconds()
            if not (_MIN_DT <= dt <= _MAX_DT):
                continue
            dpdt = (b.psi - a.psi) / dt
            flow = (a.flow + b.flow) / 2.0
            if dpdt <= 0 or flow <= _MIN_FLOW:
                continue
            pairs.append(((a.psi + b.psi) / 2.0, dpdt, flow))

    if len(pairs) < _MIN_SAMPLES:
        return None

    # Clean = fast refills: drop the slow-rise (draw-contaminated) tail.
    threshold = _quantile([p[1] for p in pairs], _CLEAN_PERCENTILE)
    ks = [
        flow * (pm + ATM_PSI) ** 2 / dpdt
        for pm, dpdt, flow in pairs
        if dpdt >= threshold
    ]
    if len(ks) < _MIN_SAMPLES:
        return None

    return OutflowModel(k=statistics.median(ks), samples=len(ks), fitted_at=fitted_at)


def pressure_slope(
    samples: Sequence[tuple[datetime, float]], now: datetime
) -> float | None:
    """Least-squares dP/dt (psi/s) over the last ``SLOPE_WINDOW_S`` seconds.

    Returns ``None`` if there are too few samples or too short a span to trust.
    """
    window = [
        (t, p) for t, p in samples if (now - t).total_seconds() <= SLOPE_WINDOW_S
    ]
    if len(window) < _MIN_SLOPE_SAMPLES:
        return None
    t0 = window[0][0]
    xs = [(t - t0).total_seconds() for t, _ in window]
    ys = [p for _, p in window]
    if xs[-1] - xs[0] < _MIN_SLOPE_SPAN_S:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom


def estimate_outflow(
    model: OutflowModel, psi: float, flow_in: float, dpdt: float
) -> float:
    """Estimated flow out of the tank (gpm), clamped to >= 0.

    ``flow_out = flow_in - C(P) * dP/dt``. Pump off (flow_in 0, pressure
    falling): a positive draw. Pump on: net of what is refilling the tank.
    """
    return max(0.0, flow_in - model.capacitance(psi) * dpdt)


def draw_level(gpm: float) -> str:
    """Coarse, trustworthy bucket for the noisy gpm estimate."""
    for cutoff, label in _LEVEL_CUTOFFS:
        if gpm < cutoff:
            return label
    return "high"


def _quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    idx = min(len(ordered) - 1, int(q * len(ordered)))
    return ordered[idx]
