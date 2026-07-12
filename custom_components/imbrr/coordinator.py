"""Data update coordinator for the imbrr integration.

The coordinator owns the full-fidelity ingestion pipeline:

1. Poll ``latest_depth`` per device. Its ``reading_id`` acts as a watermark:
   when it advances past the last processed reading, the readings endpoint
   is paged (by reading_id once a watermark exists, otherwise by date for
   the initial backfill) so every ~5-second reading is captured even though
   polling is much slower (and even across Home Assistant restarts or
   extended downtime).
2. Every newly seen row's ``gallons`` value is added to a persistent
   lifetime total exactly once (per-row gallons summed over an event have
   been verified to equal the server's ``accumulated_gallons``).
3. The same rows are folded into hourly long-term statistics
   (see ``statistics.py``) so history reflects the true profile.

An optional MQTT overlay lets readings pushed by the device to the local
broker update entity values instantly between polls. MQTT values never feed
the lifetime total or statistics — the cloud CSV remains the single
accounting source, so pushes can never double count.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import (
    FlowReading,
    ImbrrApiClient,
    ImbrrApiError,
    ImbrrAuthError,
    ImbrrConnectionError,
    ImbrrDevice,
    ImbrrError,
    PumpCycle,
)
from .const import (
    ACTIVE_FLOW_GPM,
    CONF_FAST_POLLING_ENABLED,
    CONF_FAST_SCAN_INTERVAL,
    CONF_SCAN_INTERVAL,
    DEFAULT_FAST_POLLING_ENABLED,
    DEFAULT_FAST_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MQTT_FRESHNESS_FACTOR,
    MQTT_STATE_JSON_MAP,
    MQTT_STATE_STATUS_FIELD,
    MQTT_TOPIC_KEY_MAP,
    MODEL_REFIT_DAYS,
    OUTFLOW_MODEL_DAYS,
    PUMP_CYCLE_REFRESH_SECONDS,
    RISING_PSI_PER_S,
    STORAGE_VERSION,
    TYPE_CISTERN,
)
from .outflow import (
    SLOPE_WINDOW_S,
    OutflowModel,
    estimate_outflow,
    fit_outflow_k,
    pressure_slope,
)
from .statistics import async_import_daily_k, async_import_readings

_LOGGER = logging.getLogger(__name__)

STORE_SAVE_DELAY = 10  # seconds


@dataclass
class DeviceLedger:
    """Persisted accounting state for one device."""

    lifetime_gallons: float = 0.0
    last_processed_reading_id: int = 0
    last_processed_ts: datetime | None = None
    backfill_done: bool = False
    pump_cycles_total: int = 0
    last_cycle_ts: datetime | None = None
    outflow_model: OutflowModel | None = None
    # Latest single-day k fit (the tracker) and when the model was last refit.
    daily_k: float | None = None
    last_model_refit: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "lifetime_gallons": self.lifetime_gallons,
            "last_processed_reading_id": self.last_processed_reading_id,
            "last_processed_ts": (
                self.last_processed_ts.isoformat() if self.last_processed_ts else None
            ),
            "backfill_done": self.backfill_done,
            "pump_cycles_total": self.pump_cycles_total,
            "last_cycle_ts": (
                self.last_cycle_ts.isoformat() if self.last_cycle_ts else None
            ),
            "outflow_model": (
                self.outflow_model.as_dict() if self.outflow_model else None
            ),
            "daily_k": self.daily_k,
            "last_model_refit": (
                self.last_model_refit.isoformat() if self.last_model_refit else None
            ),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeviceLedger:
        daily_k = data.get("daily_k")
        return cls(
            lifetime_gallons=float(data.get("lifetime_gallons", 0.0)),
            last_processed_reading_id=int(data.get("last_processed_reading_id", 0)),
            last_processed_ts=_parse_iso(data.get("last_processed_ts")),
            backfill_done=bool(data.get("backfill_done", False)),
            pump_cycles_total=int(data.get("pump_cycles_total", 0)),
            last_cycle_ts=_parse_iso(data.get("last_cycle_ts")),
            daily_k=float(daily_k) if daily_k is not None else None,
            last_model_refit=_parse_iso(data.get("last_model_refit")),
            outflow_model=OutflowModel.from_dict(data.get("outflow_model")),
        )


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


@dataclass
class ImbrrDeviceData:
    """Everything entities need to render one device."""

    device: ImbrrDevice
    available: bool = False
    latest: dict[str, Any] = field(default_factory=dict)
    cistern: dict[str, Any] | None = None
    live: dict[str, float | None] = field(default_factory=dict)
    lifetime_gallons: float = 0.0
    last_pump_cycle: PumpCycle | None = None
    pump_cycles_total: int = 0
    outflow_k: float | None = None
    flow_in_progress: bool = False
    mqtt: dict[str, tuple[float, datetime]] = field(default_factory=dict)
    mqtt_status: tuple[str, datetime] | None = None


class ImbrrCoordinator(DataUpdateCoordinator[dict[str, ImbrrDeviceData]]):
    """Coordinator polling the imbrr cloud for all devices of one account."""

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        api: ImbrrApiClient,
        devices: list[ImbrrDevice],
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=f"{DOMAIN} {entry.title}",
            update_interval=timedelta(seconds=self._option(entry, CONF_SCAN_INTERVAL)),
        )
        self.api = api
        self.devices = devices
        self._store: Store[dict[str, Any]] = Store(
            hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}"
        )
        self.ledgers: dict[str, DeviceLedger] = {}
        self._device_data: dict[str, ImbrrDeviceData] = {
            device.serial: ImbrrDeviceData(device=device) for device in devices
        }
        self._pump_cycle_fetched: dict[str, datetime] = {}
        # Recent (timestamp, psi) samples per device for the outflow proxy's
        # real-time pressure slope. Fed mostly by the MQTT pressure stream.
        self._psi_buffer: dict[str, list[tuple[datetime, float]]] = {}
        # Last known pump-activity state and when it flipped, so the outflow
        # slope can be restricted to samples from the current regime.
        self._activity_state: dict[str, bool] = {}
        self._activity_changed_at: dict[str, datetime] = {}
        # History ingestion writes statistics onto the sensor entities, which
        # do not exist during the first refresh. It is enabled once the
        # platforms are set up (see __init__.async_setup_entry).
        self._ingest_enabled = False

    @staticmethod
    def _option(entry: ConfigEntry, key: str) -> int:
        defaults = {
            CONF_SCAN_INTERVAL: DEFAULT_SCAN_INTERVAL,
            CONF_FAST_SCAN_INTERVAL: DEFAULT_FAST_SCAN_INTERVAL,
        }
        return int(entry.options.get(key, defaults[key]))

    @property
    def _base_interval(self) -> int:
        return self._option(self.config_entry, CONF_SCAN_INTERVAL)

    @property
    def _fast_interval(self) -> int:
        return self._option(self.config_entry, CONF_FAST_SCAN_INTERVAL)

    @property
    def _fast_polling_enabled(self) -> bool:
        return bool(
            self.config_entry.options.get(
                CONF_FAST_POLLING_ENABLED, DEFAULT_FAST_POLLING_ENABLED
            )
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def async_load_ledgers(self) -> None:
        """Load persisted accounting state before the first refresh."""
        data = await self._store.async_load() or {}
        stored = data.get("devices", {})
        for device in self.devices:
            self.ledgers[device.serial] = DeviceLedger.from_dict(
                stored.get(device.serial, {})
            )

    def _schedule_save(self) -> None:
        self._store.async_delay_save(self._ledger_snapshot, STORE_SAVE_DELAY)

    def _ledger_snapshot(self) -> dict[str, Any]:
        return {
            "devices": {
                serial: ledger.as_dict() for serial, ledger in self.ledgers.items()
            }
        }

    async def async_flush_store(self) -> None:
        """Write the ledgers out immediately (used on unload)."""
        await self._store.async_save(self._ledger_snapshot())

    # ------------------------------------------------------------------
    # Update cycle
    # ------------------------------------------------------------------

    async def _async_update_data(self) -> dict[str, ImbrrDeviceData]:
        any_success = False
        any_in_progress = False
        last_error: Exception | None = None

        for device in self.devices:
            data = self._device_data[device.serial]
            try:
                await self._async_update_device(device, data)
                data.available = True
                any_success = True
            except ImbrrAuthError as err:
                raise ConfigEntryAuthFailed(str(err)) from err
            except (ImbrrConnectionError, ImbrrApiError) as err:
                _LOGGER.warning("Update failed for imbrr device %s: %s", device.serial, err)
                data.available = False
                last_error = err
            any_in_progress = any_in_progress or self._data_flow_active(data)

        if not any_success:
            raise UpdateFailed(f"All imbrr devices failed to update: {last_error}")

        # Poll faster while water is flowing so live values track closely.
        target = (
            self._fast_interval
            if any_in_progress and self._fast_polling_enabled
            else self._base_interval
        )
        if self.update_interval != timedelta(seconds=target):
            self.update_interval = timedelta(seconds=target)

        return dict(self._device_data)

    async def _async_update_device(
        self, device: ImbrrDevice, data: ImbrrDeviceData
    ) -> None:
        latest = await self.api.async_get_latest_depth(device.serial)
        data.latest = latest
        data.flow_in_progress = latest.get("flow_event_status") == "in_progress"

        ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
        latest_reading_id = int(latest.get("reading_id") or 0)
        if (
            self._ingest_enabled
            and latest_reading_id > ledger.last_processed_reading_id
        ):
            await self.async_ingest_history(device)
        data.lifetime_gallons = ledger.lifetime_gallons
        data.pump_cycles_total = ledger.pump_cycles_total

        await self._async_update_live_values(device, data)

        if device.device_type == TYPE_CISTERN:
            try:
                data.cistern = await self.api.async_get_cistern_stats(device.serial)
            except ImbrrApiError as err:
                _LOGGER.debug("cistern_stats failed for %s: %s", device.serial, err)

        await self._async_maybe_update_pump_cycles(device, data)

    async def _async_update_live_values(
        self, device: ImbrrDevice, data: ImbrrDeviceData
    ) -> None:
        """Populate instantaneous flow/psi/temp from the active flow event."""
        active = self._data_flow_active(data)
        self._track_activity(device.serial, active)
        if not active:
            data.live["flow"] = 0.0
            return
        if not data.flow_in_progress:
            # MQTT sees the event but the cloud hasn't registered it yet:
            # latest_flow_event would return the *previous* event's rows
            # (stale flow, and a stale psi that would corrupt the slope
            # buffer). The fresh overlay is what made us active anyway.
            return
        try:
            rows = await self.api.async_get_latest_flow_event(device.serial)
        except ImbrrError as err:
            _LOGGER.debug("latest_flow_event failed for %s: %s", device.serial, err)
            return
        visible = [r for r in rows if not r.hide_from_graph] or rows
        if not visible:
            return
        last = max(visible, key=lambda r: r.reading_id)
        data.live.update(
            {
                "flow": last.flow,
                "psi": last.psi,
                "temp": last.temp,
                "depth_to_water": last.depth_to_water,
            }
        )
        if last.psi is not None:
            self._record_psi(device.serial, last.psi)

    async def _async_maybe_update_pump_cycles(
        self, device: ImbrrDevice, data: ImbrrDeviceData
    ) -> None:
        now = dt_util.utcnow()
        fetched = self._pump_cycle_fetched.get(device.serial)
        if fetched and (now - fetched).total_seconds() < PUMP_CYCLE_REFRESH_SECONDS:
            return
        try:
            cycles = await self.api.async_get_pump_cycles(device.serial)
        except ImbrrError as err:
            # Degrade quietly if the pump-cycles endpoint is unavailable.
            _LOGGER.debug("get_pump_cycles failed for %s: %s", device.serial, err)
            return
        self._pump_cycle_fetched[device.serial] = now
        if cycles:
            data.last_pump_cycle = cycles[0]
        self._count_new_pump_cycles(device, data, cycles)

    def _count_new_pump_cycles(
        self, device: ImbrrDevice, data: ImbrrDeviceData, cycles: list[PumpCycle]
    ) -> None:
        """Maintain a persistent, monotonic count of pump cycles.

        The endpoint returns the last ~7 days of cycles each time; we advance a
        timestamp watermark and add only cycles newer than it. The first fetch
        just establishes the watermark (without counting the pre-existing
        history), so the counter grows from install forward — the source for
        the daily/weekly/monthly pump-cycle statistics.
        """
        ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
        cycle_times = [c.time for c in cycles if c.time is not None]
        if cycle_times:
            newest = max(cycle_times)
            if ledger.last_cycle_ts is None:
                ledger.last_cycle_ts = newest
            else:
                new = sum(1 for t in cycle_times if t > ledger.last_cycle_ts)
                if new:
                    ledger.pump_cycles_total += new
                    ledger.last_cycle_ts = newest
                    self._schedule_save()
        data.pump_cycles_total = ledger.pump_cycles_total

    # ------------------------------------------------------------------
    # History ingestion (the "full profile through time" pipeline)
    # ------------------------------------------------------------------

    async def _async_drain_since_id(
        self, serial: str, reading_id: int
    ) -> list[FlowReading]:
        """Page through /readings/since_id until the server reports no more."""
        all_rows: list[FlowReading] = []
        cursor = reading_id
        while True:
            rows, has_more = await self.api.async_get_readings_since_id(serial, cursor)
            if not rows:
                break
            all_rows.extend(rows)
            cursor = max(r.reading_id for r in rows)
            if not has_more:
                break
        return all_rows

    async def _async_drain_since_date(
        self, serial: str, start: date, end: date
    ) -> list[FlowReading]:
        """Page through /readings/since_date, then since_id if truncated.

        since_date has no cursor of its own; once its first (date-bounded)
        page is exhausted, continuing by reading_id is safe because both
        endpoints share the same ascending reading_id ordering.
        """
        rows, has_more = await self.api.async_get_readings_since_date(
            serial, start, end
        )
        all_rows = list(rows)
        while has_more and rows:
            cursor = max(r.reading_id for r in rows)
            rows, has_more = await self.api.async_get_readings_since_id(serial, cursor)
            all_rows.extend(rows)
        return all_rows

    async def async_ingest_history(
        self, device: ImbrrDevice, start: datetime | None = None
    ) -> int:
        """Fetch raw readings since the watermark and account for them.

        Returns the number of newly processed rows. ``start`` overrides the
        fetch window start (used by the initial backfill).
        """
        ledger = self.ledgers.setdefault(device.serial, DeviceLedger())

        if ledger.last_processed_reading_id > 0:
            rows = await self._async_drain_since_id(
                device.serial, ledger.last_processed_reading_id
            )
        else:
            tz = self.api.timezone
            now_local = dt_util.utcnow().astimezone(tz)
            candidates = [
                ts for ts in (start, ledger.last_processed_ts) if ts is not None
            ]
            start_date = (
                min(candidates).astimezone(tz).date()
                if candidates
                else now_local.date()
            )
            rows = await self._async_drain_since_date(
                device.serial, start_date, now_local.date()
            )

        _LOGGER.debug(
            "imbrr %s: fetched %d raw reading(s) from the readings API this ingest",
            device.serial,
            len(rows),
        )

        new_rows = [
            r for r in rows if r.reading_id > ledger.last_processed_reading_id
        ]
        if new_rows:
            ledger.lifetime_gallons += sum(r.gallons for r in new_rows)
            ledger.last_processed_reading_id = max(r.reading_id for r in new_rows)
            ledger.last_processed_ts = max(r.timestamp for r in new_rows)

        # Statistics use the full fetched window (complete hours), not just
        # new rows, because hourly buckets are recomputed idempotently.
        await async_import_readings(self.hass, device, rows, ledger)

        self._device_data[device.serial].lifetime_gallons = ledger.lifetime_gallons
        self._schedule_save()
        return len(new_rows)

    def enable_ingest(self) -> None:
        """Allow poll cycles to ingest history (call once entities exist)."""
        self._ingest_enabled = True

    async def async_initial_ingest(self, backfill_days: int) -> None:
        """Run the first history load after the sensor entities are created.

        On a fresh install this pulls the full backfill window; on restart it
        gap-fills from the persisted watermark. Either way statistics now land
        on entities that exist. Runs as a background task, so failures are
        logged and retried on the next reload rather than blocking setup.
        """
        for device in self.devices:
            ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
            start: datetime | None = None
            if not ledger.backfill_done and backfill_days > 0:
                start = dt_util.utcnow() - timedelta(days=backfill_days)
                _LOGGER.info(
                    "imbrr %s: backfilling %d days of history", device.serial, backfill_days
                )
            try:
                await self.async_ingest_history(device, start=start)
            except ImbrrError as err:
                _LOGGER.warning(
                    "imbrr initial history load failed for %s: %s", device.serial, err
                )
                continue
            ledger.backfill_done = True
        self._schedule_save()
        self.async_update_listeners()

    async def async_reimport_history(self, days: int) -> None:
        """Re-download and re-import statistics for the last ``days`` days.

        Triggered by the ``imbrr.import_history`` service. Idempotent: it
        overwrites the hourly statistics on the entities and never touches the
        gallons ledger or watermark, so it is safe to call repeatedly (e.g. to
        recover history that predates the one-shot backfill).
        """
        tz = self.api.timezone
        now = dt_util.utcnow()
        start = (now - timedelta(days=days)).astimezone(tz).date()
        end = now.astimezone(tz).date()
        for device in self.devices:
            try:
                rows = await self._async_drain_since_date(device.serial, start, end)
            except ImbrrError as err:
                _LOGGER.warning(
                    "imbrr history re-import failed for %s: %s", device.serial, err
                )
                continue
            ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
            await async_import_readings(self.hass, device, rows, ledger)
            _LOGGER.info(
                "imbrr re-imported %d days of statistics for %s (%d readings)",
                days,
                device.serial,
                len(rows),
            )

    # ------------------------------------------------------------------
    # MQTT overlay
    # ------------------------------------------------------------------

    @callback
    def handle_mqtt_message(self, topic: str, payload: str) -> None:
        """Fold a real-time MQTT reading into the device data (display only)."""
        _LOGGER.debug("imbrr MQTT message received on %s: %s", topic, payload)

        serial = self._match_mqtt_device(topic)
        if serial is None:
            _LOGGER.debug(
                "imbrr MQTT: ignoring %s (no serial in topic and %d devices "
                "configured, so cannot attribute the reading)",
                topic,
                len(self.devices),
            )
            return

        readings, status = _parse_mqtt_payload(topic, payload)
        if not readings and status is None:
            _LOGGER.debug(
                "imbrr MQTT: ignoring %s (no recognized readings in payload %r)",
                topic,
                payload,
            )
            return

        data = self._device_data[serial]
        was_active = self._data_flow_active(data)
        now = dt_util.utcnow()
        for reading_key, reading_value in readings.items():
            data.mqtt[reading_key] = (reading_value, now)
        if "psi" in readings:
            self._record_psi(serial, readings["psi"], now)
        if status is not None:
            data.mqtt_status = (status, now)
        _LOGGER.debug(
            "imbrr MQTT: applied %s%s to device %s",
            readings,
            f" status={status}" if status is not None else "",
            serial,
        )
        self.async_set_updated_data(dict(self._device_data))

        # A reading that shows the well is now active while we thought it was
        # idle means an event just started: refresh now instead of waiting out
        # the base interval.
        now_active = self._data_flow_active(data)
        self._track_activity(serial, now_active)
        if now_active and not was_active:
            self.hass.async_create_task(self.async_request_refresh())

    def _match_mqtt_device(self, topic: str) -> str | None:
        """Attribute an MQTT topic to a device by serial (or sole device)."""
        topic_lower = topic.lower()
        matches = [d for d in self.devices if d.serial.lower() in topic_lower]
        if len(matches) == 1:
            return matches[0].serial
        if not matches and len(self.devices) == 1:
            return self.devices[0].serial
        return None

    def _fresh_mqtt(self, data: ImbrrDeviceData, key: str) -> float | None:
        """The MQTT overlay value for ``key`` if it is fresh enough to trust."""
        overlay = data.mqtt.get(key)
        if overlay is None:
            return None
        value, received = overlay
        max_age = self._base_interval * MQTT_FRESHNESS_FACTOR
        if (dt_util.utcnow() - received).total_seconds() <= max_age:
            return value
        return None

    def _data_flow_active(self, data: ImbrrDeviceData) -> bool:
        """Whether the well is pumping, combining several signals.

        The device's MQTT state blob streams every ~5 s, so it is always
        "fresh" — but its flow_event_status field keeps reporting the
        previous event's terminal state while the pump runs, so it must
        never veto the other signals on its own. Activity is therefore:
        a fresh MQTT flow above threshold, an explicit in_progress status,
        or tank pressure rising (only the pump raises tank pressure). A
        coherent set of fresh idle readings (status completed AND flow ~0)
        ends the event immediately instead of waiting out the next poll;
        without MQTT, the polled API status decides.
        """
        now = dt_util.utcnow()
        status: str | None = None
        if data.mqtt_status is not None:
            candidate, received = data.mqtt_status
            max_age = self._base_interval * MQTT_FRESHNESS_FACTOR
            if (now - received).total_seconds() <= max_age:
                status = candidate
        if status == "in_progress":
            return True

        flow = self._fresh_mqtt(data, "flow")
        if flow is not None and flow >= ACTIVE_FLOW_GPM:
            return True

        dpdt = pressure_slope(self._psi_buffer.get(data.device.serial, []), now)
        if dpdt is not None and dpdt >= RISING_PSI_PER_S:
            return True

        if status is not None and flow is not None:
            # Fresh MQTT says idle (flow ~0, no in_progress, pressure not
            # rising): trust it over a possibly stale polled status.
            return False
        return data.flow_in_progress

    def _track_activity(self, serial: str, active: bool) -> None:
        """Record pump on/off transitions (for the outflow slope window)."""
        previous = self._activity_state.get(serial)
        self._activity_state[serial] = active
        if previous is not None and previous != active:
            self._activity_changed_at[serial] = dt_util.utcnow()

    def is_flow_active(self, serial: str) -> bool:
        """Public flow-active state for a device (used by the binary sensor)."""
        data = self.data.get(serial) if self.data else self._device_data.get(serial)
        if data is None:
            return False
        active = self._data_flow_active(data)
        self._track_activity(serial, active)
        return active

    def get_live_value(self, serial: str, key: str) -> float | None:
        """Return the freshest value for a live metric (MQTT beats polling)."""
        data = self.data.get(serial) if self.data else self._device_data.get(serial)
        if data is None:
            return None
        # Flow only has meaning while the well is actively pumping. Once an
        # event completes the device can still publish a residual, non-zero
        # flow_gpm over MQTT (its model lags the shutoff), so force 0 rather
        # than trusting the overlay — this keeps flow_rate consistent with the
        # flow_active binary sensor instead of sticking at the last rate.
        if key == "flow" and not self._data_flow_active(data):
            return 0.0
        overlay = data.mqtt.get(key)
        if overlay is not None:
            value, received = overlay
            max_age = self._base_interval * MQTT_FRESHNESS_FACTOR
            if (dt_util.utcnow() - received).total_seconds() <= max_age:
                return value
        if key == "depth_to_water":
            depth = data.latest.get("depth_to_water")
            return float(depth) if depth is not None else data.live.get(key)
        if key == "event_gallons":
            gallons = data.latest.get("accumulated_gallons")
            return float(gallons) if gallons is not None else data.live.get(key)
        return data.live.get(key)

    # ------------------------------------------------------------------
    # Proxy outflow (flow out of the pressure tank)
    # ------------------------------------------------------------------

    def _record_psi(
        self, serial: str, psi: float, now: datetime | None = None
    ) -> None:
        """Append a pressure sample and trim to the slope window."""
        now = now or dt_util.utcnow()
        buf = self._psi_buffer.setdefault(serial, [])
        buf.append((now, psi))
        cutoff = now - timedelta(seconds=SLOPE_WINDOW_S)
        while buf and buf[0][0] < cutoff:
            buf.pop(0)

    def get_outflow(self, serial: str) -> float | None:
        """Estimated flow OUT of the tank (gpm), or None if not computable.

        Needs a fitted model (built automatically, or via the
        imbrr.build_outflow_model action), a current pressure, and enough
        recent pressure samples to estimate the slope — which in practice
        means the device's MQTT stream is active.

        Works through pump cycles: while the pump runs, the estimate is
        ``flow_in - C(P)*dP/dt`` with the metered inflow, so a concurrent
        draw shows up as the refill's shortfall from a clean rise. Around a
        pump on/off transition the slope window is restricted to the current
        regime; briefly returns None until enough post-transition samples
        exist rather than mixing regimes into a bogus number.
        """
        ledger = self.ledgers.get(serial)
        if ledger is None or ledger.outflow_model is None:
            return None
        data = self.data.get(serial) if self.data else self._device_data.get(serial)
        if data is None:
            return None
        psi = self.get_live_value(serial, "psi")
        if psi is None:
            return None

        now = dt_util.utcnow()
        active = self._data_flow_active(data)
        self._track_activity(serial, active)
        changed_at = self._activity_changed_at.get(serial)
        dpdt = pressure_slope(self._psi_buffer.get(serial, []), now, since=changed_at)
        if dpdt is None:
            return None

        if active:
            flow_in = self.get_live_value(serial, "flow")
            if not flow_in:
                # Pump is running (pressure rising) but the inflow reading
                # hasn't arrived yet — the balance can't be computed, and
                # reporting 0 here is what made draws vanish mid-refill.
                return None
        else:
            flow_in = 0.0
        return estimate_outflow(ledger.outflow_model, psi, flow_in, dpdt)

    async def async_build_outflow_model(self, days: int) -> dict[str, Any]:
        """Fit the tank model from the last ``days`` of readings, per device.

        Returns a per-device summary of what was fit.
        """
        tz = self.api.timezone
        now = dt_util.utcnow()
        start = (now - timedelta(days=days)).astimezone(tz).date()
        end = now.astimezone(tz).date()
        summary: dict[str, Any] = {}
        for device in self.devices:
            try:
                rows = await self._async_drain_since_date(device.serial, start, end)
            except ImbrrError as err:
                _LOGGER.warning(
                    "imbrr outflow model build failed for %s: %s", device.serial, err
                )
                summary[device.serial] = {"error": str(err)}
                continue
            model = fit_outflow_k(rows, fitted_at=now)
            ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
            if model is None:
                _LOGGER.warning(
                    "imbrr %s: not enough clean refill data to fit an outflow "
                    "model (%d readings over %d days)",
                    device.serial,
                    len(rows),
                    days,
                )
                summary[device.serial] = {"fitted": False, "readings": len(rows)}
                continue
            ledger.outflow_model = model
            ledger.last_model_refit = now
            _LOGGER.info(
                "imbrr %s: fitted outflow model k=%.0f from %d clean samples",
                device.serial,
                model.k,
                model.samples,
            )
            summary[device.serial] = {
                "fitted": True,
                "k": round(model.k, 1),
                "samples": model.samples,
                "capacitance_gal_per_psi_at_55": round(model.capacitance(55.0), 2),
            }
        self._schedule_save()
        return summary

    async def async_outflow_maintenance(self) -> None:
        """Daily upkeep of the outflow model (scheduled + at setup).

        Refreshes the daily-k tracker every run, and promotes a fresh 30-day
        model fit at most weekly (keeping the estimate stable while the tracker
        shows drift daily). Runs quietly; failures are logged, not raised.
        """
        for device in self.devices:
            ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
            due = (
                ledger.outflow_model is None
                or ledger.last_model_refit is None
                or (dt_util.utcnow() - ledger.last_model_refit)
                >= timedelta(days=MODEL_REFIT_DAYS)
            )
            if due:
                await self.async_build_outflow_model(OUTFLOW_MODEL_DAYS)
            await self._async_track_daily_k(device)

    async def _async_track_daily_k(self, device: ImbrrDevice) -> None:
        """Fit k over the most recent full local day and record it."""
        tz = self.api.timezone
        day = (dt_util.utcnow().astimezone(tz) - timedelta(days=1)).date()
        try:
            rows = await self.api.async_get_readings_since_date(
                device.serial, day, day
            )
        except ImbrrError as err:
            _LOGGER.debug("daily-k fetch failed for %s: %s", device.serial, err)
            return
        model = fit_outflow_k(rows[0] if isinstance(rows, tuple) else rows)
        if model is None:
            return
        ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
        ledger.daily_k = model.k
        self._device_data[device.serial].outflow_k = model.k
        async_import_daily_k(self.hass, device, {day: model.k})
        self._schedule_save()

    async def async_backfill_daily_k(self, days: int) -> None:
        """Fit and record a per-day k series for the last ``days`` (history)."""
        tz = self.api.timezone
        now = dt_util.utcnow()
        start = (now - timedelta(days=days)).astimezone(tz).date()
        end = now.astimezone(tz).date()
        for device in self.devices:
            try:
                rows = await self._async_drain_since_date(device.serial, start, end)
            except ImbrrError as err:
                _LOGGER.debug("daily-k backfill fetch failed for %s: %s", device.serial, err)
                continue
            by_day: dict[date, list] = {}
            for r in rows:
                by_day.setdefault(r.timestamp.astimezone(tz).date(), []).append(r)
            series: dict[date, float] = {}
            for d, day_rows in by_day.items():
                model = fit_outflow_k(day_rows)
                if model is not None:
                    series[d] = model.k
            if series:
                async_import_daily_k(self.hass, device, series)
                latest = max(series)
                ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
                ledger.daily_k = series[latest]
                self._device_data[device.serial].outflow_k = series[latest]
        self._schedule_save()


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _parse_mqtt_payload(
    topic: str, payload: str
) -> tuple[dict[str, float], str | None]:
    """Parse an MQTT payload into ``({metric: value}, flow_status)``.

    Handles the imbrr device's JSON state blob
    (``{"depth_ft": ..., "flow_gpm": ..., "flow_event_status": ...}``) and,
    as a fallback, a single-metric topic whose last segment names the metric
    with a bare-number or ``{"value": n}`` payload.
    """
    text = (payload or "").strip()
    if not text:
        return {}, None

    if text.startswith("{"):
        try:
            decoded = json.loads(text)
        except ValueError:
            return {}, None
        if not isinstance(decoded, dict):
            return {}, None

        readings: dict[str, float] = {}
        for json_key, internal_key in MQTT_STATE_JSON_MAP.items():
            value = _coerce_float(decoded.get(json_key))
            if value is not None:
                readings[internal_key] = value

        status = decoded.get(MQTT_STATE_STATUS_FIELD)
        status = status if isinstance(status, str) and status else None

        if not readings and status is None:
            # A single-metric topic may still carry {"value": n} / {"state": n}.
            metric = _topic_metric(topic)
            if metric is not None:
                for field_name in ("value", "state"):
                    value = _coerce_float(decoded.get(field_name))
                    if value is not None:
                        readings[metric] = value
                        break
        return readings, status

    metric = _topic_metric(topic)
    if metric is not None:
        value = _coerce_float(text)
        if value is not None:
            return {metric: value}, None
    return {}, None


def _topic_metric(topic: str) -> str | None:
    segments = [s for s in topic.split("/") if s]
    if not segments:
        return None
    return MQTT_TOPIC_KEY_MAP.get(segments[-1].lower())
