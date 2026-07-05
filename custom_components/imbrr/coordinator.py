"""Data update coordinator for the imbrr integration.

The coordinator owns the full-fidelity ingestion pipeline:

1. Poll ``latest_depth`` per device. Its ``reading_id`` acts as a watermark:
   when it advances past the last processed reading, the raw-data download
   endpoint is fetched for the intervening days, so every ~5-second reading
   is captured even though polling is much slower (and even across Home
   Assistant restarts or extended downtime).
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

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import (
    ImbrrApiClient,
    ImbrrApiError,
    ImbrrAuthError,
    ImbrrConnectionError,
    ImbrrDevice,
    ImbrrError,
    PumpCycle,
)
from .const import (
    CONF_FAST_POLLING_ENABLED,
    CONF_FAST_SCAN_INTERVAL,
    CONF_SCAN_INTERVAL,
    DEFAULT_FAST_POLLING_ENABLED,
    DEFAULT_FAST_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MQTT_FRESHNESS_FACTOR,
    MQTT_TOPIC_KEY_MAP,
    PUMP_CYCLE_REFRESH_SECONDS,
    STORAGE_VERSION,
    TYPE_CISTERN,
)
from .statistics import async_import_readings

_LOGGER = logging.getLogger(__name__)

STORE_SAVE_DELAY = 10  # seconds


@dataclass
class DeviceLedger:
    """Persisted accounting state for one device."""

    lifetime_gallons: float = 0.0
    last_processed_reading_id: int = 0
    last_processed_ts: datetime | None = None
    stats_gallons_sum: float = 0.0
    stats_last_imported_hour: datetime | None = None
    backfill_done: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "lifetime_gallons": self.lifetime_gallons,
            "last_processed_reading_id": self.last_processed_reading_id,
            "last_processed_ts": (
                self.last_processed_ts.isoformat() if self.last_processed_ts else None
            ),
            "stats_gallons_sum": self.stats_gallons_sum,
            "stats_last_imported_hour": (
                self.stats_last_imported_hour.isoformat()
                if self.stats_last_imported_hour
                else None
            ),
            "backfill_done": self.backfill_done,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeviceLedger:
        return cls(
            lifetime_gallons=float(data.get("lifetime_gallons", 0.0)),
            last_processed_reading_id=int(data.get("last_processed_reading_id", 0)),
            last_processed_ts=_parse_iso(data.get("last_processed_ts")),
            stats_gallons_sum=float(data.get("stats_gallons_sum", 0.0)),
            stats_last_imported_hour=_parse_iso(data.get("stats_last_imported_hour")),
            backfill_done=bool(data.get("backfill_done", False)),
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
    flow_in_progress: bool = False
    mqtt: dict[str, tuple[float, datetime]] = field(default_factory=dict)


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
            any_in_progress = any_in_progress or data.flow_in_progress

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
        if latest_reading_id > ledger.last_processed_reading_id:
            await self.async_ingest_history(device)
        data.lifetime_gallons = ledger.lifetime_gallons

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
        if not data.flow_in_progress:
            data.live["flow"] = 0.0
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

    async def _async_maybe_update_pump_cycles(
        self, device: ImbrrDevice, data: ImbrrDeviceData
    ) -> None:
        if not device.numeric_id:
            return
        now = dt_util.utcnow()
        fetched = self._pump_cycle_fetched.get(device.serial)
        if fetched and (now - fetched).total_seconds() < PUMP_CYCLE_REFRESH_SECONDS:
            return
        try:
            cycles = await self.api.async_get_pump_cycles(
                device.numeric_id, str(self.hass.config.time_zone)
            )
        except ImbrrError as err:
            # Undocumented endpoint: degrade quietly if it changes.
            _LOGGER.debug("get_pump_cycles failed for %s: %s", device.serial, err)
            return
        self._pump_cycle_fetched[device.serial] = now
        if cycles:
            data.last_pump_cycle = cycles[0]

    # ------------------------------------------------------------------
    # History ingestion (the "full profile through time" pipeline)
    # ------------------------------------------------------------------

    async def async_ingest_history(
        self, device: ImbrrDevice, start: datetime | None = None
    ) -> int:
        """Fetch raw readings since the watermark and account for them.

        Returns the number of newly processed rows. ``start`` overrides the
        fetch window start (used by the initial backfill).
        """
        ledger = self.ledgers.setdefault(device.serial, DeviceLedger())
        tz = self.api.timezone
        now_local = dt_util.utcnow().astimezone(tz)

        candidates = [
            ts
            for ts in (start, ledger.last_processed_ts, ledger.stats_last_imported_hour)
            if ts is not None
        ]
        start_date = (
            min(candidates).astimezone(tz).date() if candidates else now_local.date()
        )

        rows = await self.api.async_download_readings(
            device.serial, start_date, now_local.date()
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

    # ------------------------------------------------------------------
    # MQTT overlay
    # ------------------------------------------------------------------

    @callback
    def handle_mqtt_message(self, topic: str, payload: str) -> None:
        """Fold a real-time MQTT reading into the device data (display only)."""
        segments = [s for s in topic.split("/") if s]
        if not segments:
            return
        key = MQTT_TOPIC_KEY_MAP.get(segments[-1].lower())
        if key is None:
            return

        value = _parse_mqtt_payload(payload)
        if value is None:
            return

        topic_lower = topic.lower()
        matches = [
            d for d in self.devices if d.serial.lower() in topic_lower
        ]
        if len(matches) == 1:
            serial = matches[0].serial
        elif not matches and len(self.devices) == 1:
            serial = self.devices[0].serial
        else:
            return

        data = self._device_data[serial]
        data.mqtt[key] = (value, dt_util.utcnow())
        self.async_set_updated_data(dict(self._device_data))

        # A flow push while we think the well is idle means an event just
        # started: refresh now instead of waiting out the base interval.
        if key == "flow" and value > 0 and not data.flow_in_progress:
            self.hass.async_create_task(self.async_request_refresh())

    def get_live_value(self, serial: str, key: str) -> float | None:
        """Return the freshest value for a live metric (MQTT beats polling)."""
        data = self.data.get(serial) if self.data else self._device_data.get(serial)
        if data is None:
            return None
        overlay = data.mqtt.get(key)
        if overlay is not None:
            value, received = overlay
            max_age = self._base_interval * MQTT_FRESHNESS_FACTOR
            if (dt_util.utcnow() - received).total_seconds() <= max_age:
                return value
        if key == "depth_to_water":
            depth = data.latest.get("depth_to_water")
            return float(depth) if depth is not None else data.live.get(key)
        return data.live.get(key)


def _parse_mqtt_payload(payload: str) -> float | None:
    """Parse an MQTT payload that is either a bare number or {"value": n}."""
    text = (payload or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        pass
    import json

    try:
        decoded = json.loads(text)
    except ValueError:
        return None
    if isinstance(decoded, (int, float)):
        return float(decoded)
    if isinstance(decoded, dict):
        for field_name in ("value", "state"):
            if isinstance(decoded.get(field_name), (int, float)):
                return float(decoded[field_name])
    return None
