"""The imbrr integration."""

from __future__ import annotations

import logging
from dataclasses import asdict

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD, Platform
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import aiohttp_client
from homeassistant.util import dt as dt_util

from .api import ImbrrApiClient, ImbrrDevice
from .const import (
    CONF_BACKFILL_DAYS,
    CONF_DEVICE_TIMEZONE,
    CONF_DEVICES,
    CONF_MQTT_ENABLED,
    CONF_MQTT_TOPIC,
    DEFAULT_BACKFILL_DAYS,
    DEFAULT_MQTT_ENABLED,
    DEFAULT_MQTT_TOPIC,
    DOMAIN,
)
from .coordinator import ImbrrCoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.BINARY_SENSOR, Platform.SENSOR]

SERVICE_IMPORT_HISTORY = "import_history"
ATTR_DAYS = "days"
IMPORT_HISTORY_SCHEMA = vol.Schema(
    {vol.Optional(ATTR_DAYS): vol.All(vol.Coerce(int), vol.Range(min=1, max=365))}
)

type ImbrrConfigEntry = ConfigEntry[ImbrrCoordinator]


def _resolve_timezone(hass: HomeAssistant, entry: ConfigEntry):
    """Timezone used to interpret the cloud's naive timestamps."""
    name = (entry.options.get(CONF_DEVICE_TIMEZONE) or "").strip()
    if name:
        tz = dt_util.get_time_zone(name)
        if tz is not None:
            return tz
        _LOGGER.warning(
            "Unknown timezone %s configured for imbrr; using Home Assistant's", name
        )
    return dt_util.get_default_time_zone()


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Migrate old config entries to the current schema."""
    if entry.version > 1:
        # A newer major schema than this code understands (downgrade).
        return False

    if entry.minor_version < 2:
        # Rewrite stored devices to the canonical shape, dropping fields that
        # no longer exist on ImbrrDevice (e.g. numeric_id). from_dict tolerates
        # them at runtime too; this just keeps .storage clean.
        devices = [
            asdict(ImbrrDevice.from_dict(device))
            for device in entry.data.get(CONF_DEVICES, [])
        ]
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_DEVICES: devices},
            minor_version=2,
        )
        _LOGGER.debug("Migrated imbrr config entry %s to minor version 2", entry.entry_id)

    return True


async def async_setup_entry(hass: HomeAssistant, entry: ImbrrConfigEntry) -> bool:
    """Set up imbrr from a config entry."""
    api = ImbrrApiClient(
        aiohttp_client.async_get_clientsession(hass),
        entry.data[CONF_EMAIL],
        entry.data[CONF_PASSWORD],
        _resolve_timezone(hass, entry),
    )
    devices = [
        ImbrrDevice.from_dict(device) for device in entry.data[CONF_DEVICES]
    ]
    if not devices:
        raise ConfigEntryNotReady("No imbrr devices configured")

    coordinator = ImbrrCoordinator(hass, entry, api, devices)
    await coordinator.async_load_ledgers()

    # The first refresh only polls current values; history ingestion is
    # deferred until the sensor entities exist, because statistics are
    # imported onto those entities.
    await coordinator.async_config_entry_first_refresh()

    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Entities now exist: enable ongoing ingestion and run the initial
    # backfill / gap-fill in the background so it never blocks setup.
    coordinator.enable_ingest()
    backfill_days = int(entry.options.get(CONF_BACKFILL_DAYS, DEFAULT_BACKFILL_DAYS))
    entry.async_create_background_task(
        hass,
        coordinator.async_initial_ingest(backfill_days),
        name=f"{DOMAIN}_initial_ingest_{entry.entry_id}",
    )

    await _async_setup_mqtt(hass, entry, coordinator)

    _async_register_services(hass)

    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


@callback
def _async_register_services(hass: HomeAssistant) -> None:
    """Register the account-wide imbrr services once."""
    if hass.services.has_service(DOMAIN, SERVICE_IMPORT_HISTORY):
        return

    async def _handle_import_history(call: ServiceCall) -> None:
        days_override = call.data.get(ATTR_DAYS)
        entries = [
            entry
            for entry in hass.config_entries.async_entries(DOMAIN)
            if entry.state is ConfigEntryState.LOADED
        ]
        for entry in entries:
            coordinator: ImbrrCoordinator = entry.runtime_data
            days = days_override or int(
                entry.options.get(CONF_BACKFILL_DAYS, DEFAULT_BACKFILL_DAYS)
            )
            await coordinator.async_reimport_history(days)

    hass.services.async_register(
        DOMAIN,
        SERVICE_IMPORT_HISTORY,
        _handle_import_history,
        schema=IMPORT_HISTORY_SCHEMA,
    )


async def _async_setup_mqtt(
    hass: HomeAssistant, entry: ImbrrConfigEntry, coordinator: ImbrrCoordinator
) -> None:
    """Subscribe to the device's local MQTT topics for real-time values."""
    if not entry.options.get(CONF_MQTT_ENABLED, DEFAULT_MQTT_ENABLED):
        return
    if "mqtt" not in hass.config.components:
        _LOGGER.warning(
            "imbrr MQTT real-time updates are enabled but the MQTT integration "
            "is not set up; continuing with cloud polling only"
        )
        return

    from homeassistant.components import mqtt

    topic = entry.options.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC)

    async def _message_received(msg) -> None:
        payload = msg.payload
        if isinstance(payload, bytes):
            payload = payload.decode(errors="ignore")
        coordinator.handle_mqtt_message(msg.topic, str(payload))

    entry.async_on_unload(await mqtt.async_subscribe(hass, topic, _message_received))
    _LOGGER.debug("imbrr subscribed to MQTT topic %s", topic)


async def _async_update_listener(hass: HomeAssistant, entry: ImbrrConfigEntry) -> None:
    """Reload the entry when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ImbrrConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator: ImbrrCoordinator = entry.runtime_data
        await coordinator.async_flush_store()
        # Drop the shared service once the last entry is gone.
        remaining = [
            other
            for other in hass.config_entries.async_entries(DOMAIN)
            if other.entry_id != entry.entry_id
            and other.state is ConfigEntryState.LOADED
        ]
        if not remaining and hass.services.has_service(DOMAIN, SERVICE_IMPORT_HISTORY):
            hass.services.async_remove(DOMAIN, SERVICE_IMPORT_HISTORY)
    return unload_ok
