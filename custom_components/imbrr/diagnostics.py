"""Diagnostics support for the imbrr integration."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant

from .coordinator import ImbrrCoordinator

TO_REDACT = {CONF_EMAIL, CONF_PASSWORD, "serial", "id", "unique_id"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return redacted diagnostics for a config entry."""
    coordinator: ImbrrCoordinator = entry.runtime_data
    devices: dict[str, Any] = {}
    for index, device in enumerate(coordinator.devices):
        data = coordinator.data.get(device.serial) if coordinator.data else None
        ledger = coordinator.ledgers.get(device.serial)
        devices[f"device_{index}"] = {
            "device_type": device.device_type,
            "available": data.available if data else None,
            "flow_in_progress": data.flow_in_progress if data else None,
            "latest": async_redact_data(data.latest, TO_REDACT) if data else None,
            "ledger": ledger.as_dict() if ledger else None,
        }
    return {
        "entry_data": async_redact_data(dict(entry.data), TO_REDACT | {"devices"}),
        "options": dict(entry.options),
        "devices": devices,
    }
