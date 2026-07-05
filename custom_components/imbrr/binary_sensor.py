"""Binary sensor platform for the imbrr integration."""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import ImbrrCoordinator
from .sensor import ImbrrBaseEntity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up imbrr binary sensors from a config entry."""
    coordinator: ImbrrCoordinator = entry.runtime_data
    async_add_entities(
        ImbrrFlowActiveBinarySensor(coordinator, device.serial)
        for device in coordinator.devices
    )


class ImbrrFlowActiveBinarySensor(ImbrrBaseEntity, BinarySensorEntity):
    """On while a flow event is in progress (water is moving)."""

    _attr_translation_key = "flow_active"
    _attr_device_class = BinarySensorDeviceClass.RUNNING

    def __init__(self, coordinator: ImbrrCoordinator, serial: str) -> None:
        super().__init__(coordinator, serial)
        self._attr_unique_id = f"{serial}_flow_active"

    @property
    def is_on(self) -> bool:
        return self.coordinator.is_flow_active(self._serial)
