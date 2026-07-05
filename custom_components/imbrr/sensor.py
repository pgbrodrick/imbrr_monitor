"""Sensor platform for the imbrr integration."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    EntityCategory,
    UnitOfLength,
    UnitOfPressure,
    UnitOfTemperature,
    UnitOfTime,
    UnitOfVolume,
    UnitOfVolumeFlowRate,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import BASE_URL, DOMAIN, MANUFACTURER, MODEL, TYPE_CISTERN, TYPE_WELL
from .coordinator import ImbrrCoordinator, ImbrrDeviceData

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class ImbrrSensorDescription(SensorEntityDescription):
    """Sensor description with an imbrr value extractor."""

    value_fn: Callable[[ImbrrCoordinator, ImbrrDeviceData], Any]
    attrs_fn: Callable[[ImbrrDeviceData], dict[str, Any]] | None = None
    device_types: tuple[str, ...] = (TYPE_WELL, TYPE_CISTERN)
    requires_numeric_id: bool = False


def _latest_timestamp(
    coordinator: ImbrrCoordinator, data: ImbrrDeviceData
) -> datetime | None:
    return coordinator.api.parse_timestamp(str(data.latest.get("timestamp", "")))


def _cistern_value(key: str) -> Callable[[ImbrrCoordinator, ImbrrDeviceData], Any]:
    def getter(coordinator: ImbrrCoordinator, data: ImbrrDeviceData) -> Any:
        return (data.cistern or {}).get(key)

    return getter


def _cistern_timestamp(
    coordinator: ImbrrCoordinator, data: ImbrrDeviceData
) -> datetime | None:
    raw = (data.cistern or {}).get("last_connected")
    return coordinator.api.parse_timestamp(str(raw)) if raw else None


def _pump_cycle_value(attr: str) -> Callable[[ImbrrCoordinator, ImbrrDeviceData], Any]:
    def getter(coordinator: ImbrrCoordinator, data: ImbrrDeviceData) -> Any:
        cycle = data.last_pump_cycle
        return getattr(cycle, attr) if cycle else None

    return getter


def _pump_cycle_attrs(data: ImbrrDeviceData) -> dict[str, Any]:
    cycle = data.last_pump_cycle
    if not cycle:
        return {}
    return {
        "cycle_time": cycle.time.isoformat() if cycle.time else None,
        "trimmed_gpm": cycle.trimmed_gpm,
    }


SENSOR_DESCRIPTIONS: tuple[ImbrrSensorDescription, ...] = (
    ImbrrSensorDescription(
        key="depth_to_water",
        translation_key="depth_to_water",
        native_unit_of_measurement=UnitOfLength.FEET,
        device_class=SensorDeviceClass.DISTANCE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:waves-arrow-down",
        value_fn=lambda c, d: c.get_live_value(d.device.serial, "depth_to_water"),
    ),
    ImbrrSensorDescription(
        key="flow_rate",
        translation_key="flow_rate",
        native_unit_of_measurement=UnitOfVolumeFlowRate.GALLONS_PER_MINUTE,
        device_class=SensorDeviceClass.VOLUME_FLOW_RATE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=2,
        icon="mdi:water-pump",
        value_fn=lambda c, d: c.get_live_value(d.device.serial, "flow"),
    ),
    ImbrrSensorDescription(
        key="water_temperature",
        translation_key="water_temperature",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda c, d: c.get_live_value(d.device.serial, "temp"),
    ),
    ImbrrSensorDescription(
        key="pressure",
        translation_key="pressure",
        native_unit_of_measurement=UnitOfPressure.PSI,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda c, d: c.get_live_value(d.device.serial, "psi"),
    ),
    ImbrrSensorDescription(
        key="current_event_gallons",
        translation_key="current_event_gallons",
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=2,
        icon="mdi:cup-water",
        value_fn=lambda c, d: d.latest.get("accumulated_gallons"),
    ),
    ImbrrSensorDescription(
        key="last_reading",
        translation_key="last_reading",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_latest_timestamp,
    ),
    # Last pump cycle summary (well devices; undocumented endpoint, fails soft)
    ImbrrSensorDescription(
        key="last_pump_cycle_gpm",
        translation_key="last_pump_cycle_gpm",
        native_unit_of_measurement=UnitOfVolumeFlowRate.GALLONS_PER_MINUTE,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:pump",
        device_types=(TYPE_WELL,),
        requires_numeric_id=True,
        value_fn=_pump_cycle_value("gpm"),
        attrs_fn=_pump_cycle_attrs,
    ),
    ImbrrSensorDescription(
        key="last_pump_cycle_gallons",
        translation_key="last_pump_cycle_gallons",
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:water",
        device_types=(TYPE_WELL,),
        requires_numeric_id=True,
        value_fn=_pump_cycle_value("gallons"),
    ),
    ImbrrSensorDescription(
        key="last_pump_cycle_duration",
        translation_key="last_pump_cycle_duration",
        native_unit_of_measurement=UnitOfTime.SECONDS,
        device_class=SensorDeviceClass.DURATION,
        entity_category=EntityCategory.DIAGNOSTIC,
        device_types=(TYPE_WELL,),
        requires_numeric_id=True,
        value_fn=_pump_cycle_value("duration_seconds"),
    ),
    ImbrrSensorDescription(
        key="last_pump_cycle_start_psi",
        translation_key="last_pump_cycle_start_psi",
        native_unit_of_measurement=UnitOfPressure.PSI,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:gauge-low",
        device_types=(TYPE_WELL,),
        requires_numeric_id=True,
        value_fn=_pump_cycle_value("start_psi"),
    ),
    ImbrrSensorDescription(
        key="last_pump_cycle_stop_psi",
        translation_key="last_pump_cycle_stop_psi",
        native_unit_of_measurement=UnitOfPressure.PSI,
        state_class=SensorStateClass.MEASUREMENT,
        entity_category=EntityCategory.DIAGNOSTIC,
        icon="mdi:gauge-full",
        device_types=(TYPE_WELL,),
        requires_numeric_id=True,
        value_fn=_pump_cycle_value("stop_psi"),
    ),
    # Cistern-only statistics
    ImbrrSensorDescription(
        key="storage_gallons",
        translation_key="storage_gallons",
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:storage-tank",
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_value("storage_gallons"),
    ),
    ImbrrSensorDescription(
        key="storage_percent",
        translation_key="storage_percent",
        native_unit_of_measurement="%",
        state_class=SensorStateClass.MEASUREMENT,
        icon="mdi:storage-tank-outline",
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_value("storage_percent"),
    ),
    ImbrrSensorDescription(
        key="usage_24h",
        translation_key="usage_24h",
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:water-minus",
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_value("24_hour_usage"),
    ),
    ImbrrSensorDescription(
        key="usage_31d",
        translation_key="usage_31d",
        native_unit_of_measurement=UnitOfVolume.GALLONS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:water-minus-outline",
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_value("31_day_usage"),
    ),
    ImbrrSensorDescription(
        key="cistern_temperature",
        translation_key="cistern_temperature",
        native_unit_of_measurement=UnitOfTemperature.FAHRENHEIT,
        device_class=SensorDeviceClass.TEMPERATURE,
        state_class=SensorStateClass.MEASUREMENT,
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_value("last_temp"),
    ),
    ImbrrSensorDescription(
        key="cistern_pressure",
        translation_key="cistern_pressure",
        native_unit_of_measurement=UnitOfPressure.PSI,
        device_class=SensorDeviceClass.PRESSURE,
        state_class=SensorStateClass.MEASUREMENT,
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_value("last_pressure"),
    ),
    ImbrrSensorDescription(
        key="last_connected",
        translation_key="last_connected",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        device_types=(TYPE_CISTERN,),
        value_fn=_cistern_timestamp,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up imbrr sensors from a config entry."""
    coordinator: ImbrrCoordinator = entry.runtime_data
    entities: list[SensorEntity] = []
    for device in coordinator.devices:
        for description in SENSOR_DESCRIPTIONS:
            if device.device_type not in description.device_types:
                continue
            if description.requires_numeric_id and not device.numeric_id:
                continue
            entities.append(ImbrrSensor(coordinator, device.serial, description))
        entities.append(ImbrrLifetimeWaterSensor(coordinator, device.serial))
    async_add_entities(entities)


def build_device_info(data: ImbrrDeviceData) -> DeviceInfo:
    """Device registry info shared by all imbrr entities."""
    device = data.device
    return DeviceInfo(
        identifiers={(DOMAIN, device.serial)},
        name=device.name,
        manufacturer=MANUFACTURER,
        model=f"{MODEL} ({device.device_type})",
        serial_number=device.serial,
        configuration_url=f"{BASE_URL}/dashboard/?id={device.serial}",
    )


class ImbrrBaseEntity(CoordinatorEntity[ImbrrCoordinator]):
    """Common wiring for imbrr entities."""

    _attr_has_entity_name = True

    def __init__(self, coordinator: ImbrrCoordinator, serial: str) -> None:
        super().__init__(coordinator)
        self._serial = serial
        self._attr_device_info = build_device_info(self.device_data)

    @property
    def device_data(self) -> ImbrrDeviceData:
        return self.coordinator.data[self._serial]

    @property
    def available(self) -> bool:
        return super().available and self.device_data.available


class ImbrrSensor(ImbrrBaseEntity, SensorEntity):
    """A sensor driven by an ImbrrSensorDescription."""

    entity_description: ImbrrSensorDescription

    def __init__(
        self,
        coordinator: ImbrrCoordinator,
        serial: str,
        description: ImbrrSensorDescription,
    ) -> None:
        super().__init__(coordinator, serial)
        self.entity_description = description
        self._attr_unique_id = f"{serial}_{description.key}"

    @property
    def native_value(self) -> Any:
        return self.entity_description.value_fn(self.coordinator, self.device_data)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        if self.entity_description.attrs_fn is None:
            return None
        return self.entity_description.attrs_fn(self.device_data)


class ImbrrLifetimeWaterSensor(ImbrrBaseEntity, RestoreEntity, SensorEntity):
    """Persistent, monotonically increasing total of all water pumped.

    Backed by the coordinator's store-persisted ledger; the restored entity
    state is only used to recover if the store was lost.
    """

    _attr_translation_key = "total_water"
    _attr_device_class = SensorDeviceClass.WATER
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_native_unit_of_measurement = UnitOfVolume.GALLONS
    _attr_suggested_display_precision = 1

    def __init__(self, coordinator: ImbrrCoordinator, serial: str) -> None:
        super().__init__(coordinator, serial)
        self._attr_unique_id = f"{serial}_total_water"

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        ledger = self.coordinator.ledgers.get(self._serial)
        if ledger is None or ledger.lifetime_gallons > 0:
            return
        last_state = await self.async_get_last_state()
        if last_state is None:
            return
        try:
            restored = float(last_state.state)
        except (TypeError, ValueError):
            return
        if restored > 0:
            _LOGGER.warning(
                "imbrr ledger for %s was empty; restoring total water %.2f gal "
                "from the previous entity state",
                self._serial,
                restored,
            )
            ledger.lifetime_gallons = restored
            self.device_data.lifetime_gallons = restored

    @property
    def native_value(self) -> float:
        return round(self.device_data.lifetime_gallons, 3)
