"""Constants for the imbrr integration."""

from __future__ import annotations

DOMAIN = "imbrr"
MANUFACTURER = "imbrr"
MODEL = "IMB-WMS1"

BASE_URL = "https://www.imbrr.com"

# Config entry data keys
CONF_DEVICES = "devices"

# Options keys
CONF_SCAN_INTERVAL = "scan_interval"
CONF_FAST_POLLING_ENABLED = "fast_polling_enabled"
CONF_FAST_SCAN_INTERVAL = "fast_scan_interval"
CONF_MQTT_ENABLED = "mqtt_enabled"
CONF_MQTT_TOPIC = "mqtt_topic"
CONF_DEVICE_TIMEZONE = "device_timezone"
CONF_BACKFILL_DAYS = "backfill_days"

DEFAULT_SCAN_INTERVAL = 60  # seconds
DEFAULT_FAST_SCAN_INTERVAL = 15  # seconds, while a flow event is in progress
DEFAULT_FAST_POLLING_ENABLED = True
DEFAULT_MQTT_ENABLED = False
DEFAULT_MQTT_TOPIC = "imbrr/#"
DEFAULT_DEVICE_TIMEZONE = ""  # empty = use Home Assistant's timezone
DEFAULT_BACKFILL_DAYS = 30

MIN_SCAN_INTERVAL = 30
MAX_SCAN_INTERVAL = 600
MIN_FAST_SCAN_INTERVAL = 5
MAX_FAST_SCAN_INTERVAL = 60
MAX_BACKFILL_DAYS = 365

# How often to refresh the pump-cycle summary
PUMP_CYCLE_REFRESH_SECONDS = 15 * 60

# How long an MQTT-pushed value is preferred over the polled cloud value,
# expressed as a multiple of the base scan interval.
MQTT_FRESHNESS_FACTOR = 2

# Device types
TYPE_WELL = "well"
TYPE_CISTERN = "cistern"

# Persistence
STORAGE_VERSION = 1

# External statistics ids: imbrr:<serial_lower>_<key>
STATISTIC_KEYS = ("depth_to_water", "flow", "psi", "temp")
STATISTIC_GALLONS_KEY = "gallons"

# MQTT topic suffixes recognized by the real-time overlay, mapped to
# coordinator live-value keys.
MQTT_TOPIC_KEY_MAP = {
    "flow": "flow",
    "flow_rate": "flow",
    "temperature": "temp",
    "temp": "temp",
    "pressure": "psi",
    "psi": "psi",
    "depth_to_water": "depth_to_water",
}
