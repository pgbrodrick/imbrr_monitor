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
DEFAULT_MQTT_TOPIC = "imbrr/+/state"
DEFAULT_DEVICE_TIMEZONE = ""  # empty = use Home Assistant's timezone
DEFAULT_BACKFILL_DAYS = 30

MIN_SCAN_INTERVAL = 30
MAX_SCAN_INTERVAL = 600
MIN_FAST_SCAN_INTERVAL = 5
MAX_FAST_SCAN_INTERVAL = 60
MAX_BACKFILL_DAYS = 365

# How often to refresh the pump-cycle summary
PUMP_CYCLE_REFRESH_SECONDS = 15 * 60

# Outflow model: trailing window for a full fit, how often to promote a fresh
# fit into the live model, and how much daily-k history to backfill.
OUTFLOW_MODEL_DAYS = 30
MODEL_REFIT_DAYS = 7
OUTFLOW_DAILY_K_BACKFILL_DAYS = 30

# How long an MQTT-pushed value is preferred over the polled cloud value,
# expressed as a multiple of the base scan interval.
MQTT_FRESHNESS_FACTOR = 2

# Device types
TYPE_WELL = "well"
TYPE_CISTERN = "cistern"

# Persistence
STORAGE_VERSION = 1

# The imbrr device publishes a single JSON state blob to <prefix>/<serial>/state,
# e.g. {"depth_ft":91.56,"temp_f":61.03,"pressure_psi":48.32,"flow_gpm":0.0,
#       "event_gallons":0.0,"flow_event_status":"completed"}.
# Map its JSON fields to the coordinator's internal live-value keys.
MQTT_STATE_JSON_MAP = {
    "depth_ft": "depth_to_water",
    "temp_f": "temp",
    "pressure_psi": "psi",
    "flow_gpm": "flow",
    "event_gallons": "event_gallons",
}
MQTT_STATE_STATUS_FIELD = "flow_event_status"

# Fallback shape: some payloads may instead publish one metric per topic, with
# the metric name as the last topic segment and a bare-number (or {"value": n})
# payload.
MQTT_TOPIC_KEY_MAP = {
    "flow": "flow",
    "flow_rate": "flow",
    "temperature": "temp",
    "temp": "temp",
    "pressure": "psi",
    "psi": "psi",
    "depth_to_water": "depth_to_water",
}
