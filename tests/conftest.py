"""Shared fixtures for the imbrr integration tests."""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from custom_components.imbrr.api import FlowReading, ImbrrApiClient, ImbrrDevice
from custom_components.imbrr.const import CONF_DEVICES, DOMAIN, TYPE_WELL
from pytest_homeassistant_custom_component.common import MockConfigEntry

pytest_plugins = "pytest_homeassistant_custom_component"

FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures"

TEST_EMAIL = "user@example.com"
TEST_PASSWORD = "test-password"
TEST_SERIAL = "AABBCCDDEEFF"


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    """Enable loading custom integrations in all tests."""
    return


def load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


def make_device(
    serial: str = TEST_SERIAL,
    name: str = "Test Well Site",
    device_type: str = TYPE_WELL,
) -> ImbrrDevice:
    return ImbrrDevice(serial=serial, name=name, device_type=device_type)


def make_reading(
    reading_id: int,
    ts: datetime,
    gallons: float = 0.5,
    flow: float | None = 5.0,
    psi: float | None = 45.0,
    temp: float | None = 57.6,
    depth: float | None = 136.4,
    unique_id: int | None = 1,
    hidden: bool = False,
) -> FlowReading:
    return FlowReading(
        reading_id=reading_id,
        timestamp=ts,
        unique_id=unique_id,
        gallons=gallons,
        flow=flow,
        psi=psi,
        temp=temp,
        depth_to_water=depth,
        hide_from_graph=hidden,
    )


def make_latest_depth(
    reading_id: int = 100,
    status: str = "completed",
    accumulated: float = 8.55,
    serial: str = TEST_SERIAL,
) -> dict:
    return {
        "status": "success",
        "id": serial,
        "depth_to_water": 136.416,
        "timestamp": "2026-07-03 22:43:10",
        "reading_id": reading_id,
        "unique_id": 1783132889,
        "flow_event_status": status,
        "accumulated_gallons": accumulated,
    }


def make_mock_api(devices: list[ImbrrDevice] | None = None) -> MagicMock:
    """A shape-faithful mock of ImbrrApiClient with benign defaults."""
    api = MagicMock(spec=ImbrrApiClient)
    api.timezone = timezone.utc
    api.parse_timestamp.side_effect = (
        lambda text: datetime.strptime(text.strip(), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        if text and text.strip()
        else None
    )
    api.async_get_latest_depth.return_value = make_latest_depth()
    api.async_download_readings.return_value = []
    api.async_get_latest_flow_event.return_value = []
    api.async_get_pump_cycles.return_value = []
    api.async_get_devices.return_value = devices or [make_device()]
    return api


@pytest.fixture
def mock_config_entry(hass) -> MockConfigEntry:
    """A config entry for one well device, added to hass."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title=TEST_EMAIL,
        unique_id=TEST_EMAIL,
        data={
            "email": TEST_EMAIL,
            "password": TEST_PASSWORD,
            CONF_DEVICES: [
                {
                    "serial": TEST_SERIAL,
                    "name": "Test Well Site",
                    "device_type": TYPE_WELL,
                }
            ],
        },
        options={},
    )
    entry.add_to_hass(hass)
    return entry
