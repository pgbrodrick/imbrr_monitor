"""Tests for the imbrr config and options flows."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResultType

from custom_components.imbrr.api import ImbrrAuthError, ImbrrConnectionError
from custom_components.imbrr.const import (
    CONF_BACKFILL_DAYS,
    CONF_DEVICE_TIMEZONE,
    CONF_DEVICES,
    CONF_FAST_POLLING_ENABLED,
    CONF_FAST_SCAN_INTERVAL,
    CONF_MQTT_ENABLED,
    CONF_MQTT_TOPIC,
    CONF_SCAN_INTERVAL,
    DOMAIN,
    TYPE_WELL,
)

from .conftest import TEST_EMAIL, TEST_PASSWORD, TEST_SERIAL, make_device


@pytest.fixture(autouse=True)
def mock_setup_entry():
    """Prevent actual integration setup when entries are created."""
    with patch(
        "custom_components.imbrr.async_setup_entry", AsyncMock(return_value=True)
    ):
        yield


@pytest.fixture
def mock_client():
    """Patch the config flow's API client with a happy-path mock."""
    client = MagicMock()
    client.async_login = AsyncMock()
    client.async_get_devices = AsyncMock(return_value=[make_device()])
    with patch(
        "custom_components.imbrr.config_flow.ImbrrApiClient", return_value=client
    ):
        yield client


async def test_full_flow_creates_entry(hass, mock_client) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"email": TEST_EMAIL, "password": TEST_PASSWORD}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "select_devices"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_DEVICES: [TEST_SERIAL]}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == TEST_EMAIL
    entry_data = result["data"]
    assert entry_data["email"] == TEST_EMAIL
    assert entry_data["password"] == TEST_PASSWORD
    assert entry_data[CONF_DEVICES] == [
        {
            "serial": TEST_SERIAL,
            "name": "Test Well Site",
            "numeric_id": "115",
            "device_type": TYPE_WELL,
        }
    ]
    assert result["result"].unique_id == TEST_EMAIL


async def test_invalid_auth_shows_error(hass, mock_client) -> None:
    mock_client.async_login.side_effect = ImbrrAuthError("nope")
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"email": TEST_EMAIL, "password": "wrong"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}


async def test_cannot_connect_shows_error(hass, mock_client) -> None:
    mock_client.async_login.side_effect = ImbrrConnectionError("offline")
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"email": TEST_EMAIL, "password": TEST_PASSWORD}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}


async def test_no_devices_shows_error(hass, mock_client) -> None:
    mock_client.async_get_devices.return_value = []
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"email": TEST_EMAIL, "password": TEST_PASSWORD}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "no_devices"}


async def test_duplicate_account_aborts(hass, mock_client, mock_config_entry) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"email": TEST_EMAIL.upper(), "password": TEST_PASSWORD}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_reauth_flow_updates_password(
    hass, mock_client, mock_config_entry
) -> None:
    result = await mock_config_entry.start_reauth_flow(hass)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"password": "new-password"}
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reauth_successful"
    assert mock_config_entry.data["password"] == "new-password"


async def test_reauth_flow_rejects_bad_password(
    hass, mock_client, mock_config_entry
) -> None:
    mock_client.async_login.side_effect = ImbrrAuthError("nope")
    result = await mock_config_entry.start_reauth_flow(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"password": "still-wrong"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "invalid_auth"}


async def test_options_flow_round_trip(hass, mock_config_entry) -> None:
    result = await hass.config_entries.options.async_init(mock_config_entry.entry_id)
    assert result["type"] is FlowResultType.FORM

    user_input = {
        CONF_SCAN_INTERVAL: 120,
        CONF_FAST_POLLING_ENABLED: False,
        CONF_FAST_SCAN_INTERVAL: 30,
        CONF_MQTT_ENABLED: True,
        CONF_MQTT_TOPIC: "imbrr/#",
        CONF_DEVICE_TIMEZONE: "America/New_York",
        CONF_BACKFILL_DAYS: 7,
    }
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], user_input
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert mock_config_entry.options[CONF_SCAN_INTERVAL] == 120
    assert mock_config_entry.options[CONF_DEVICE_TIMEZONE] == "America/New_York"


async def test_options_flow_rejects_bad_timezone(hass, mock_config_entry) -> None:
    result = await hass.config_entries.options.async_init(mock_config_entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            CONF_SCAN_INTERVAL: 60,
            CONF_FAST_POLLING_ENABLED: True,
            CONF_FAST_SCAN_INTERVAL: 15,
            CONF_MQTT_ENABLED: False,
            CONF_MQTT_TOPIC: "imbrr/#",
            CONF_DEVICE_TIMEZONE: "Not/AZone",
            CONF_BACKFILL_DAYS: 30,
        },
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_DEVICE_TIMEZONE: "invalid_timezone"}
