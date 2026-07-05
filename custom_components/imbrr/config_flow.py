"""Config flow for the imbrr integration."""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import (
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import callback
from homeassistant.helpers import aiohttp_client, config_validation as cv
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)
from homeassistant.util import dt as dt_util

from .api import ImbrrApiClient, ImbrrAuthError, ImbrrConnectionError, ImbrrDevice
from .const import (
    CONF_BACKFILL_DAYS,
    CONF_DEVICE_TIMEZONE,
    CONF_DEVICES,
    CONF_FAST_POLLING_ENABLED,
    CONF_FAST_SCAN_INTERVAL,
    CONF_MQTT_ENABLED,
    CONF_MQTT_TOPIC,
    CONF_SCAN_INTERVAL,
    DEFAULT_BACKFILL_DAYS,
    DEFAULT_DEVICE_TIMEZONE,
    DEFAULT_FAST_POLLING_ENABLED,
    DEFAULT_FAST_SCAN_INTERVAL,
    DEFAULT_MQTT_ENABLED,
    DEFAULT_MQTT_TOPIC,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_BACKFILL_DAYS,
    MAX_FAST_SCAN_INTERVAL,
    MAX_SCAN_INTERVAL,
    MIN_FAST_SCAN_INTERVAL,
    MIN_SCAN_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)

CREDENTIALS_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): TextSelector(
            TextSelectorConfig(type=TextSelectorType.EMAIL, autocomplete="username")
        ),
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(
                type=TextSelectorType.PASSWORD, autocomplete="current-password"
            )
        ),
    }
)


class ImbrrConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the imbrr config flow."""

    VERSION = 1

    def __init__(self) -> None:
        self._email: str | None = None
        self._password: str | None = None
        self._discovered: list[ImbrrDevice] = []
        self._client: ImbrrApiClient | None = None

    async def _async_validate_login(
        self, email: str, password: str
    ) -> dict[str, str]:
        """Try to log in; return a config-flow errors dict (empty on success)."""
        client = ImbrrApiClient(
            aiohttp_client.async_get_clientsession(self.hass),
            email,
            password,
            dt_util.get_default_time_zone(),
        )
        try:
            await client.async_login()
        except ImbrrAuthError:
            return {"base": "invalid_auth"}
        except ImbrrConnectionError:
            return {"base": "cannot_connect"}
        except Exception:  # noqa: BLE001
            _LOGGER.exception("Unexpected error validating imbrr credentials")
            return {"base": "unknown"}
        self._client = client
        return {}

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Collect and validate account credentials."""
        errors: dict[str, str] = {}
        if user_input is not None:
            email = user_input[CONF_EMAIL].strip()
            await self.async_set_unique_id(email.lower())
            self._abort_if_unique_id_configured()

            errors = await self._async_validate_login(
                email, user_input[CONF_PASSWORD]
            )
            if not errors:
                self._email = email
                self._password = user_input[CONF_PASSWORD]
                try:
                    self._discovered = await self._client.async_get_devices()
                except Exception:  # noqa: BLE001
                    _LOGGER.exception("Device discovery failed")
                    errors = {"base": "no_devices"}
                if not errors and not self._discovered:
                    errors = {"base": "no_devices"}
            if not errors:
                return await self.async_step_select_devices()

        return self.async_show_form(
            step_id="user", data_schema=CREDENTIALS_SCHEMA, errors=errors
        )

    async def async_step_select_devices(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Let the user pick which discovered devices to add."""
        options = {
            device.serial: f"{device.name} ({device.serial}, {device.device_type})"
            for device in self._discovered
        }
        if user_input is not None:
            selected = user_input[CONF_DEVICES]
            if selected:
                devices = [
                    asdict(device)
                    for device in self._discovered
                    if device.serial in selected
                ]
                return self.async_create_entry(
                    title=self._email or DOMAIN,
                    data={
                        CONF_EMAIL: self._email,
                        CONF_PASSWORD: self._password,
                        CONF_DEVICES: devices,
                    },
                )

        return self.async_show_form(
            step_id="select_devices",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_DEVICES, default=list(options)
                    ): cv.multi_select(options)
                }
            ),
        )

    async def async_step_reauth(
        self, entry_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Start a reauthentication flow after credentials stop working."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Ask for a new password and update the entry."""
        entry = self._get_reauth_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = await self._async_validate_login(
                entry.data[CONF_EMAIL], user_input[CONF_PASSWORD]
            )
            if not errors:
                return self.async_update_reload_and_abort(
                    entry,
                    data={**entry.data, CONF_PASSWORD: user_input[CONF_PASSWORD]},
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_PASSWORD): TextSelector(
                        TextSelectorConfig(
                            type=TextSelectorType.PASSWORD,
                            autocomplete="current-password",
                        )
                    )
                }
            ),
            description_placeholders={CONF_EMAIL: entry.data[CONF_EMAIL]},
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry) -> ImbrrOptionsFlow:
        """Return the options flow handler."""
        return ImbrrOptionsFlow()


class ImbrrOptionsFlow(OptionsFlow):
    """Options for polling cadence, MQTT overlay, timezone, and backfill."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            timezone = user_input.get(CONF_DEVICE_TIMEZONE, "").strip()
            if timezone and dt_util.get_time_zone(timezone) is None:
                errors[CONF_DEVICE_TIMEZONE] = "invalid_timezone"
            else:
                return self.async_create_entry(data=user_input)

        options = self.config_entry.options
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_SCAN_INTERVAL,
                    default=options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL),
                ): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=MIN_SCAN_INTERVAL, max=MAX_SCAN_INTERVAL),
                ),
                vol.Required(
                    CONF_FAST_POLLING_ENABLED,
                    default=options.get(
                        CONF_FAST_POLLING_ENABLED, DEFAULT_FAST_POLLING_ENABLED
                    ),
                ): bool,
                vol.Required(
                    CONF_FAST_SCAN_INTERVAL,
                    default=options.get(
                        CONF_FAST_SCAN_INTERVAL, DEFAULT_FAST_SCAN_INTERVAL
                    ),
                ): vol.All(
                    vol.Coerce(int),
                    vol.Range(min=MIN_FAST_SCAN_INTERVAL, max=MAX_FAST_SCAN_INTERVAL),
                ),
                vol.Required(
                    CONF_MQTT_ENABLED,
                    default=options.get(CONF_MQTT_ENABLED, DEFAULT_MQTT_ENABLED),
                ): bool,
                vol.Required(
                    CONF_MQTT_TOPIC,
                    default=options.get(CONF_MQTT_TOPIC, DEFAULT_MQTT_TOPIC),
                ): str,
                vol.Optional(
                    CONF_DEVICE_TIMEZONE,
                    default=options.get(
                        CONF_DEVICE_TIMEZONE, DEFAULT_DEVICE_TIMEZONE
                    ),
                ): str,
                vol.Required(
                    CONF_BACKFILL_DAYS,
                    default=options.get(CONF_BACKFILL_DAYS, DEFAULT_BACKFILL_DAYS),
                ): vol.All(vol.Coerce(int), vol.Range(min=0, max=MAX_BACKFILL_DAYS)),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
