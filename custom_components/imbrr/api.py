"""Async client for the imbrr cloud API.

This module is intentionally free of Home Assistant imports so it can be
exercised standalone (and in tests) with nothing but aiohttp.

Authentication is a PHP session cookie obtained by POSTing the login form.
A successful login redirects to /dashboard; an expired session redirects
API requests back to /login, which we detect and recover from by
re-authenticating exactly once per request.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, tzinfo
from typing import Any

import aiohttp

from .const import BASE_URL, TYPE_WELL

_LOGGER = logging.getLogger(__name__)

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=120)

# Longest span requested from the raw-data download endpoint in one call.
MAX_DOWNLOAD_SPAN_DAYS = 31


class ImbrrError(Exception):
    """Base error for the imbrr client."""


class ImbrrAuthError(ImbrrError):
    """Authentication failed or session could not be re-established."""


class ImbrrConnectionError(ImbrrError):
    """Network-level failure talking to the imbrr cloud."""


class ImbrrApiError(ImbrrError):
    """The imbrr cloud returned an unexpected response."""


@dataclass
class ImbrrDevice:
    """A device attached to an imbrr account."""

    serial: str
    name: str
    device_type: str = TYPE_WELL


@dataclass
class FlowReading:
    """One raw sensor reading parsed from a CSV row."""

    reading_id: int
    timestamp: datetime
    unique_id: int | None
    gallons: float
    flow: float | None = None
    psi: float | None = None
    temp: float | None = None
    depth_to_water: float | None = None
    hide_from_graph: bool = False


@dataclass
class PumpCycle:
    """One pump cycle from the pump-cycles summary."""

    time: datetime | None
    gpm: float | None
    trimmed_gpm: float | None
    gallons: float | None
    duration_seconds: int | None
    start_psi: float | None
    stop_psi: float | None
    raw: dict[str, Any] = field(default_factory=dict)


def _to_float(value: Any) -> float | None:
    """Parse a CSV/JSON cell to float, treating blanks and NULLs as None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() == "NULL":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None else None


class ImbrrApiClient:
    """Session-cookie client for www.imbrr.com."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        email: str,
        password: str,
        timezone: tzinfo,
        base_url: str = BASE_URL,
    ) -> None:
        self._session = session
        self._email = email
        self._password = password
        self._tz = timezone
        self._base_url = base_url.rstrip("/")
        self._login_lock = asyncio.Lock()
        self._logged_in = False

    @property
    def timezone(self) -> tzinfo:
        """Timezone used to localize naive timestamps from the cloud."""
        return self._tz

    def parse_timestamp(self, text: str) -> datetime | None:
        """Parse a naive 'YYYY-MM-DD HH:MM:SS' cloud timestamp to aware local time."""
        text = (text or "").strip().strip('"')
        if not text:
            return None
        try:
            naive = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        return naive.replace(tzinfo=self._tz)

    async def async_login(self) -> None:
        """Authenticate and store the PHP session cookie on the shared session."""
        try:
            async with self._session.post(
                f"{self._base_url}/login",
                data={"email": self._email, "password": self._password},
                allow_redirects=True,
                max_redirects=5,
                timeout=REQUEST_TIMEOUT,
            ) as resp:
                final_path = resp.url.path
        except aiohttp.TooManyRedirects as err:
            # Bad credentials bounce between /login?error=... pages.
            self._logged_in = False
            raise ImbrrAuthError("Login failed (redirect loop back to login)") from err
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise ImbrrConnectionError(f"Error connecting to imbrr: {err}") from err

        if "dashboard" not in final_path:
            self._logged_in = False
            raise ImbrrAuthError("Login failed: invalid email or password")
        self._logged_in = True
        _LOGGER.debug("imbrr login succeeded")

    async def _async_ensure_login(self) -> None:
        async with self._login_lock:
            if not self._logged_in:
                await self.async_login()

    async def _async_get(
        self,
        path: str,
        params: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout = REQUEST_TIMEOUT,
        _retry: bool = True,
    ) -> str:
        """GET a path, re-authenticating once if the session has expired."""
        await self._async_ensure_login()
        url = f"{self._base_url}{path}"
        try:
            async with self._session.get(
                url, params=params, allow_redirects=True, timeout=timeout
            ) as resp:
                final_path = resp.url.path
                body = await resp.text()
                status = resp.status
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            raise ImbrrConnectionError(f"Error connecting to imbrr: {err}") from err

        session_expired = (
            "/login" in final_path
            or status == 401
            or "User not authenticated" in body[:500]
        )
        if session_expired:
            if not _retry:
                self._logged_in = False
                raise ImbrrAuthError("Session expired and re-login failed")
            _LOGGER.debug("imbrr session expired; re-authenticating")
            async with self._login_lock:
                self._logged_in = False
                await self.async_login()
            return await self._async_get(path, params, timeout, _retry=False)

        if status >= 400:
            raise ImbrrApiError(f"imbrr returned HTTP {status} for {path}")
        return body

    async def _async_get_json(
        self, path: str, params: dict[str, str] | None = None
    ) -> dict[str, Any]:
        import json

        body = await self._async_get(path, params)
        try:
            return json.loads(body)
        except ValueError as err:
            raise ImbrrApiError(f"Invalid JSON from {path}") from err

    # ------------------------------------------------------------------
    # Documented API v1 endpoints
    # ------------------------------------------------------------------

    async def async_get_latest_depth(self, serial: str) -> dict[str, Any]:
        """Return the latest reading summary for a device."""
        data = await self._async_get_json(f"/api/v1/latest_depth/{serial}")
        if data.get("status") != "success":
            raise ImbrrApiError(
                f"latest_depth failed: {data.get('message', 'unknown error')}"
            )
        return data

    async def async_get_latest_flow_event(self, serial: str) -> list[FlowReading]:
        """Return all readings of the most recent flow event."""
        body = await self._async_get(f"/api/v1/latest_flow_event/{serial}")
        return self._parse_readings_csv(body)

    async def async_get_cistern_stats(self, serial: str) -> dict[str, Any]:
        """Return cistern statistics. Raises ImbrrApiError for well devices."""
        data = await self._async_get_json(f"/api/v1/cistern_stats/{serial}")
        if data.get("status") != "success":
            raise ImbrrApiError(
                f"cistern_stats failed: {data.get('message', 'unknown error')}"
            )
        return data

    async def async_get_pump_cycles(self, serial: str) -> list[PumpCycle]:
        """Return the last 7 days of pump cycles for a device."""
        data = await self._async_get_json(f"/api/v1/pump_cycles/{serial}")
        if data.get("status") != "success":
            raise ImbrrApiError(
                f"pump_cycles failed: {data.get('message', 'unknown error')}"
            )
        return [self._parse_pump_cycle(cycle) for cycle in data.get("cycles", [])]

    async def async_get_devices(self) -> list[ImbrrDevice]:
        """Discover the account's devices via the documented devices endpoint."""
        data = await self._async_get_json("/api/v1/devices")
        if data.get("status") != "success":
            raise ImbrrApiError(
                f"devices failed: {data.get('message', 'unknown error')}"
            )
        devices: list[ImbrrDevice] = []
        for item in data.get("devices", []):
            serial = str(item.get("serial", "")).upper()
            if not serial:
                continue
            devices.append(
                ImbrrDevice(
                    serial=serial,
                    name=str(item.get("name") or serial),
                    device_type=item.get("device_type") or TYPE_WELL,
                )
            )
        return devices

    # ------------------------------------------------------------------
    # Dashboard endpoints (undocumented but stable in practice)
    # ------------------------------------------------------------------

    async def async_download_readings(
        self, serial: str, start: date, end: date
    ) -> list[FlowReading]:
        """Download all raw readings for a device between two dates (inclusive)."""
        readings: list[FlowReading] = []
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=MAX_DOWNLOAD_SPAN_DAYS), end)
            body = await self._async_get(
                "/dashboard/",
                {
                    "id": serial,
                    "download": "true",
                    "date_range": "custom",
                    "custom_start": chunk_start.isoformat(),
                    "custom_end": chunk_end.isoformat(),
                },
                timeout=DOWNLOAD_TIMEOUT,
            )
            readings.extend(self._parse_readings_csv(body))
            chunk_start = chunk_end + timedelta(days=1)
        readings.sort(key=lambda r: r.reading_id)
        return readings

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_readings_csv(self, body: str) -> list[FlowReading]:
        """Parse a readings CSV body (flow event or raw download) tolerantly."""
        body = body.lstrip("\ufeff")
        if not body.strip() or body.lstrip().startswith("<"):
            # Empty result or an HTML page instead of CSV
            return []
        readings: list[FlowReading] = []
        reader = csv.DictReader(io.StringIO(body))
        if not reader.fieldnames or "reading_id" not in reader.fieldnames:
            return []
        for row in reader:
            reading_id = _to_int(row.get("reading_id"))
            timestamp = self.parse_timestamp(row.get("timestamp", ""))
            if reading_id is None or timestamp is None:
                continue
            readings.append(
                FlowReading(
                    reading_id=reading_id,
                    timestamp=timestamp,
                    unique_id=_to_int(row.get("unique_id")),
                    gallons=_to_float(row.get("gallons")) or 0.0,
                    flow=_to_float(row.get("flow")),
                    psi=_to_float(row.get("psi")),
                    temp=_to_float(row.get("temp")),
                    depth_to_water=_to_float(row.get("depth_to_water")),
                    hide_from_graph=str(row.get("hide_from_graph", "0")).strip()
                    not in ("0", "", "0.0"),
                )
            )
        return readings

    def _parse_pump_cycle(self, cycle: dict[str, Any]) -> PumpCycle:
        """Parse one pump-cycle dict, e.g. time '2026-07-03 22:43:00', duration '01:50'."""
        when = self.parse_timestamp(str(cycle.get("time", "")))

        duration_seconds: int | None = None
        raw_duration = str(cycle.get("duration", "")).strip()
        if raw_duration:
            parts = raw_duration.split(":")
            try:
                numbers = [int(p) for p in parts]
            except ValueError:
                numbers = []
            if len(numbers) == 2:
                duration_seconds = numbers[0] * 60 + numbers[1]
            elif len(numbers) == 3:
                duration_seconds = numbers[0] * 3600 + numbers[1] * 60 + numbers[2]

        return PumpCycle(
            time=when,
            gpm=_to_float(cycle.get("gpm")),
            trimmed_gpm=_to_float(cycle.get("trimmed_gpm")),
            gallons=_to_float(cycle.get("gallons")),
            duration_seconds=duration_seconds,
            start_psi=_to_float(cycle.get("start_psi")),
            stop_psi=_to_float(cycle.get("stop_psi")),
            raw=cycle,
        )
