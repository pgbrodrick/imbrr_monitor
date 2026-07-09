"""Tests for the imbrr API client (pure aiohttp, no Home Assistant)."""

from __future__ import annotations

from datetime import date, timezone

import aiohttp
import pytest
from aioresponses import aioresponses

from custom_components.imbrr.api import (
    ImbrrApiClient,
    ImbrrAuthError,
    ImbrrConnectionError,
)
from custom_components.imbrr.const import TYPE_CISTERN, TYPE_WELL

from .conftest import TEST_SERIAL, load_fixture

BASE = "https://www.imbrr.com"


@pytest.fixture
async def session():
    async with aiohttp.ClientSession() as client_session:
        yield client_session


@pytest.fixture
def client(session) -> ImbrrApiClient:
    return ImbrrApiClient(session, "user@example.com", "secret", timezone.utc)


def mock_login_success(mocked: aioresponses) -> None:
    mocked.post(
        f"{BASE}/login",
        status=302,
        headers={"Location": f"{BASE}/dashboard/"},
    )
    mocked.get(f"{BASE}/dashboard/", status=200, body="<html>dashboard</html>")


async def test_login_success(client) -> None:
    with aioresponses() as mocked:
        mock_login_success(mocked)
        await client.async_login()


async def test_login_invalid_credentials(client) -> None:
    with aioresponses() as mocked:
        mocked.post(
            f"{BASE}/login",
            status=302,
            headers={"Location": f"{BASE}/login?error=empty_fields"},
        )
        mocked.get(f"{BASE}/login?error=empty_fields", status=200, body="login page")
        with pytest.raises(ImbrrAuthError):
            await client.async_login()


async def test_login_connection_error(client) -> None:
    with aioresponses() as mocked:
        mocked.post(f"{BASE}/login", exception=aiohttp.ClientConnectionError("boom"))
        with pytest.raises(ImbrrConnectionError):
            await client.async_login()


async def test_latest_depth(client) -> None:
    with aioresponses() as mocked:
        mock_login_success(mocked)
        mocked.get(
            f"{BASE}/api/v1/latest_depth/{TEST_SERIAL}",
            status=200,
            body=load_fixture("latest_depth.json"),
        )
        data = await client.async_get_latest_depth(TEST_SERIAL)
    assert data["depth_to_water"] == 136.416
    assert data["flow_event_status"] == "completed"


async def test_relogin_once_on_expired_session(client) -> None:
    """An expired session (401) triggers exactly one re-login and a retry."""
    with aioresponses() as mocked:
        mock_login_success(mocked)
        mocked.get(
            f"{BASE}/api/v1/latest_depth/{TEST_SERIAL}",
            status=401,
            body='{"status":"failed","message":"User not authenticated"}',
        )
        mock_login_success(mocked)  # re-login
        mocked.get(
            f"{BASE}/api/v1/latest_depth/{TEST_SERIAL}",
            status=200,
            body=load_fixture("latest_depth.json"),
        )
        data = await client.async_get_latest_depth(TEST_SERIAL)
    assert data["status"] == "success"


async def test_relogin_fails_raises_auth_error(client) -> None:
    """If the session stays broken after a re-login, raise ImbrrAuthError."""
    with aioresponses() as mocked:
        mock_login_success(mocked)
        mocked.get(f"{BASE}/api/v1/latest_depth/{TEST_SERIAL}", status=401, body="{}")
        mock_login_success(mocked)
        mocked.get(f"{BASE}/api/v1/latest_depth/{TEST_SERIAL}", status=401, body="{}")
        with pytest.raises(ImbrrAuthError):
            await client.async_get_latest_depth(TEST_SERIAL)


async def test_flow_event_csv_parsing(client) -> None:
    """The flow-event CSV parses fully and sums to the server's total."""
    readings = client._parse_readings_csv(load_fixture("flow_event.csv"))
    assert len(readings) == 22
    total = sum(r.gallons for r in readings)
    # Verified against the live API's accumulated_gallons for this event.
    assert total == pytest.approx(8.552385, abs=0.001)
    assert readings[0].hide_from_graph is True
    assert readings[1].hide_from_graph is False
    assert readings[1].flow == pytest.approx(5.5619, abs=0.001)
    assert readings[0].timestamp.tzinfo is not None


async def test_download_csv_parsing_with_extra_column(client) -> None:
    """The raw download CSV has an extra max_pk column; parsing tolerates it."""
    readings = client._parse_readings_csv(load_fixture("download_week.csv"))
    assert len(readings) == 20
    assert all(r.reading_id > 0 for r in readings)


async def test_csv_parsing_edge_cases(client) -> None:
    assert client._parse_readings_csv("") == []
    assert client._parse_readings_csv("<html>not csv</html>") == []
    header_only = "reading_id,gallons,timestamp\n"
    assert client._parse_readings_csv(header_only) == []
    blanks = 'reading_id,gallons,flow,timestamp\n7,,NULL,"2026-07-03 10:00:00"\n'
    readings = client._parse_readings_csv(blanks)
    assert readings[0].gallons == 0.0
    assert readings[0].flow is None


async def test_device_discovery(client) -> None:
    """Devices are discovered from dashboard markup and classified by probe."""
    dashboard = load_fixture("dashboard.html")
    with aioresponses() as mocked:
        mock_login_success(mocked)
        mocked.get(f"{BASE}/dashboard/", status=200, body=dashboard)
        # Per-device pages for the numeric id
        mocked.get(
            f"{BASE}/dashboard/?id={TEST_SERIAL}",
            status=200,
            body="<script>const deviceId = '115';</script>",
        )
        mocked.get(
            f"{BASE}/api/v1/cistern_stats/{TEST_SERIAL}",
            status=200,
            body='{"status":"failed","message":"This endpoint is only available for cistern devices"}',
        )
        mocked.get(f"{BASE}/dashboard/?id=112233445566", status=200, body="<html/>")
        mocked.get(
            f"{BASE}/api/v1/cistern_stats/112233445566",
            status=200,
            body=load_fixture("cistern_stats.json"),
        )
        devices = await client.async_get_devices()

    assert [d.serial for d in devices] == [TEST_SERIAL, "112233445566"]
    well, cistern = devices
    assert well.name == "Test Well Site"
    assert well.numeric_id == "115"
    assert well.device_type == TYPE_WELL
    assert cistern.name == "Test Cistern Site"
    assert cistern.device_type == TYPE_CISTERN


async def test_download_readings_chunks_long_ranges(client) -> None:
    """Ranges longer than one chunk issue multiple download requests."""
    csv_body = load_fixture("download_week.csv")
    with aioresponses() as mocked:
        mock_login_success(mocked)
        # 90 days => 3 chunked requests; match any download query.
        import re as _re

        mocked.get(
            _re.compile(rf"{BASE}/dashboard/\?.*download=true.*"),
            status=200,
            body=csv_body,
            repeat=True,
        )
        readings = await client.async_download_readings(
            TEST_SERIAL, date(2026, 4, 1), date(2026, 6, 30)
        )
    assert len(readings) == 60  # 20 rows x 3 chunks
    assert readings == sorted(readings, key=lambda r: r.reading_id)


async def test_pump_cycles_parsing(client) -> None:
    with aioresponses() as mocked:
        mock_login_success(mocked)
        mocked.get(
            f"{BASE}/api/v1/pump_cycles/{TEST_SERIAL}",
            status=200,
            body=load_fixture("pump_cycles.json"),
        )
        cycles = await client.async_get_pump_cycles(TEST_SERIAL)

    assert len(cycles) == 3
    first = cycles[0]
    assert first.gpm == 4.7
    assert first.gallons == 8.6
    assert first.duration_seconds == 110  # "01:50"
    assert first.start_psi == 44.1
    assert first.time is not None
    assert (first.time.year, first.time.hour) == (2026, 22)  # 10:43pm
