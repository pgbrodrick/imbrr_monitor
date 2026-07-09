# imbrr for Home Assistant

A [HACS](https://hacs.xyz) custom integration for [imbrr](https://www.imbrr.com) well and cistern water-monitoring systems (IMB-WMS1).

Unlike a simple "latest value" poller, this integration is built to preserve the **complete data profile through time**:

- During pump/flow events your imbrr sensor records a reading every ~5 seconds. The integration detects new data and pulls **every raw reading** from the imbrr cloud — not just whatever value happened to be current at poll time. This works even across Home Assistant restarts or extended downtime: missed readings are backfilled automatically.
- A persistent **Total water** sensor accumulates every gallon ever pumped (verified to match imbrr's own per-event accounting exactly), ready to use on Home Assistant's **Water dashboard**.
- Hourly **long-term statistics** (mean/min/max depth-to-water, flow, pressure, and temperature) are written directly onto each sensor with the readings' true timestamps, so a sensor's History card shows backfilled and live data as one continuous series. On first setup, the last 30 days (configurable) of history are imported.
- Optional **MQTT real-time updates**: if your imbrr device is connected to your local MQTT broker, pushed readings update the entities instantly between cloud polls.

Each imbrr unit appears as its own device in Home Assistant.

## Installation

### HACS (recommended)

1. In Home Assistant, open **HACS**.
2. Click the three-dot menu (top right) → **Custom repositories**.
3. Add pgbrodrick/imbrr_monitor, with type **Integration**.
4. Search for **imbrr** in HACS and click **Download**.
5. Restart Home Assistant.

### Manual

1. Download `imbrr.zip` from the latest [release](../../releases) (or copy the `custom_components/imbrr` folder from this repository).
2. Extract/copy it to `config/custom_components/imbrr` in your Home Assistant configuration directory.
3. Restart Home Assistant.

## Setup

1. Go to **Settings → Devices & services → Add integration** and search for **imbrr**.
2. Enter the **email address and password** you use to sign in at www.imbrr.com.
   Your credentials are stored by Home Assistant's config-entry storage and are only ever sent to imbrr's servers.
3. The integration discovers the devices on your account — select the ones you want and finish.

On first setup the integration imports the last 30 days of history (configurable) as long-term statistics on the sensors; this runs in the background just after the entities are created and can take a minute. The **Total water** counter also starts from this window — it reflects water pumped since (backfill window before) install, not since the device was manufactured. (If the device is newer than the backfill window, you only get history back to when it came online.)

## Entities

### Well devices

| Entity | Description |
|---|---|
| Depth to water | Water level below the sensor (ft) |
| Flow rate | Live pumping rate (gal/min); 0 when idle |
| Water temperature | Water temperature (°F) |
| Pressure | Line pressure (psi) |
| Current event water | Gallons in the flow event currently in progress / most recently completed |
| **Total water** | Lifetime accumulated gallons (persistent; Water-dashboard ready) |
| Flow active | On while water is flowing |
| Last reading | Timestamp of the most recent reading (diagnostic) |
| Last pump cycle rate / water / duration / start & stop pressure | Summary of the most recent pump cycle (diagnostic) |

### Cistern devices

Everything above (as applicable), plus: Storage (gal), Storage percentage, Usage last 24 hours, Usage last 31 days, Water temperature, Pressure, and Last connected.

### Long-term statistics

The Depth to water, Flow rate, Pressure, and Water temperature sensors get **hourly statistics imported directly onto the entity itself** (mean/min/max), with each reading's true timestamp. So the sensor's own History card — and any Statistics graph card pointed at the sensor — shows the backfilled history and the live data as one continuous line, including data from events that happened between polls or while Home Assistant was off. There's no separate statistic ID to hunt for; use the sensor entities directly.

> Note: Home Assistant cannot inject past *states*, only statistics. So a sensor's short-term (recent, dot-by-dot) history still only covers time since install, but its long-term statistics — what History shows for older ranges — include the full backfilled/gap-filled profile.

## Water dashboard

**Settings → Dashboards → Energy → Water consumption**: add the **Total water** sensor as a water source (it's a `device_class: water`, total-increasing sensor).

## Example dashboard

A ready-made single-page dashboard that exercises every entity — status glance with icons, gauges, a live "pump running" banner, and Plotly graphs for pressure over time, coupled water depth (axis reversed so the water surface is at the top) + use, and a daily cumulative-water curve — is in [`examples/well-dashboard.yaml`](examples/well-dashboard.yaml). It only needs the [Plotly Graph Card](https://github.com/dbuezas/lovelace-plotly-graph-card) (HACS → Frontend); the cumulative graph integrates flow inside the card, so no helper is required. Paste the `views:` block into a dashboard's YAML editor and find/replace the `well_` entity prefix with your device's.

## Options

**Settings → Devices & services → imbrr → Configure**:

| Option | Default | Description |
|---|---|---|
| Update interval | 60 s | How often the imbrr cloud is polled |
| Poll faster while water is flowing | on | Temporarily poll at the fast interval during flow events |
| Fast update interval | 15 s | Poll rate while a flow event is active |
| Use MQTT for real-time updates | off | Merge readings pushed to your local MQTT broker (see below) |
| MQTT topic pattern | `imbrr/+/state` | Topic/wildcard the integration subscribes to |
| Device timezone | (blank) | Timezone of the imbrr cloud timestamps; blank uses Home Assistant's |
| History backfill window | 30 days | How much history to import — only used on first setup |

## MQTT real-time updates (optional)

The imbrr device itself can publish readings directly to your local MQTT broker. Two things happen when you set this up:

1. The device registers itself via native MQTT discovery, creating its own set of push-updated entities under the MQTT integration.
2. If you also enable **Use MQTT for real-time updates** in this integration's options, pushed readings (flow, temperature, pressure, depth-to-water) update *this* integration's entities instantly between cloud polls — and a flow push while idle triggers an immediate cloud refresh. All totals and statistics still come exclusively from the cloud data, so nothing is ever double-counted.

To connect the device to your broker:

1. Install and start the **Mosquitto broker** add-on and make sure the **MQTT** integration shows as configured (**Settings → Devices & services**). Anonymous connections are not allowed, so the device needs a real login.
2. Create a **dedicated Home Assistant user** for the device: **Settings → People → Add person**, name it `imbrr_devices`, toggle **Allow login**, and set the password to `pass1234` (the device's factory defaults) — or pick your own username/password and enter those on the device page in step 4. A dedicated user is strongly recommended over your personal HA login (see the security note below). If the connection is refused, make sure the user is *not* restricted to "local network only" login.
3. Browse to the device's local setup page: `http://imbrr_<SERIAL>.local/` (lowercase serial works too; use the device's IP address if mDNS doesn't resolve).
4. In **Host / IP Address**, enter **only the hostname or IP** of your Home Assistant machine — e.g. `homeassistant.local` or `192.168.1.50`. **Do not include a port or `http://`.** In particular, don't append `:8123` (that's HA's web port; the device talks MQTT on port 1883 and adds that automatically).
5. Enter the username/password from step 2, click **Save MQTT Settings**, and wait a few seconds — the status line should change to **Connected**, and if you go to Settings → Devices → MQTT, you should see the device. 

### Connect MQTT to the integration

Getting the device onto your broker (above) only creates the device's *own* MQTT-discovery entities under the MQTT integration. To have **this integration** use those real-time readings for its entities, you have to turn MQTT on in its options — it is **off by default**:

1. **Settings → Devices & services → imbrr → Configure.**
2. Enable **Use MQTT for real-time updates**.
3. Leave the **MQTT topic pattern** at its default `imbrr/+/state` (or set it explicitly to `imbrr/<SERIAL>/state`), then submit. The integration reloads and subscribes.

**How the device publishes.** imbrr firmware publishes a single JSON state message to `imbrr/<SERIAL>/state`, for example:

```json
{"depth_ft":91.56,"temp_f":61.03,"pressure_psi":48.32,"flow_gpm":0.00,"event_gallons":0.000,"flow_event_status":"completed"}
```

The integration reads `depth_ft`, `temp_f`, `pressure_psi`, `flow_gpm`, `event_gallons`, and `flow_event_status` from that payload and updates the matching entities in real time (the device publishes continuously, so depth/temperature/pressure stay live even when the pump is idle). It attributes the message to a device by finding the serial in the topic, so with one device the default pattern just works.

> **Note:** the pattern must actually match the topic. `imbrr/<SERIAL>` (no trailing segment) does **not** match `imbrr/<SERIAL>/state` — MQTT topic matching is exact unless you use a wildcard. Use `imbrr/+/state`, `imbrr/<SERIAL>/state`, or `imbrr/#`.

If your firmware publishes a different topic or payload shape, capture it (below) and open an issue so the parser can be extended.

To see exactly what your device sends: **Settings → Devices & services → MQTT → Configure → Listen to a topic**, enter `#`, **Start listening**.

**Verify it's actually being used:**

- Turn on debug logging — add this to `configuration.yaml` and restart:
  ```yaml
  logger:
    logs:
      custom_components.imbrr: debug
  ```
  On startup you should see `imbrr subscribed to MQTT topic <your pattern>`. (If you instead see a warning that the MQTT integration is not set up, finish the MQTT integration setup first.)
- **Behavioral check**: run water and watch this integration's **Flow rate** / **Depth to water** entities — on the device named after your dashboard location, *not* the separate "imbrr Monitoring System" device that MQTT discovery creates. With MQTT working they update within a second or two; with it off they only change on each cloud poll (60 s by default).

Totals and long-term statistics always come from the cloud data, never from MQTT, so real-time updates can never double-count your water usage.

### MQTT troubleshooting

If the status stays **Disconnected**:

- **Re-open the page and check what was actually saved.** The device stores the full broker URI; if you included a port in the host field it gets saved verbatim. The saved value must end in `:1883` — if you see `:8123` (or anything else), re-enter the bare host/IP and save again.
- **Verify the broker is reachable**: from any machine on your network, `nc -z <HA-IP> 1883` should report the port open. If not, the Mosquitto add-on isn't running.
- **Verify the credentials**: the username/password must match an HA user with "Allow login" enabled (or a user defined in the Mosquitto add-on's own configuration). The device's defaults (`imbrr_devices` / `pass1234`) only work if you actually created that user. You can test with `mosquitto_pub -h <HA-IP> -u <user> -P <password> -t test -m hi` from any machine with mosquitto clients installed.
- After fixing anything on the HA side, click **Save MQTT Settings** once more to force an immediate reconnect.

> **Security note:** the device's local setup page is unauthenticated and will show the stored MQTT password to anyone on your network. Use a dedicated, least-privilege MQTT user for the device — never your own Home Assistant login.

## FAQ

**Why doesn't Total water match my well's true lifetime?**
It counts from the backfill window at install time forward. imbrr's cloud is still the authoritative record.

**What happens if Home Assistant is off during a flow event?**
Nothing is lost. On the next poll the integration notices the reading-id watermark advanced, downloads the missed days of raw data, and accounts for every reading exactly once.

**Backfilled history didn't appear immediately after setup.**
The backfill runs in the background right after the entities are created and can take a minute; imported statistics are hourly, so the current hour only appears once it has fully elapsed. Check the sensor's History card over a longer range (e.g. the last week).

**I want to (re)load older history on demand.**
The automatic backfill runs only once. To re-pull history at any time — for example to recover data from before you first installed, or after a long outage — call the **`imbrr.import_history`** action (Developer tools → Actions → *imbrr: Import history*), optionally with a `days` value. It re-imports statistics onto the sensors and never changes the lifetime total, so it's safe to run repeatedly.

**Timestamps look shifted.**
imbrr reports timestamps in the device's local timezone. If your Home Assistant timezone differs from the device's, set **Device timezone** in the options.

## Troubleshooting

- **Re-authentication required**: if your imbrr password changes, Home Assistant will prompt you to re-enter it (Settings → Devices & services → imbrr).
- **Debug logging**: add to `configuration.yaml`:

  ```yaml
  logger:
    logs:
      custom_components.imbrr: debug
  ```

- **Removing and re-adding** the integration re-runs the backfill and re-imports statistics onto the sensors (overwriting overlapping hours). Long-term statistics for a removed sensor can be cleared from Developer tools → Statistics.

## Disclaimer

This is a third-party integration, not affiliated with imbrr. It uses only the documented imbrr API v1.
