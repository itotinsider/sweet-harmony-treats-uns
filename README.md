# Waffle Line 01 — MQTT Simulator & HMI

Simulates a waffle production line (Sweet Harmony Treats, Munich) and publishes ~46 process values
every 5 seconds to an MQTT broker. Includes a SCADA-style web HMI with a button that injects a
reproducible energy anomaly, for testing anomaly-detection agents.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Ports free on the host: **1883** (MQTT), **8090** (HMI), **1880** (Node-RED), **4000** (MQTT Explorer)

## 1. Start everything

From this directory:

```bash
docker compose up -d --build
```

This starts the whole stack, broker included: an [Eclipse Mosquitto](https://mosquitto.org/)
broker (`waffle-line-mosquitto`, config in `mosquitto/config/mosquitto.conf`), the simulator,
Node-RED and MQTT Explorer. The broker listens on **localhost:1883** with no username or password,
so external clients (e.g. an AI agent) on the host connect there; containers in the stack reach it
by its service name, `mosquitto:1883`. Retained messages are persisted in `mosquitto/data/`.

If you previously ran a standalone broker on port 1883 (e.g. the old `hivemq` container), stop it
first: `docker rm -f hivemq`.

## 2. Open the hub

http://localhost:8090

A directory of every system in this environment — the two SCADA HMIs, Quality Lab, Production
(MES), Maintenance, Node-RED and MQTT Explorer. Click into **Modern HMI** (`/modern`) first: you
should see live values across the line, a green **Broker … connected** indicator in the header,
and the specific-energy-consumption trend filling in over the first minute.

## 3. Verify data is flowing

```bash
docker logs waffle-line-simulator          # expect "Connected to mosquitto:1883"
```

Or subscribe with any MQTT client to `v1.0/SHT/#` (covers the structured tree, the raw feed, and
everything Node-RED publishes) — or just open **MQTT Explorer** at http://localhost:4000, where a
"Waffle Line 01" connection is already saved and pointed at the broker; click Connect.

## Injecting an energy anomaly

Click **⚡ Generate energy anomaly** in the HMI (or `curl -X POST localhost:8090/api/anomaly`).

It simulates a clogged oven air filter: filter pressure and fan speeds rise, airflow falls, and oven
power climbs from ~85 kW to ~113 kW while throughput stays flat — so energy per pack rises ~34% for
about a minute, then returns to normal. The button re-enables when it's over.

Nothing about the injection is published over MQTT. A diagnosing agent has to work the cause out
from the process topics alone.

## Topic structure

Every tag is published twice, to two deliberately different namespaces:

**Structured**, an ISA-95-flavoured tree:

```
v1.0/SHT/Munich/Production/WaffleLine01/<Area>/<Module>/<Metric>
```

for example `…/WaffleLine01/Oven/HeatingZone01/TemperatureTop`.

**Raw**, flat and unstructured — the same value, the same instant, no folders:

```
v1.0/SHT/Munich_RAW/Production/<Area>_<Module>_<Metric>
```

for example `Oven_HeatingZone01_TemperatureTop`. It's meant to look like an unrelated historian
feed, as a contrast with the structured tree above — same data, no hierarchy imposed on it.

Both use the same JSON payload:

```json
{"value": 207.42, "unit": "°C", "timestamp": "2026-09-17T12:49:10.251+00:00"}
```

## ERP/LIMS exports & Node-RED

Two backend systems export a flat CSV the moment something meaningful happens, mimicking a batch
extract from a real ERP/LIMS — both files live under `exports/` on the host, and nothing else
writes to them:

- **`exports/workorders.csv`** — one row appended the moment a work order is *created* in the
  Maintenance HMI (http://localhost:8090/maint); never updated on start/complete.
- **`exports/lab_samples.csv`** — one row appended the moment a result is *registered* in the
  Lab HMI (http://localhost:8090/lab), for either stage.

**View them:** http://localhost:8090/exports (live-rendered tables, reload to see new rows), or
download the raw files at `/exports/workorders.csv` and `/exports/lab_samples.csv`. They're also
just normal files on disk — open them in a spreadsheet if you like.

**Node-RED** (http://localhost:1880) has two pre-loaded flows, each polling its CSV every 10
seconds and publishing to the same MQTT broker the simulator uses, under a namespace that puts
the domain right after the site (`SHT/Munich/<Domain>/WaffleLine01/…`), unlike the structured
process tree which puts the line before the domain:

- **`Maintenance → Unified Namespace`** publishes each functional location's *most recent* work
  order — one retained topic per location, overwritten by whatever was created there last, not
  one topic per order:

  ```
  v1.0/SHT/Munich/Maintenance/WaffleLine01/<FunctionalLocation>/LatestWorkOrder
  ```

- **`Quality → Unified Namespace`** publishes each new Proofing result as three retained topics
  (Oven-stage results aren't published yet):

  ```
  v1.0/SHT/Munich/Quality/WaffleLine01/Proofing/DoughCoreTemperature   (value + unit)
  v1.0/SHT/Munich/Quality/WaffleLine01/Proofing/DoughPh                (value + unit)
  v1.0/SHT/Munich/Quality/WaffleLine01/Proofing/LatestSample           (the whole record, JSON)
  ```

All of the above are retained, so subscribing after the fact still shows the current state.

**First run:** Node-RED logs `ENOENT … workorders.csv` / `… lab_samples.csv` until the first work
order / lab result exists — harmless, it just means that export doesn't exist yet.

## Browsing MQTT with MQTT Explorer

[MQTT Explorer](https://mqtt-explorer.com/) — as a browser-based build, so nothing to install — is
included at http://localhost:4000, for browsing the topic tree, watching values live, and
publishing test messages by hand.

A connection named **Waffle Line 01** (`mosquitto:1883`, no auth, subscribed to
`v1.0/SHT/#`) is pre-saved — open the page and click **Connect**. Saved connections persist across
restarts in `mqtt_explorer_data/settings.json` on the host; delete that file (or just remove the
connection in the UI) to start over.

## Configuration

Set these under `environment:` in `docker-compose.yml`. Defaults apply whether or not the variable is
listed there — several are code defaults only.

| Variable | Default | Purpose |
|---|---|---|
| `MQTT_HOST` | `mosquitto` | Broker address **as seen from inside the container** (the compose service name) |
| `MQTT_PORT` | `1883` | Broker port |
| `PUBLISH_INTERVAL_SECONDS` | `5` | Publish rate |
| `MQTT_QOS` | `0` | QoS for published messages |
| `MQTT_CLIENT_ID` | `sweet-harmony-waffle-line-01-simulator` | MQTT client identifier |
| `WEB_PORT` | `8090` | HMI port |
| `ANOMALY_RAMP_SECONDS` | `12` | Ramp in/out time of the anomaly |
| `ANOMALY_HOLD_SECONDS` | `36` | Time held at full severity |
| `AGENT_KPI_TOPIC` | `…/KPIs/EnergyConsumptionPerPack` | External KPI topic shown on the HMI |
| `EXPORT_DIR` | `./exports` (code default) / `/app/exports` (compose) | Where the ERP/LIMS CSV exports are written |
| `PROOFING_SAMPLE_INTERVAL_SECONDS` | `360` | Interval between scheduled Proofing lab samples |
| `OVEN_SAMPLE_INTERVAL_SECONDS` | `300` | Interval between scheduled Oven lab samples |
| `SAMPLE_GRACE_SECONDS` | `120` | Time past due before a pending lab sample is flagged overdue |

## Troubleshooting

**"Broker … offline" in the HMI.** Check the broker container is running
(`docker ps` → `waffle-line-mosquitto`, `docker logs waffle-line-mosquitto`). Inside a container
`localhost` means the container itself — that's why `MQTT_HOST` is the service name `mosquitto`.
If Mosquitto won't start because port 1883 is taken, another broker is still running on the host.

**Code changes don't show up.** `simulator.py` and `web/` are baked into the image. Rebuild:

```bash
docker compose up -d --build
```

**Port already in use.** Change the left-hand side of the port mapping in `docker-compose.yml`
(e.g. `"9090:8090"`), or stop whatever holds the port.

## Stopping

```bash
docker compose down
```
