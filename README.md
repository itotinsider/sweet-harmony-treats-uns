# Waffle Line 01 — MQTT Simulator & HMI

> **Disclaimer.** This code is part of the [IT/OT Academy](https://itot.academy), offered by
> [The IT/OT Insider](https://itotinsider.com). It is a generated, vibe-coded system made **only for
> demo purposes**. It may not be used for anything other than demonstration or training: not in
> production, not connected to real equipment, and not as the basis for any real control, quality
> or maintenance decision.
>
> Its main purpose is to explain the **Unified Namespace (UNS)**, so it includes one: process data,
> maintenance orders and quality results from separate systems, published into a single
> ISA-95-structured MQTT topic tree (see [The ISA-95 structure](#the-isa-95-structure) below).

Simulates a waffle production line (Sweet Harmony Treats, Munich) and publishes ~46 process values
every 5 seconds to an MQTT broker. Includes a SCADA-style web HMI with a button that injects a
reproducible energy anomaly, for testing anomaly-detection agents.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Ports free on the host: **1883** (MQTT), **8090** (HMI), **1880** (Node-RED), **4000** (MQTT Explorer)

## Easiest way to start
Open Claude Code in this directory and run the following prompt:
```
read the CLAUDE.md file and README.md file and set up the environment 
```

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

## How the bakery works

**Sweet Harmony Treats** is a fictional bakery in Munich. Its one modelled production line,
**Waffle Line 01**, makes yeasted waffles continuously, in six process areas from raw material to
packed carton:

1. **Ingredient Handling** — flour, sugar and yeast are held in silos and dosed into the line
   together with process water (silo levels, dosing flow, water flow).
2. **Batter Mixing** — a mixer combines them into ~250 kg batches of batter (mixer speed and motor
   current, batter temperature and viscosity, batch weight).
3. **Proofing** — the yeasted dough rests ~45 minutes in a warm (35 °C), humid (80 %) chamber so it
   rises (chamber temperature, humidity and pressure, proofing time, dough height).
4. **Oven** — the heart of the line, and the most detailed part of the model. A belt carries the
   waffles through two gas-fired **heating zones** (~210 °C and ~225 °C, top and bottom), each with
   its own burner valve, gas flow and circulation fan. An **air circulation** system (exhaust fan,
   airflow, and the pressure drop across the air filter) and **humidity control** (steam valve)
   condition the oven atmosphere, and **utility metering** measures its electricity (~85 kW) and
   gas use.
5. **Cooling** — a cooling conveyor brings the product down to ~28 °C (conveyor and fan speed,
   product temperature and moisture).
6. **Packaging** — waffles are sealed into packs at ~60 packs/minute and packed 12 to a carton
   (pack rate, reject rate, seal temperature, film tension, carton count).

The headline KPI is **specific energy consumption**: oven electricity per pack produced (kWh/pack).
It is the number the energy anomaly below disturbs.

Around the line run the plant systems people use day to day, each with its own screen in the hub:

- **SCADA** (`/modern`, `/classic`): operators watch the live process and start or stop each area.
- **Quality Lab** (`/lab`): a technician takes scheduled dough samples at Proofing and baked samples
  at the Oven, and registers results against spec limits.
- **Production (MES)** (`/mes`): production orders for three recipes (Classic Belgian, Chocolate
  Chip, Wholegrain Honey). Each has a bill of materials and a six-step checklist, one step per
  process area.
- **Maintenance** (`/maint`): SAP-PM-style work orders against functional locations such as
  `WL01-OVN-AIR`.

These systems are deliberately *not* integrated with each other. Each keeps its own data, the way
it often is in a real plant, and the Unified Namespace is what brings them together.

## The ISA-95 structure

ISA-95 (IEC 62264) describes a plant as a hierarchy of equipment levels. The structured topic tree
follows that hierarchy. For example, the top temperature of the oven's first heating zone:

```
v1.0/SHT/Munich/Production/WaffleLine01/Oven/HeatingZone01/TemperatureTop
```

| Topic segment | ISA-95 level | In this plant |
|---|---|---|
| `v1.0` | — | Namespace version, so the tree can change later without breaking consumers |
| `SHT` | Enterprise | Sweet Harmony Treats |
| `Munich` | Site | The Munich bakery |
| `Production` | Area | The production area of the site |
| `WaffleLine01` | Work center (production line) | Waffle Line 01 |
| `Oven` | Work unit | One of the six process areas |
| `HeatingZone01` | Equipment module | A part of the unit (ISA-88 terminology) |
| `TemperatureTop` | Data point (tag) | The measured value, with unit and timestamp |

The same tree carries more than process data. Node-RED publishes the maintenance and quality
systems' records into it, under the same enterprise and site but a different domain:

```
v1.0/SHT/Munich/Production/WaffleLine01/Oven/AirCirculation/DifferentialPressure     ← SCADA
v1.0/SHT/Munich/Maintenance/WaffleLine01/Oven/AirCirculation/LatestWorkOrder         ← Maintenance
v1.0/SHT/Munich/Quality/WaffleLine01/Proofing/DoughPh                                ← Quality lab
```

That is the point of a UNS. A consumer, such as the diagnosing AI agent, can find the oven filter's
pressure, the latest work order on that filter and the latest dough quality result by navigating
one structure, without integrating with the SCADA, the maintenance system and the LIMS one by one.
Compare it with the flat `Munich_RAW` feed described under [Topic structure](#topic-structure):
same values, no structure.

The systems themselves also map onto the ISA-95 *functional* levels, which is how the hub at
http://localhost:8090 groups them:

| Level | Role | Here |
|---|---|---|
| Level 0–1 | Physical process, sensors and actuators | Simulated by `simulator.py` |
| Level 2 | Monitoring and control | Modern and Classic SCADA HMIs |
| Level 3 | Manufacturing operations (MOM) | MES, Quality Lab (LIMS), Maintenance |
| IT/OT integration | Unified Namespace | Mosquitto broker, Node-RED, MQTT Explorer |

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
