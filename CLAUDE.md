# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A demo MQTT data simulator for a fictional waffle production line (Sweet Harmony Treats, Munich, Waffle Line 01). It publishes ~46 simulated process values every `PUBLISH_INTERVAL_SECONDS` (default 5s) to a local broker, and serves a SCADA-style HMI that can inject a reproducible energy anomaly. The purpose is to give an external AI agent (running separately, not in this repo) realistic data to detect a KPI deviation and diagnose its root cause.

Seven files hold everything: `simulator.py` (data model, MQTT client, HTTP API) and six self-contained pages under `web/`:

- `index.html` — **the hub**, a static directory of every system below plus Node-RED/MQTT Explorer/exports, grouped loosely by ISA-95 level. At `/`. This is the only page with no API calls and no live data — it's pure navigation, on purpose: the individual persona pages used to each carry a full cross-link nav bar to every sibling system, which got old the moment there were five of them, so that nav collapsed to one link back here (`⌂ All systems`) and this page took over being the map.
- `modern.html` — the modern HMI. At `/modern`. (Note: **not** at `/` — that's the hub now.)
- `classic.html` — a 1990s Windows-95-era SCADA look: beveled 3D chrome, saturated ISA-101-violating colours, a marquee alarm ticker. At `/classic`.
- `lab.html` — a quality-lab / LIMS-style screen for a lab technician, not a SCADA mimic: clean light business-app styling, scoped to Proofing and Oven sampling only. See **Quality lab sampling** below. At `/lab`.
- `maint.html` — an old-school SAP-PM-flavoured plant-maintenance screen (think IW38): dense ALV-grid order list, red/yellow/green system-status traffic lights, a selection-screen filter bar with an Execute button. See **Work orders** below. At `/maint`.
- `mes.html` — a very basic, non-integrated MES: a dark shop-floor Andon/dispatch board (three columns — Scheduled / In Progress / Completed), not a desktop app. See **MES (production orders)** below. At `/mes`.

No build step, no framework, no dependencies beyond CDN-free vanilla JS in each file. The two SCADA-style pages (`modern.html`, `classic.html`) duplicate their own copy of the relevant data-model arrays (`AREAS`, `OVEN_TAGS`, `FANS`, `METERS`, `LIMITS`) and rendering code rather than sharing a module — keep both in sync by hand when the signal model changes. `lab.html`, `maint.html` and `mes.html` instead fetch their reference data (sample-parameter specs; functional locations/work centers/order types/priorities/downtime reasons; recipes/BOMs/steps) from the API rather than hardcoding it, since that data conceptually belongs to the backend system-of-record (the "LIMS", the "SAP PM", the "MES"), not the page. All routes are in `simulator.py`'s `do_GET`.

Four more services run alongside the app (`docker-compose.yml`): a `mosquitto` container ([Eclipse Mosquitto](https://mosquitto.org/), `eclipse-mosquitto:2`) — the MQTT broker everything else connects to, config in `mosquitto/config/mosquitto.conf`, retained-message persistence in the bind-mounted `./mosquitto/data`; a bind-mounted `./exports` folder holding the ERP/LIMS CSV exports (see **ERP/LIMS exports & Node-RED** below); a `nodered` container (official `nodered/node-red` image) with its own bind-mounted `./nodered_data`, pre-loaded with the flows in `nodered_data/flows.json`; and an `mqtt-explorer` container ([smeagolworms4/mqtt-explorer](https://github.com/Smeagolworms4/MQTT-Explorer), a Node.js/browser build of [MQTT Explorer](https://mqtt-explorer.com/) — not the Electron desktop app) at http://localhost:4000, with its own bind-mounted `./mqtt_explorer_data`.

## Commands

```bash
docker compose up -d --build     # build + (re)start; REQUIRED after any code change
docker compose down              # stop
docker logs -f waffle-line-simulator
curl -s localhost:8090/api/state | python3 -m json.tool | head -40
curl -X POST localhost:8090/api/anomaly     # trigger the anomaly (409 if one is running)
curl -s localhost:8090/api/samples | python3 -m json.tool   # lab sampling queue/history
curl -s localhost:8090/api/workorders | python3 -m json.tool   # maintenance order list
```

There is no test suite, linter, or package manifest beyond `requirements.txt` (paho-mqtt only). Verification is done by observing the broker and the HMI.

**Verify over MQTT** by running a throwaway container from the built image (which already has paho-mqtt) on the compose network:

```bash
docker run --rm --network sweet-harmony-treats-main_default \
  --entrypoint python sweet-harmony-treats/waffle-line-simulator:latest -c '
import paho.mqtt.client as mqtt, time
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.on_message = lambda cl, u, m: print(m.topic, m.payload.decode())
c.connect("mosquitto", 1883)
c.subscribe("v1.0/SHT/#")
c.loop_start(); time.sleep(3); c.loop_stop()'
```

**Screenshot the HMI** (headless Chrome is available; Playwright is not):

```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --disable-gpu \
  --hide-scrollbars --window-size=1440,1000 --virtual-time-budget=4000 \
  --screenshot=/tmp/hmi.png http://localhost:8090/modern
```

Chrome will not lay out narrower than ~500px, so a 400px capture is a cropped 500px layout, not a real phone viewport.

## Environment assumptions

- The broker is **Eclipse Mosquitto, a service in this compose file** (`waffle-line-mosquitto`), port 1883, no auth (`allow_anonymous true`). It replaced a standalone HiveMQ container on the host; there is no control-center UI any more — use MQTT Explorer. Port 1883 is still published to the host so the external agent can connect to `localhost:1883`.
- From inside a container, `localhost` is the container. Every service reaches the broker by its compose service name, `mosquitto` (simulator `MQTT_HOST`, Node-RED's `cfg-broker`, MQTT Explorer's saved connection). Never "fix" this back to localhost.
- HMI is on 8090.
- `simulator.py` and `web/` are **copied into the image**, not mounted. Editing e.g. `web/modern.html` and reloading the browser changes nothing until you rebuild. This is the most common mistake when iterating on the UI.

## Architecture

**Signal model.** `SIGNALS` is a nested dict mirroring the topic tree. Leaves are one of three node types:

- `Signal` — setpoint + slow sine (random period/phase) + gaussian noise, optionally clamped, rounded to fixed decimals.
- `Derived` — computed from other values so related meters stay physically consistent (mains current from oven kW at 400V/PF 0.95; total gas as the sum of both zone burner flows).
- `Counter` — monotonic production count integrated from a rate signal (cartons = packs ÷ 12). Resets on restart.

`flatten()` walks the dict into `"Area/Module/Metric"` paths; the MQTT topic is `TOPIC_PREFIX + "/" + path`. The publish loop is two-pass: all `Signal`s first, then `Derived`/`Counter` (which read the just-computed values dict). Payload is `{"value", "unit", "timestamp"}`.

**Topic naming.** Derived from a customer topic-tree diagram: spaces, hyphens and parentheses removed, parent label folded into the metric name (`Gas burner: Valve position` → `GasBurnerValvePosition`; `Proofing (yeasted waffles)` → `Proofing`). Keep new topics consistent with this.

**Two namespaces, same tags.** Every tick, each tag is published twice: once under `TOPIC_PREFIX` (`v1.0/SHT/Munich/Production/WaffleLine01/<Area>/<Module>/<Metric>` — the structured, ISA-95-flavoured tree everything else in this doc calls "the topic") and once under `RAW_TOPIC_PREFIX` (`v1.0/SHT/Munich_RAW/Production/<Area>_<Module>_<Metric>` — the exact same JSON payload, just flattened to one segment per tag with `/` → `_`, and note `Munich_RAW` is a sibling of `Munich`, not nested under it). The raw feed is a deliberate teaching contrast — an "unmodelled historian dump" sitting next to a proper Unified Namespace — not a different data source; both are written from the exact same `values` dict in the same publish loop, so they can never disagree. `AGENT_KPI_TOPIC`'s default is derived from `TOPIC_PREFIX`, so it moves with it.

**Equipment run states.** `UNITS` (one `Equipment` per process area — same ids as `AREAS` in every HMI) is start/stoppable via `POST /api/units/<id>/start|stop`. `Equipment.factor(now)` is 0..1 and ramps with a cosine ease over `start_s`/`stop_s`. `GATE_SPEC`/`GATES` tie individual signals to a unit: each gated path blends from its normal "running" sample toward an `off` value through a first-order lag `tau` (seconds) — fast for a fan (2–4s), slow for oven-zone temperatures (90s) so a stop/start looks like a real motor/heater, not a switch. Gating runs in the main loop *before* `Derived` nodes, so mains current/total gas/cartons settle correctly on their own. Only signals tied to an actual motor, heater or flow are gated (see the comment above `GATE_SPEC`) — silo inventory and slow product properties are not. `GET /api/state` includes `"units"`. **A stopped/starting/stopping unit's tags are not evaluated against `LIMITS`** — every HMI's `checkLimit()` checks the owning unit (`path.split("/")[0]`) is `"running"` first, otherwise clears/skips the alarm, since an `off` target can sit outside a real limit (e.g. airflow's off-value is below its low limit) without meaning anything is wrong. `POST /api/anomaly` is rejected with 409 unless the Oven unit is `"running"`.

**Anomaly injection.** `EnergyAnomaly` models a clogged oven air filter. `IMPACT` maps topic path → `(offset at full severity, lag in seconds)`. Severity follows a cosine-eased envelope: ramp up (12s), hold (36s), ramp down (12s), plus up to 8s of lagging effects — the button stays disabled for the full ~68s, then waits for another click. `offsets()` is added into each `Signal.sample()` call, before gating — so the anomaly's effect is itself naturally suppressed while the Oven is stopped.

Three design constraints that are easy to break:

1. **The anomaly is never published.** Nothing in MQTT or the payloads says an anomaly is active; the agent must diagnose it from process topics alone. Don't add a status topic.
2. **Effects are ordered by lag** so the cause precedes its effects: differential pressure (0s) → airflow (1s) → fan speeds (3s) → power (4s) → zone temperatures (6s) → burner valves (8s). **This ordering is in wall-clock seconds, not publish ticks** — at the current 5s `PUBLISH_INTERVAL` several of these lags (1s, 3s, 4s) fall *inside* one publish gap, so a client sampling only at publish time sees them all move together rather than in sequence. The lag values themselves weren't changed to compensate; if the step-by-step ordering needs to be visible again, either shorten `PUBLISH_INTERVAL_SECONDS` back down or widen `IMPACT`'s lags to multiples of the new interval.
3. **`ElectricityTotalOven` and `PacksPerMinute` have deliberately small amplitude/noise** (±1.5 kW, ±0.8 packs) so the kWh-per-pack baseline stays within ±0.1% and the SMA crosses a +15% threshold exactly once per injection (peak ~+34% at the original 1s/20-sample setup; re-verified at 5s/4-sample after the `SMA_WINDOW` fix below, peak ~+29% — same shape, comfortably over the threshold). **`SMA_WINDOW` is derived from `SMA_WINDOW_SECONDS` (20) ÷ `PUBLISH_INTERVAL`, not a fixed sample count** — at 1 sample/5s a fixed 20-*sample* window would have been 100s, longer than the whole ~68s anomaly, and empirically the SMA never even reached +15% (measured peak +14.3%, arriving *after* the anomaly had already gone `idle`). If `PUBLISH_INTERVAL_SECONDS` changes again, this scales automatically; only worry about it if `SMA_WINDOW_SECONDS`, `SEC_LIMIT`, or `ANOMALY_HOLD_SECONDS` change instead. Raising `ElectricityTotalOven`/`PacksPerMinute`'s variation would independently cause false detections regardless of interval.

**Threading.** One `ThreadingHTTPServer` daemon thread serves the HMI; paho runs its own network thread; the main thread does the publish loop (every `PUBLISH_INTERVAL`, default 5s — was 1s; constraint 2 under **Anomaly injection** covers what changing this interval still costs, even after `SMA_WINDOW` was made to scale with it). `LiveState` (mutex-guarded) is the only shared state: latest values, SEC history, and the external agent's KPI. MQTT publishing happens only on the main thread.

**External agent KPI.** The simulator also *subscribes* to `KPIs/#` and surfaces the agent's own KPI on the HMI. Two quirks to preserve: the agent publishes to `…/KPIs/EnergyConsumptionPerPack ` **with a trailing space** (matching is `topic.strip()`, and the HMI flags it), and its payload is a **bare float**, not JSON (`parse_number()` accepts both). Its value is oven kW ÷ packs-per-minute, i.e. kW·min/pack — 60× larger than kWh/pack; the HMI shows both.

**HMI.** Single file, no build. Polls `GET /api/state` every second and re-renders; `POST /api/anomaly` triggers injection. Layout config lives in JS consts near the top (`AREAS`, `OVEN_TAGS`, `FANS`, `METERS`, `LIMITS`) — add new topics there rather than editing markup. Alarm limits are client-side with a 2% deadband for clearing. Fans and belt animate via `requestAnimationFrame` driven by live values.

**Equipment Control panel.** Present on both SCADA-style pages (a plain "Equipment"/"Equipment Control" panel, styled per page). Built from the same `AREAS` array, one row per area, calling `POST /api/units/<id>/start|stop`. Each page keeps a `units` object refreshed from `s.units` at the top of `render()` — `checkLimit()`, the area badges, and the fault-injection button (disabled with a note when the Oven isn't `"running"`) all read from it. Running/stopped/starting/stopping gets a colour (green/grey/amber) consistent with each page's existing alarm colour language.

**Quality lab sampling (`lab.html`, Proofing & Oven only).** `SAMPLE_TYPES` defines the two sample types — id, label, MQTT-style prefix (`PRF`/`OVN`), interval, and a `params` list (`id`, `label`, `unit`, `lsl`, `usl`, optionally `options` for a select/scale instead of a number input, e.g. baked colour 1–5). This is deliberately independent of `SIGNALS`: a lab technician measures a physical sample off-line, so these are typed, spec-limited entry forms, not sensor readbacks — there is no automatic link to e.g. `Cooling/ProductMoisture`, even though a real lab would cross-check its wet-chemistry moisture result against that sensor.

A `SamplingPlan` per stage (in the module-level `SAMPLING` dict) schedules recurring samples: at most one *scheduled* occurrence is open at a time (a technician works one through before the next is due), though any number of ad-hoc ones can sit alongside it. `next_due` starts at process-start time so the queue isn't empty on first load. `tick(now)`, called from `GET /api/samples` before building the snapshot, creates a new occurrence once `next_due` has passed and nothing is open, and flags a `pending` occurrence `overdue` once `SAMPLE_GRACE_SECONDS` (default 120s) has elapsed past `due_at`. There's no other scheduler loop — the queue only advances when something polls it.

API: `GET /api/samples` → `{server_time, stages: {Proofing, Oven}}`, each with `label`, `params`, `next_due`, `open` (list of `{id, stage, due_at, status, adhoc}`), and `history` (last 50, newest first). `POST /api/samples/<stage>/adhoc` creates an unscheduled occurrence for that stage. `POST /api/samples/<sample_id>/result` (body: `{values, visualCheck: "pass"|"fail", note, technician}`) evaluates each numeric param against its `lsl`/`usl`, ANDs in the visual check, moves the occurrence from `open` to `history` with the computed `result` and per-param `checks`, and returns it. Pass/fail is computed server-side once, so `lab.html` and any future consumer read the same verdict rather than re-deriving it.

`lab.html` keeps a `cardEls` map (sample id → DOM element) across polls specifically so an in-progress registration form is never overwritten by the next 3-second poll — only a card's status chip/due-time text is patched on refresh; the form subtree is built once and left alone until submitted or cancelled. History rows and the per-parameter trend sparklines (last 12 results, spec band shaded, out-of-spec points in red) are safe to fully re-render every poll since they hold no live user input.

**Work orders (`maint.html`).** `WORK_ORDER_META` holds the reference lists (functional locations, work centers, order types, priorities, downtime reasons) served to the page rather than hardcoded there. `WorkOrderLog` is a flat, most-recent-first list of order dicts with a SAP-PM-style status flow — `CRTD` (created) → `REL` (released / work in progress) → `TECO` (technically completed) — matching real SAP PM, where TECO requires REL first. `_seed()` pre-populates three starter orders spanning all three statuses so the list isn't empty on first load. This is deliberately **not** linked to the Equipment run-state engine (`UNITS`/`Equipment`): creating or working a maintenance order never starts or stops anything on the process pages — the two systems only share a functional-location vocabulary (`WL01-<AREA>`), which does *not* need to match `AREAS`'/`SAMPLE_TYPES`' ids exactly (it's one level more granular for the Oven, splitting it into its modules).

API: `GET /api/workorders` → `{meta, orders}`. `POST /api/workorders` (body: `order_type, priority, functional_location, work_center, short_text, breakdown, reported_by`) creates a `CRTD` order. `POST /api/workorders/<id>/start` moves `CRTD → REL` (409 if not `CRTD`). `POST /api/workorders/<id>/complete` (body: `work_performed, work_duration_h, downtime_duration_h, downtime_reason`) moves `REL → TECO` (409 if not `REL`); `work_performed` isn't otherwise validated, `downtime_reason` is dropped silently if it's not one of `WORK_ORDER_META["downtime_reasons"]`.

`maint.html` uses the same "never rebuild an open form from underneath the user" discipline as `lab.html`, generalised: each row's `rowEls` entry tracks `detailStatus` (the order status its detail panel was last built for), and `renderRow()` only calls `render_detail()` again when the order's live status differs from that — i.e. on a real CRTD→REL or REL→TECO transition (including the one driven by the row's own Start/Complete button, which calls `renderRow()` directly) — never on a routine poll tick while status is unchanged. **This is the general pattern for any future page with an inline edit form fed by a poll: gate the rebuild on a value that actually changes at the moment the form should be replaced, never on the poll itself,** or typed input gets silently wiped out from under the user.

**MES / production orders (`mes.html`).** `RECIPES` holds the 3 waffle products (Classic Belgian, Chocolate Chip, Wholegrain Honey), each an ISA-88-style Master Recipe: a name, `packs_per_batch`, and a `bom` list (`material`, `unit`, `qty` per one batch). `MES_STEPS` is the fixed 6-step checklist — ISA-88 unit procedures — with ids matching the `AREAS`/functional-location vocabulary elsewhere (`Oven`, not `Baking`) so the systems stay easy to cross-reference; steps are a flat checklist, not an enforced sequence, so an operator can tick them in whatever order things actually happened.

`MESLog` orders move `scheduled → in_progress → completed`; `_seed()` pre-populates 4 orders spanning all three (`PO-10039` completed, `PO-10042` in progress with 2 of 6 steps ticked, `PO-10041`/`PO-10043` scheduled) so the board isn't empty on first load. `_materials_for(recipe, batches)` computes each material's *standard* quantity (BOM qty × batches) at order-creation time; `actual` starts `null` and is only ever filled in by `complete()`, matched by material name — **this is deliberately a single end-of-run report, not per-step or progressive material tracking**, combining what the initial proposal called "report material usage" and "report total yield" into one action, since a real non-integrated MES operator reports both at the same moment anyway. This whole subsystem is **not linked to the live process signals or the Equipment run-state engine** — ticking a step or reporting a yield never touches `SIGNALS`/`UNITS`, and nothing here is read back from them either; a non-integrated MES's data is exactly and only what an operator typed in.

API: `GET /api/mes` → `{recipes, steps, orders}`. `POST /api/mes/orders/<id>/start` (`scheduled → in_progress`, 409 otherwise). `POST /api/mes/orders/<id>/steps/<step_id>` (body `{done}`, 409 unless `in_progress`). `POST /api/mes/orders/<id>/complete` (body `{actuals, yield_good, yield_scrap, note}`, 409 unless `in_progress`) — `actuals` is `{material_name: value}`; any material name not in the order's BOM is silently ignored.

`mes.html`'s `placeTile()` generalises the same discipline as `maint.html`/`lab.html` once more: a tile is only fully rebuilt (`renderScheduled`/`renderInProgress`/`renderCompleted`) when the order's status actually changes (or after the tile's own action succeeds, which calls `placeTile(order, true)` directly) — a routine poll on an unchanged `in_progress` order calls `syncSteps()` only, which touches just the step chips, never the open "Complete & Report" form.

**ERP/LIMS exports & Node-RED.** Two independent CSV exports, same shape of code, same pattern:

- `export_work_order_csv()` appends one row to `EXPORT_DIR/workorders.csv` (`WORK_ORDER_CSV_FIELDS`) — called only from `WorkOrderLog.create()`, i.e. only on `CRTD`, never on start/complete, mimicking a periodic ERP batch extract rather than a live sync. `_seed()`'s starter orders deliberately bypass this (no export call), so the CSV only ever reflects orders actually created through the UI/API. `WorkOrderLog._resume_counter()` scans the existing CSV at startup so a fresh container never reissues an id already sitting in a previous export row.
- `export_lab_sample_csv()` appends one row to `EXPORT_DIR/lab_samples.csv` (`LAB_SAMPLE_CSV_FIELDS`) — called from `SamplingPlan.submit()`, i.e. when a *result* is registered (the meaningful event for a sample; there's no equivalent of `CRTD` here since an unregistered occurrence isn't a record of anything yet). One wide table covers every stage's parameters, blank where not applicable to that row's stage — a single flat extract, the way a real LIMS export usually looks, rather than one file per stage.

`render_exports_page()` (served at `GET /exports`; raw files at `GET /exports/workorders.csv` and `GET /exports/lab_samples.csv`) is the one page in this repo generated inline in Python rather than as a `web/*.html` file, showing both tables — it's a read-only utility view of the CSVs, not a persona HMI, so a static template didn't earn its own file.

Node-RED (`docker-compose.yml`'s `nodered` service, official image, `FLOWS=flows.json` pinned so a container recreation doesn't rename the flow file away from what's checked in) runs **two tabs** in `nodered_data/flows.json`, sharing one `mqtt-broker` config node (`cfg-broker`):

- **`Maintenance → Unified Namespace`** re-reads `workorders.csv` every 10s. The core `csv` node parses it — **in `multi: "mult"` mode this still delivers one message whose `payload` is the array of every row in the file, not one message per row**. The dedup function groups rows by `functional_location`, keeps only the *last* (most recently created) row per location, and only lets one through if that location's latest `order_id` has changed since the last publish (`flow.context.lastOrderIdByLocation`) — this is deliberately not "seen this order before": the namespace holds one *current-state* topic per location, not one topic per order ever created. The topic-builder function maps `functional_location` through a small `FL_PATH` lookup (hand-kept in sync with `WORK_ORDER_META["functional_locations"]` in simulator.py — no single source of truth shared between Python and the flow) and builds `v1.0/SHT/Munich/Maintenance/WaffleLine01/<FLPath>/LatestWorkOrder`, retained, with an explicit `domain: "maintenance"` in the JSON payload. Creating a second order at the same location overwrites the first's retained message.
- **`Quality → Unified Namespace`** re-reads `lab_samples.csv` every 10s the same way, but the dedup function also filters `stage !== "Proofing"` — **the Oven stage isn't published yet**, only Proofing, matching what was actually asked for; extending it means adding an equivalent branch (own `FL_PATH`-style constant if needed, own dedup keyed by `sample_id`, own topic builder) alongside this one, not editing it in place. The topic-builder function fans one sample out to **three** retained messages under `v1.0/SHT/Munich/Quality/WaffleLine01/Proofing/`: `DoughCoreTemperature` and `DoughPh` (each `{value, unit, timestamp}`, matching the simulator's own payload shape) plus `LatestSample` (the whole record as JSON, `domain: "quality"`). Returning `[[msgA, msgB, msgC]]` — one output, three messages — is how a Node-RED function fans out to multiple published messages from a single input.

Both flows' `mqtt out` nodes have `retain: true`, so a subscriber connecting after the fact still sees current state. Every tab carries numbered `comment` nodes explaining its stages — keep them current if a flow is ever extended, they're what makes it presentable without narration.

**MQTT Explorer.** `mqtt_explorer_data/settings.json` pre-seeds one saved connection, `"Waffle Line 01"` (`mosquitto:1883`, subscribed to `v1.0/SHT/#`) — this is the app's own real settings-file schema (`ConnectionManager_connections`, keyed by connection name), confirmed by inspecting a fresh container's generated file, not guessed from the (undocumented) `INITIAL_CONFIG` env var the image also exposes. The app persists further UI changes (new connections, expanded-node state, etc.) back to that same file, so edits made in the browser survive a container restart; delete the file to reset to empty. No topic-naming constant lives here to keep in sync with anything else — it's a generic MQTT client, not aware of this repo's data model.

**Training pane.** The left-hand collapsible pane (open state remembered in `localStorage`) is for teaching use. `ITEMS` (near the end of the script) holds its accordion items; each has a title, intro and a `tree`. Every tree node carries a CSS selector `sel` for the SCADA elements it points at, and a `topic` shown below the tree. Selecting a node adds the `.hl` class to the matches. Oven mimic tags are addressable as `[data-tag="<path>"]`, meter cards as `[data-meter="<path>"]`, and the zone frames as `#zone-HeatingZone01/02`. To add an item, add an entry to `ITEMS` and, if it needs new anchors, give the markup an id or data attribute. Ghost nodes (`…`) are placeholders for branches not yet modelled.

Styling follows ISA-101 conventions: muted greys, colour reserved for alarms; the caramel accent is brand/agent-KPI only and is deliberately distinct from the amber warning colour.
