"""MQTT data simulator and HMI for Sweet Harmony Treats - Munich - Waffle Line 01.

Continuously publishes simulated sensor values following the (structured, ISA-95-flavoured)
topic hierarchy:
    v1.0/SHT/Munich/Production/WaffleLine01/<Area>/<Module>/<Metric>

and, unchanged, the exact same value/unit/timestamp payload a second time under a flat,
unstructured "raw historian" topic per tag — deliberately not nested, as a contrast with the
structured tree above:
    v1.0/SHT/Munich_RAW/Production/<Area>_<Module>_<Metric>

A small web HMI (default port 8090) shows live values and can inject an energy
anomaly. The anomaly itself is never published; its cause has to be diagnosed
from the regular process topics.
"""

import csv
import json
import logging
import math
import os
import random
import signal
import statistics
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import paho.mqtt.client as mqtt

BROKER_HOST = os.getenv("MQTT_HOST", "mosquitto")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "5"))
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "sweet-harmony-waffle-line-01-simulator")
QOS = int(os.getenv("MQTT_QOS", "0"))
WEB_PORT = int(os.getenv("WEB_PORT", "8090"))
ANOMALY_RAMP = float(os.getenv("ANOMALY_RAMP_SECONDS", "12"))
ANOMALY_HOLD = float(os.getenv("ANOMALY_HOLD_SECONDS", "36"))

TOPIC_PREFIX = "v1.0/SHT/Munich/Production/WaffleLine01"
# Same tags, same payloads, published a second time under a flat/unstructured topic per tag —
# a deliberate contrast with the structured tree above. Note the "_RAW" site segment, not a
# sub-path of the structured tree: this is meant to look like an unrelated, un-modelled feed.
RAW_TOPIC_PREFIX = "v1.0/SHT/Munich_RAW/Production"
# KPI published by the external agent, shown on the HMI. Matched ignoring surrounding
# whitespace, so a topic published with a stray trailing space is still picked up.
AGENT_KPI_TOPIC = os.getenv("AGENT_KPI_TOPIC", f"{TOPIC_PREFIX}/KPIs/EnergyConsumptionPerPack").strip()
AGENT_KPI_SUBSCRIPTION = AGENT_KPI_TOPIC.rsplit("/", 1)[0] + "/#"
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
# Mimics a periodic batch export from an ERP's maintenance module. A separate volume so the
# host and the Node-RED container can both see it without reaching into the app image.
EXPORT_DIR = os.getenv("EXPORT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "exports"))
WORK_ORDER_CSV = os.path.join(EXPORT_DIR, "workorders.csv")
WORK_ORDER_CSV_FIELDS = [
    "order_id", "order_type", "priority", "functional_location", "work_center",
    "short_text", "breakdown", "reported_by", "status",
    "created_at", "started_at", "completed_at",
    "work_performed", "work_duration_h", "downtime_duration_h", "downtime_reason",
    "export_timestamp",
]
LAB_SAMPLE_CSV = os.path.join(EXPORT_DIR, "lab_samples.csv")
# One wide table covering every stage's parameters (blank where not applicable) — a single
# flat extract, the way a real LIMS export usually looks, rather than one file per stage.
LAB_SAMPLE_CSV_FIELDS = [
    "sample_id", "stage", "adhoc", "due_at", "completed_at", "technician",
    "visual_check", "result", "note",
    "doughTemp", "doughPh", "proofHeight", "coreTemp", "moisture", "colorScore",
    "export_timestamp",
]
# Smoothing window for the SEC KPI, held at a constant ~20s of wall-clock time rather than a
# fixed sample count — otherwise changing PUBLISH_INTERVAL_SECONDS silently changes how long the
# anomaly detector takes to react (at 1 sample/5s, 20 samples would be a 100s window, longer than
# the whole ~68s anomaly, and the SMA would never actually reach the +15% alarm threshold).
SMA_WINDOW_SECONDS = 20
SMA_WINDOW = max(1, round(SMA_WINDOW_SECONDS / PUBLISH_INTERVAL))
HISTORY_LENGTH = 300

POWER = "Oven/OvenUtilityMetering/ElectricityTotalOven"
PACKS = "Packaging/PacksPerMinute"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("simulator")


class Signal:
    """A value that drifts around a setpoint with a slow sine wave and random noise."""

    def __init__(self, setpoint, amplitude, noise, unit, low=None, high=None, decimals=2):
        self.setpoint = setpoint
        self.amplitude = amplitude
        self.noise = noise
        self.unit = unit
        self.low = low
        self.high = high
        self.decimals = decimals
        self.phase = random.uniform(0, 2 * math.pi)
        self.period = random.uniform(120, 600)  # seconds

    def sample(self, t, offset=0.0):
        value = (
            self.setpoint
            + self.amplitude * math.sin(2 * math.pi * t / self.period + self.phase)
            + random.gauss(0, self.noise)
            + offset
        )
        if self.low is not None:
            value = max(self.low, value)
        if self.high is not None:
            value = min(self.high, value)
        return round(value, self.decimals)


class Derived:
    """A value calculated from other signals, so related meters stay consistent."""

    def __init__(self, unit, fn, decimals=2):
        self.unit = unit
        self.fn = fn
        self.decimals = decimals

    def compute(self, values):
        return round(self.fn(values), self.decimals)


class Counter:
    """Monotonic production counter, integrated from a per-minute rate signal."""

    def __init__(self, unit, source, per_unit=1.0, interval=PUBLISH_INTERVAL):
        self.unit = unit
        self.source = source
        self.per_unit = per_unit
        self.interval = interval
        self.total = 0.0

    def compute(self, values):
        self.total += values[self.source] / 60 * self.interval / self.per_unit
        return int(self.total)


class Equipment:
    """A start/stoppable unit of equipment (one per process area).

    `factor` is 0..1: 0 fully stopped, 1 fully running, in between while ramping.
    It drives every `Gate` tied to this unit. Ramping uses the same cosine ease as
    the energy anomaly, so starts and stops feel like a real motor/heater, not a switch.
    """

    def __init__(self, label, start_s=5.0, stop_s=5.0, running=True):
        self.label = label
        self.start_s = start_s
        self.stop_s = stop_s
        self.lock = threading.RLock()
        now = time.monotonic()
        self.target = running
        self.since = now
        self.from_factor = 1.0 if running else 0.0

    def factor(self, now):
        with self.lock:
            dur = self.start_s if self.target else self.stop_s
            elapsed = now - self.since
            if elapsed >= dur:
                return 1.0 if self.target else 0.0
            end = 1.0 if self.target else 0.0
            return self.from_factor + (end - self.from_factor) * ease(elapsed / dur)

    def command(self, run):
        with self.lock:
            if run == self.target:
                return False
            now = time.monotonic()
            self.from_factor = self.factor(now)
            self.target = run
            self.since = now
            log.info("%s %s", self.label, "starting" if run else "stopping")
            return True

    def state(self, now):
        f = self.factor(now)
        if self.target:
            return "running" if f >= 0.999 else "starting"
        return "stopped" if f <= 0.001 else "stopping"

    def status(self, now):
        return {"label": self.label, "running": self.target, "state": self.state(now), "factor": round(self.factor(now), 3)}


class Gate:
    """Ties one signal's value to an `Equipment` unit's run state.

    Blends the raw (fully "running") sample toward `off` as the unit's factor falls,
    through a first-order lag `tau` (seconds) so the transition has physical inertia —
    a fan spins down over a couple of seconds, an oven zone cools over a couple of minutes.
    """

    def __init__(self, unit, off, tau):
        self.unit = unit
        self.off = off
        self.tau = tau
        self.smoothed = None

    def apply(self, raw, dt, now):
        factor = self.unit.factor(now)
        target = self.off + factor * (raw - self.off)
        if self.smoothed is None:
            self.smoothed = target
        alpha = 1 - math.exp(-dt / self.tau)
        self.smoothed += (target - self.smoothed) * alpha
        return self.smoothed


def heating_zone(top_c, bottom_c):
    return {
        "TemperatureTop": Signal(top_c, 3, 0.5, "°C"),
        "TemperatureBottom": Signal(bottom_c, 3, 0.5, "°C"),
        "GasBurnerValvePosition": Signal(62, 8, 1.5, "%", 0, 100, 1),
        "GasBurnerMassFlow": Signal(18.5, 2, 0.3, "Nm³/h", 0),
        "CirculationFanSpeed": Signal(75, 5, 1, "%", 0, 100, 1),
    }


# Metrics under "…" / unexpanded nodes in the topic tree are reasonable placeholders.
SIGNALS = {
    "IngredientHandling": {
        "FlourSiloLevel": Signal(68, 10, 0.5, "%", 0, 100, 1),
        "SugarSiloLevel": Signal(54, 8, 0.5, "%", 0, 100, 1),
        "DosingFlowRate": Signal(420, 25, 5, "kg/h", 0),
        "YeastSiloLevel": Signal(46, 8, 0.5, "%", 0, 100, 1),
        "WaterFlowRate": Signal(310, 20, 4, "l/h", 0),
    },
    "BatterMixing": {
        "MixerSpeed": Signal(120, 6, 1.5, "rpm", 0, decimals=1),
        "BatterTemperature": Signal(22, 1.5, 0.2, "°C"),
        "BatterViscosity": Signal(8500, 400, 80, "cP", 0, decimals=0),
        "MixerMotorCurrent": Signal(24, 2, 0.4, "A", 0),
        "BatchWeight": Signal(250, 12, 2, "kg", 0, decimals=1),
    },
    "Proofing": {
        "Temperature": Signal(35, 1, 0.2, "°C"),
        "RelativeHumidity": Signal(80, 3, 0.5, "%", 0, 100, 1),
        "ProofingTime": Signal(45, 2, 0.3, "min", 0, decimals=1),
        "ChamberPressure": Signal(15, 3, 0.5, "Pa", decimals=1),
        "DoughHeight": Signal(28, 2, 0.3, "mm", 0, decimals=1),
    },
    "Oven": {
        "HeatingZone01": heating_zone(210, 200),
        "HeatingZone02": heating_zone(225, 215),
        "BeltDriveModule": {
            "BeltSpeed": Signal(3.2, 0.1, 0.02, "m/min", 0),
            "MotorCurrent": Signal(12.5, 1, 0.2, "A", 0),
            "MotorTemperature": Signal(55, 4, 0.5, "°C"),
        },
        "AirCirculation": {
            "ExhaustFanSpeed": Signal(60, 6, 1, "%", 0, 100, 1),
            "AirFlow": Signal(1450, 80, 15, "m³/h", 0, decimals=0),
            "DifferentialPressure": Signal(120, 10, 2, "Pa", decimals=1),
        },
        "HumidityControl": {
            "RelativeHumidity": Signal(35, 4, 0.5, "%", 0, 100, 1),
            "SteamValvePosition": Signal(25, 5, 1, "%", 0, 100, 1),
        },
        "OvenUtilityMetering": {
            # Kept steady so kWh per pack has a quiet baseline for anomaly detection.
            "ElectricityTotalOven": Signal(85, 1.5, 0.4, "kW", 0),
            # 3-phase 400 V supply, power factor 0.95.
            "ElectricityIncomingMainsCurrent": Derived(
                "A", lambda v: v[POWER] * 1000 / (math.sqrt(3) * 400 * 0.95) + random.gauss(0, 0.4)
            ),
            "GasTotalOven": Derived(
                "Nm³/h",
                lambda v: v["Oven/HeatingZone01/GasBurnerMassFlow"]
                + v["Oven/HeatingZone02/GasBurnerMassFlow"]
                + random.gauss(0, 0.1),
            ),
        },
    },
    "Cooling": {
        "ConveyorSpeed": Signal(4.0, 0.15, 0.03, "m/min", 0),
        "ProductTemperature": Signal(28, 2, 0.4, "°C"),
        "AmbientTemperature": Signal(18, 1, 0.2, "°C"),
        "FanSpeed": Signal(55, 6, 1, "%", 0, 100, 1),
        "ProductMoisture": Signal(12.5, 0.8, 0.15, "%", 0, 100),
    },
    "Packaging": {
        "PacksPerMinute": Signal(60, 0.8, 0.3, "packs/min", 0, decimals=1),
        "RejectRate": Signal(0.8, 0.3, 0.1, "%", 0, 100),
        "SealTemperature": Signal(165, 3, 0.5, "°C"),
        "FilmTension": Signal(18, 1.5, 0.3, "N", 0),
        # Cartons of 12 packs, integrated from the pack rate since start-up.
        "CartonCount": Counter("cartons", "Packaging/PacksPerMinute", 12),
    },
}


def ease(x):
    return 0.5 - 0.5 * math.cos(math.pi * x)


# One start/stoppable unit per process area — matches the area ids used throughout the HMIs.
UNITS = {
    "IngredientHandling": Equipment("Ingredient Feed", start_s=4, stop_s=4),
    "BatterMixing": Equipment("Mixer", start_s=5, stop_s=5),
    "Proofing": Equipment("Proofing Chamber", start_s=8, stop_s=8),
    "Oven": Equipment("Oven", start_s=12, stop_s=12),
    "Cooling": Equipment("Cooling", start_s=4, stop_s=4),
    "Packaging": Equipment("Packaging Line", start_s=4, stop_s=4),
}

# relative topic path -> (owning unit, value when fully stopped, settle time constant in seconds).
# Only signals tied to an actual motor, heater or flow are gated; inventory (silo levels), slow
# product properties (viscosity, dough height, moisture) and the anomaly's own dynamics are not.
GATE_SPEC = {
    "IngredientHandling/DosingFlowRate": ("IngredientHandling", 0, 3),
    "IngredientHandling/WaterFlowRate": ("IngredientHandling", 0, 3),
    "BatterMixing/MixerSpeed": ("BatterMixing", 0, 3),
    "BatterMixing/MixerMotorCurrent": ("BatterMixing", 0, 2),
    "BatterMixing/BatterTemperature": ("BatterMixing", 20, 25),
    "Proofing/Temperature": ("Proofing", 24, 40),
    "Proofing/RelativeHumidity": ("Proofing", 45, 30),
    "Proofing/ChamberPressure": ("Proofing", 0, 10),
    "Oven/HeatingZone01/TemperatureTop": ("Oven", 25, 90),
    "Oven/HeatingZone01/TemperatureBottom": ("Oven", 25, 90),
    "Oven/HeatingZone02/TemperatureTop": ("Oven", 25, 90),
    "Oven/HeatingZone02/TemperatureBottom": ("Oven", 25, 90),
    "Oven/HeatingZone01/GasBurnerValvePosition": ("Oven", 0, 5),
    "Oven/HeatingZone02/GasBurnerValvePosition": ("Oven", 0, 5),
    "Oven/HeatingZone01/GasBurnerMassFlow": ("Oven", 0, 5),
    "Oven/HeatingZone02/GasBurnerMassFlow": ("Oven", 0, 5),
    "Oven/HeatingZone01/CirculationFanSpeed": ("Oven", 0, 4),
    "Oven/HeatingZone02/CirculationFanSpeed": ("Oven", 0, 4),
    "Oven/AirCirculation/ExhaustFanSpeed": ("Oven", 0, 4),
    "Oven/AirCirculation/AirFlow": ("Oven", 50, 6),
    "Oven/AirCirculation/DifferentialPressure": ("Oven", 2, 6),
    "Oven/HumidityControl/RelativeHumidity": ("Oven", 30, 20),
    "Oven/HumidityControl/SteamValvePosition": ("Oven", 0, 4),
    "Oven/BeltDriveModule/BeltSpeed": ("Oven", 0, 3),
    "Oven/BeltDriveModule/MotorCurrent": ("Oven", 0, 2),
    "Oven/BeltDriveModule/MotorTemperature": ("Oven", 30, 60),
    "Oven/OvenUtilityMetering/ElectricityTotalOven": ("Oven", 3, 25),
    "Cooling/ConveyorSpeed": ("Cooling", 0, 3),
    "Cooling/ProductTemperature": ("Cooling", 45, 30),
    "Cooling/FanSpeed": ("Cooling", 0, 3),
    "Packaging/PacksPerMinute": ("Packaging", 0, 3),
    "Packaging/SealTemperature": ("Packaging", 25, 20),
    "Packaging/FilmTension": ("Packaging", 0, 3),
}
GATES = {path: Gate(UNITS[unit_id], off, tau) for path, (unit_id, off, tau) in GATE_SPEC.items()}


class EnergyAnomaly:
    """Clogged oven air-circulation filter.

    Airflow resistance rises, so the circulation and exhaust fans speed up to hold
    airflow and draw more electrical power. Heat transfer drops slightly, so zone
    temperatures sag and the burners open up. Throughput is unaffected, so kWh per
    pack rises purely because of oven power. Effects follow the filter with a lag.
    """

    # relative topic path: (offset at full severity, lag in seconds)
    IMPACT = {
        "Oven/AirCirculation/DifferentialPressure": (140, 0),
        "Oven/AirCirculation/AirFlow": (-350, 1),
        "Oven/AirCirculation/ExhaustFanSpeed": (32, 3),
        "Oven/HeatingZone01/CirculationFanSpeed": (20, 3),
        "Oven/HeatingZone02/CirculationFanSpeed": (20, 3),
        POWER: (28, 4),
        "Oven/HeatingZone01/TemperatureTop": (-4, 6),
        "Oven/HeatingZone01/TemperatureBottom": (-3, 6),
        "Oven/HeatingZone02/TemperatureTop": (-4, 6),
        "Oven/HeatingZone02/TemperatureBottom": (-3, 6),
        "Oven/HeatingZone01/GasBurnerValvePosition": (7, 8),
        "Oven/HeatingZone02/GasBurnerValvePosition": (7, 8),
        "Oven/HeatingZone01/GasBurnerMassFlow": (2, 8),
        "Oven/HeatingZone02/GasBurnerMassFlow": (2, 8),
    }

    def __init__(self, ramp, hold):
        self.ramp = ramp
        self.hold = hold
        self.max_lag = max(lag for _, lag in self.IMPACT.values())
        self.duration = 2 * ramp + hold + self.max_lag
        self.started = None
        self.count = 0
        self.lock = threading.Lock()

    def _elapsed(self, now):
        if self.started is None or now - self.started >= self.duration:
            return None
        return now - self.started

    def trigger(self):
        with self.lock:
            now = time.monotonic()
            if self._elapsed(now) is not None:
                return False
            self.started = now
            self.count += 1
            log.info("Energy anomaly #%d injected (%.0fs)", self.count, self.duration)
            return True

    def severity(self, now, lag=0.0):
        elapsed = self._elapsed(now)
        if elapsed is None:
            return 0.0
        e = elapsed - lag
        if e <= 0:
            return 0.0
        if e < self.ramp:
            return ease(e / self.ramp)
        if e < self.ramp + self.hold:
            return 1.0
        if e < 2 * self.ramp + self.hold:
            return ease(1 - (e - self.ramp - self.hold) / self.ramp)
        return 0.0

    def offsets(self, now):
        return {path: delta * self.severity(now, lag) for path, (delta, lag) in self.IMPACT.items()}

    def status(self, now):
        elapsed = self._elapsed(now)
        if elapsed is None:
            state = "idle"
        elif elapsed < self.ramp:
            state = "ramp_up"
        elif elapsed < self.ramp + self.hold:
            state = "hold"
        elif elapsed < 2 * self.ramp + self.hold:
            state = "ramp_down"
        else:
            state = "recovery"
        return {
            "state": state,
            "elapsed": round(elapsed or 0, 1),
            "duration": self.duration,
            "ramp": self.ramp,
            "hold": self.hold,
            "severity": round(self.severity(now, self.IMPACT[POWER][1]), 3),
            "count": self.count,
        }


PROOFING_SAMPLE_INTERVAL = float(os.getenv("PROOFING_SAMPLE_INTERVAL_SECONDS", "360"))
OVEN_SAMPLE_INTERVAL = float(os.getenv("OVEN_SAMPLE_INTERVAL_SECONDS", "300"))
SAMPLE_GRACE_SECONDS = float(os.getenv("SAMPLE_GRACE_SECONDS", "120"))

# Quality-lab sampling plan for the Proofing and Oven stages, for a separate lab-technician /
# LIMS-style screen (web/lab.html). Independent of the live process signals above: a technician
# measures a physical sample off-line, so these are typed, spec-limited entry forms, not sensor
# readbacks. A param with "options" renders as a scale/select client-side rather than a number
# input; its value is still numeric so pass/fail evaluation works the same way for every param.
SAMPLE_TYPES = {
    "Proofing": {
        "label": "Proofing — dough sample",
        "prefix": "PRF",
        "interval": PROOFING_SAMPLE_INTERVAL,
        "params": [
            {"id": "doughTemp", "label": "Dough core temperature", "unit": "°C", "lsl": 33, "usl": 38},
            {"id": "doughPh", "label": "Dough pH", "unit": "pH", "lsl": 4.6, "usl": 5.4},
            {"id": "proofHeight", "label": "Proof height", "unit": "mm", "lsl": 24, "usl": 32},
        ],
    },
    "Oven": {
        "label": "Oven — baked sample",
        "prefix": "OVN",
        "interval": OVEN_SAMPLE_INTERVAL,
        "params": [
            {"id": "coreTemp", "label": "Core temperature at exit", "unit": "°C", "lsl": 90, "usl": 99},
            {"id": "moisture", "label": "Moisture content", "unit": "%", "lsl": 11, "usl": 16},
            {
                "id": "colorScore", "label": "Crumb & crust colour", "unit": "1–5 scale", "lsl": 2, "usl": 4,
                "options": [[1, "Pale"], [2, "Light"], [3, "Golden (ideal)"], [4, "Dark"], [5, "Burnt"]],
            },
        ],
    },
}


class SamplingPlan:
    """Schedules recurring lab samples for one stage.

    At most one *scheduled* sample is open at a time — a technician works one sample through to
    a result before the next is due — but any number of ad-hoc samples can be requested alongside
    it. `tick()` should be called before `snapshot()` on every read so the queue stays current
    even though nothing else in the process drives it.
    """

    def __init__(self, stage):
        self.stage = stage
        self.interval = SAMPLE_TYPES[stage]["interval"]
        self.lock = threading.Lock()
        self.counter = 0
        self.next_due = time.time()  # first sample due right away, so the queue isn't empty on startup
        self.open = []
        self.history = deque(maxlen=50)

    def _new(self, due_at, adhoc=False):
        self.counter += 1
        sample_id = f"{SAMPLE_TYPES[self.stage]['prefix']}-{'A' if adhoc else ''}{self.counter:04d}"
        occ = {"id": sample_id, "stage": self.stage, "due_at": due_at, "status": "pending", "adhoc": adhoc}
        self.open.append(occ)
        return occ

    def tick(self, now):
        with self.lock:
            if not self.open and now >= self.next_due:
                self._new(self.next_due)
                self.next_due += self.interval
            for occ in self.open:
                if occ["status"] == "pending" and now - occ["due_at"] > SAMPLE_GRACE_SECONDS:
                    occ["status"] = "overdue"

    def request_adhoc(self, now):
        with self.lock:
            return dict(self._new(now, adhoc=True))

    def submit(self, sample_id, values, visual_check, note, technician, now):
        with self.lock:
            occ = next((o for o in self.open if o["id"] == sample_id), None)
            if occ is None:
                return None
            checks = {}
            clean_values = {}
            ok = visual_check == "pass"
            for p in SAMPLE_TYPES[self.stage]["params"]:
                try:
                    v = float(values.get(p["id"]))
                except (TypeError, ValueError):
                    v = None
                clean_values[p["id"]] = v
                within = v is not None and p["lsl"] <= v <= p["usl"]
                checks[p["id"]] = within
                ok = ok and within
            record = dict(occ)
            record.update({
                "values": clean_values,
                "checks": checks,
                "visualCheck": visual_check,
                "note": (note or "").strip()[:500],
                "technician": (technician or "").strip()[:80],
                "result": "pass" if ok else "fail",
                "completed_at": now,
            })
            self.open.remove(occ)
            self.history.appendleft(record)
            export_lab_sample_csv(record)
            return record

    def snapshot(self):
        with self.lock:
            return {
                "label": SAMPLE_TYPES[self.stage]["label"],
                "params": SAMPLE_TYPES[self.stage]["params"],
                "next_due": self.next_due,
                "open": list(self.open),
                "history": list(self.history),
            }


SAMPLING = {stage: SamplingPlan(stage) for stage in SAMPLE_TYPES}


# Plant-maintenance work orders (web/maint.html), SAP-PM flavoured: CRTD -> REL -> TECO.
# Deliberately not linked to the Equipment run-state engine — creating or working an order
# here never starts or stops anything on the process pages, the two systems just share a
# functional-location vocabulary.
WORK_ORDER_META = {
    "functional_locations": [
        ["WL01-ING", "Ingredient Handling"],
        ["WL01-MIX", "Batter Mixing"],
        ["WL01-PRF", "Proofing"],
        ["WL01-OVN-Z1", "Oven / Heating Zone 01"],
        ["WL01-OVN-Z2", "Oven / Heating Zone 02"],
        ["WL01-OVN-AIR", "Oven / Air Circulation"],
        ["WL01-OVN-HUM", "Oven / Humidity Control"],
        ["WL01-OVN-BDM", "Oven / Belt Drive Module"],
        ["WL01-OVN-UTL", "Oven / Utility Metering"],
        ["WL01-COOL", "Cooling"],
        ["WL01-PACK", "Packaging"],
    ],
    "work_centers": [
        ["MECH", "Mechanical"],
        ["ELEC", "Electrical"],
        ["INST", "Instrumentation"],
        ["UTIL", "Utilities"],
        ["PROD", "Production"],
    ],
    "order_types": [
        ["PM01", "Corrective Maintenance"],
        ["PM02", "Preventive Maintenance"],
        ["PM03", "Inspection"],
        ["PM04", "Calibration"],
    ],
    "priorities": [
        ["1", "Very High"],
        ["2", "High"],
        ["3", "Medium"],
        ["4", "Low"],
    ],
    "downtime_reasons": [
        ["MECH", "Mechanical failure"],
        ["ELEC", "Electrical fault"],
        ["INST", "Instrumentation / sensor fault"],
        ["PROC", "Process fault"],
        ["UTIL", "Utility failure (power / water / air)"],
        ["MATL", "Material shortage"],
        ["OPER", "Operator error"],
        ["PLND", "Planned maintenance"],
        ["OTHR", "Other"],
    ],
}
_DOWNTIME_REASON_CODES = {code for code, _ in WORK_ORDER_META["downtime_reasons"]}


def _iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds") if ts else ""


def export_work_order_csv(order):
    """Append one row to the (simulated) ERP's maintenance export, mimicking a periodic batch
    extract: append-only, never rewritten, one row per record — never per status change, since
    only *creation* is exported here. Node-RED (see nodered_data/flows.json) reads this file."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    is_new = not os.path.exists(WORK_ORDER_CSV) or os.path.getsize(WORK_ORDER_CSV) == 0
    with open(WORK_ORDER_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=WORK_ORDER_CSV_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow({
            "order_id": order["id"],
            "order_type": order["order_type"],
            "priority": order["priority"],
            "functional_location": order["functional_location"],
            "work_center": order["work_center"],
            "short_text": order["short_text"],
            "breakdown": order["breakdown"],
            "reported_by": order["reported_by"],
            "status": order["status"],
            "created_at": _iso(order["created_at"]),
            "started_at": _iso(order["started_at"]),
            "completed_at": _iso(order["completed_at"]),
            "work_performed": order["work_performed"],
            "work_duration_h": order["work_duration_h"] if order["work_duration_h"] is not None else "",
            "downtime_duration_h": order["downtime_duration_h"] if order["downtime_duration_h"] is not None else "",
            "downtime_reason": order["downtime_reason"] or "",
            "export_timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })


def export_lab_sample_csv(record):
    """Append one row to the (simulated) LIMS's export, the moment a result is registered —
    the meaningful event for a lab sample is submitting a result, not the occurrence becoming
    due. Node-RED's Quality flow reads this file (see nodered_data/flows.json)."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    is_new = not os.path.exists(LAB_SAMPLE_CSV) or os.path.getsize(LAB_SAMPLE_CSV) == 0
    with open(LAB_SAMPLE_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LAB_SAMPLE_CSV_FIELDS)
        if is_new:
            writer.writeheader()
        row = {
            "sample_id": record["id"],
            "stage": record["stage"],
            "adhoc": record["adhoc"],
            "due_at": _iso(record["due_at"]),
            "completed_at": _iso(record["completed_at"]),
            "technician": record["technician"],
            "visual_check": record["visualCheck"],
            "result": record["result"],
            "note": record["note"],
            "export_timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        for param_id, value in record["values"].items():
            if param_id in LAB_SAMPLE_CSV_FIELDS:
                row[param_id] = value if value is not None else ""
        writer.writerow(row)


def _exports_table(rows, cols):
    def esc(v):
        return (v or "").replace("&", "&amp;").replace("<", "&lt;")

    body = "".join(
        "<tr>" + "".join(f"<td>{esc(r.get(c))}</td>" for c in cols) + "</tr>" for r in reversed(rows)
    ) or f'<tr><td colspan="{len(cols)}">No records exported yet.</td></tr>'
    head = "".join(f"<th>{c}</th>" for c in cols)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_exports_page():
    wo_rows = []
    if os.path.exists(WORK_ORDER_CSV):
        with open(WORK_ORDER_CSV, newline="", encoding="utf-8") as f:
            wo_rows = list(csv.DictReader(f))
    lab_rows = []
    if os.path.exists(LAB_SAMPLE_CSV):
        with open(LAB_SAMPLE_CSV, newline="", encoding="utf-8") as f:
            lab_rows = list(csv.DictReader(f))

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data Exports</title>
<style>
  body {{ font: 12px/1.4 -apple-system, Arial, sans-serif; margin: 0; padding: 16px; background:#f3f5f7; color:#1a1a1a; }}
  h1 {{ font-size: 16px; margin: 24px 0 4px; }}
  h1:first-of-type {{ margin-top: 0; }}
  .sub {{ color:#5b6675; margin-bottom: 12px; }}
  .links {{ margin-bottom: 14px; }}
  .links a {{ margin-right: 16px; }}
  table {{ border-collapse: collapse; width: 100%; background:#fff; font-size: 11px; }}
  th, td {{ border: 1px solid #d6dee6; padding: 4px 7px; text-align: left; white-space: nowrap; }}
  th {{ background: #dce6f1; position: sticky; top: 0; }}
  tbody tr:nth-child(even) {{ background: #f7f9fb; }}
</style></head>
<body>
  <div class="links">
    <a href="/">⌂ All systems</a>
    <a href="http://localhost:1880" target="_blank" rel="noopener">Open Node-RED flow editor ↗</a>
    <a href="/maint">Maintenance</a>
    <a href="/lab">Lab</a>
  </div>

  <h1>ERP Export &mdash; Work Orders</h1>
  <div class="sub">Raw CSV extract from the (simulated) ERP's maintenance module &mdash; one row per order,
    written the moment it's created. Node-RED reads this file and republishes each functional location's
    latest order into the plant's MQTT namespace. Reload this page to see new rows.
    <a href="/exports/workorders.csv">Download raw CSV ↓</a></div>
  {_exports_table(wo_rows, WORK_ORDER_CSV_FIELDS)}

  <h1>LIMS Export &mdash; Lab Samples</h1>
  <div class="sub">Raw CSV extract from the (simulated) LIMS &mdash; one row per registered result,
    written the moment it's submitted. Node-RED reads this file and republishes Proofing results into
    the plant's MQTT namespace. Reload this page to see new rows.
    <a href="/exports/lab_samples.csv">Download raw CSV ↓</a></div>
  {_exports_table(lab_rows, LAB_SAMPLE_CSV_FIELDS)}
</body></html>"""


class WorkOrderLog:
    """Work orders: CRTD (created) -> REL (released / work in progress) -> TECO (technically
    completed), matching real SAP PM's status flow (TECO requires REL first)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.counter = 40000000
        self.orders = []
        self._resume_counter()
        self._seed()

    def _resume_counter(self):
        # Keep order numbers moving forward across restarts, since the CSV export (unlike the
        # in-memory order list) persists on the host — a stray restart shouldn't reissue an id
        # that's already sitting in a previous export row.
        if not os.path.exists(WORK_ORDER_CSV):
            return
        try:
            with open(WORK_ORDER_CSV, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    try:
                        self.counter = max(self.counter, int(row["order_id"]))
                    except (KeyError, TypeError, ValueError):
                        continue
        except OSError:
            pass

    def _next_id(self):
        self.counter += 1
        return str(self.counter)

    def _seed(self):
        # A few starter orders so the list isn't empty on first load, spanning all three statuses.
        now = time.time()
        seed = [
            dict(order_type="PM01", priority="2", functional_location="WL01-OVN-AIR", work_center="MECH",
                 short_text="Filter differential pressure trending high", breakdown=False,
                 reported_by="Line Operator", status="CRTD", created_at=now - 3600),
            dict(order_type="PM02", priority="3", functional_location="WL01-OVN-BDM", work_center="MECH",
                 short_text="Quarterly belt drive lubrication", breakdown=False,
                 reported_by="Planner", status="REL", created_at=now - 7200, started_at=now - 5400),
            dict(order_type="PM01", priority="1", functional_location="WL01-PACK", work_center="ELEC",
                 short_text="Sealer intermittent trip on startup", breakdown=True,
                 reported_by="Line Operator", status="TECO", created_at=now - 86400, started_at=now - 85000,
                 completed_at=now - 82000, work_performed="Replaced worn contactor on sealer heater circuit; tested 10 cycles OK.",
                 work_duration_h=1.5, downtime_duration_h=0.75, downtime_reason="ELEC"),
        ]
        for s in seed:
            s["id"] = self._next_id()
            for field in ("started_at", "completed_at", "work_performed", "work_duration_h", "downtime_duration_h", "downtime_reason"):
                s.setdefault(field, "" if field == "work_performed" else None)
            self.orders.append(s)

    def create(self, fields):
        with self.lock:
            order = {
                "id": self._next_id(),
                "order_type": fields.get("order_type") or WORK_ORDER_META["order_types"][0][0],
                "priority": fields.get("priority") or "3",
                "functional_location": fields.get("functional_location") or WORK_ORDER_META["functional_locations"][0][0],
                "work_center": fields.get("work_center") or WORK_ORDER_META["work_centers"][0][0],
                "short_text": (fields.get("short_text") or "").strip()[:120] or "(no description)",
                "breakdown": bool(fields.get("breakdown")),
                "reported_by": (fields.get("reported_by") or "").strip()[:80],
                "status": "CRTD",
                "created_at": time.time(),
                "started_at": None,
                "completed_at": None,
                "work_performed": "",
                "work_duration_h": None,
                "downtime_duration_h": None,
                "downtime_reason": None,
            }
            self.orders.insert(0, order)
            export_work_order_csv(order)
            return order

    def _find(self, order_id):
        return next((o for o in self.orders if o["id"] == order_id), None)

    def start(self, order_id):
        with self.lock:
            order = self._find(order_id)
            if order is None or order["status"] != "CRTD":
                return None
            order["status"] = "REL"
            order["started_at"] = time.time()
            return order

    def complete(self, order_id, work_performed, work_duration_h, downtime_duration_h, downtime_reason):
        with self.lock:
            order = self._find(order_id)
            if order is None or order["status"] != "REL":
                return None

            def as_hours(v):
                try:
                    return max(0.0, round(float(v), 2))
                except (TypeError, ValueError):
                    return None

            order["status"] = "TECO"
            order["completed_at"] = time.time()
            order["work_performed"] = (work_performed or "").strip()[:1000]
            order["work_duration_h"] = as_hours(work_duration_h)
            order["downtime_duration_h"] = as_hours(downtime_duration_h)
            order["downtime_reason"] = downtime_reason if downtime_reason in _DOWNTIME_REASON_CODES else None
            return order

    def snapshot(self):
        with self.lock:
            return list(self.orders)


WORK_ORDERS = WorkOrderLog()


# Very basic, non-integrated MES (web/mes.html): production orders "received from the ERP"
# (invented here — no ERP exists) against one of 3 waffle recipes, a 6-step checklist mirroring
# the physical line (ISA-88 unit procedures over the ISA-95 area model), and one end-of-run
# report of material actuals + yield. Nothing here reads back from the live process signals —
# an operator's word is the only data source, which is the point of a non-integrated MES.
RECIPES = {
    "WF-CLASSIC": {
        "name": "Classic Belgian",
        "packs_per_batch": 1200,
        "bom": [
            {"material": "Flour", "unit": "kg", "qty": 60},
            {"material": "Sugar", "unit": "kg", "qty": 15},
            {"material": "Yeast", "unit": "kg", "qty": 2},
            {"material": "Water", "unit": "L", "qty": 23},
        ],
    },
    "WF-CHOC": {
        "name": "Chocolate Chip",
        "packs_per_batch": 1200,
        "bom": [
            {"material": "Flour", "unit": "kg", "qty": 55},
            {"material": "Sugar", "unit": "kg", "qty": 15},
            {"material": "Yeast", "unit": "kg", "qty": 2},
            {"material": "Water", "unit": "L", "qty": 21},
            {"material": "Chocolate Chips", "unit": "kg", "qty": 7},
        ],
    },
    "WF-WHOLEGRAIN": {
        "name": "Wholegrain Honey",
        "packs_per_batch": 1200,
        "bom": [
            {"material": "Wholegrain Flour", "unit": "kg", "qty": 50},
            {"material": "Flour", "unit": "kg", "qty": 10},
            {"material": "Sugar", "unit": "kg", "qty": 8},
            {"material": "Yeast", "unit": "kg", "qty": 2},
            {"material": "Water", "unit": "L", "qty": 22},
            {"material": "Honey", "unit": "kg", "qty": 8},
        ],
    },
}

# ISA-88 unit procedures, in order — ids match the AREAS/functional-location vocabulary used
# elsewhere (Oven, not Baking) so the three systems stay easy to cross-reference.
MES_STEPS = [
    {"id": "IngredientHandling", "label": "Ingredient Handling"},
    {"id": "BatterMixing", "label": "Batter Mixing"},
    {"id": "Proofing", "label": "Proofing"},
    {"id": "Oven", "label": "Baking (Oven)"},
    {"id": "Cooling", "label": "Cooling"},
    {"id": "Packaging", "label": "Packaging"},
]


class MESLog:
    """Production orders: scheduled -> in_progress -> completed. Steps are a flat checklist,
    not an enforced sequence — an operator can tick them in whatever order things actually
    happened. Material usage and yield are reported together, once, when the order completes."""

    def __init__(self):
        self.lock = threading.Lock()
        self.orders = []
        self._seed()

    def _materials_for(self, recipe, batches):
        return [
            {"material": b["material"], "unit": b["unit"], "standard": round(b["qty"] * batches, 2), "actual": None}
            for b in RECIPES[recipe]["bom"]
        ]

    def _new_steps(self):
        return {s["id"]: {"done": False, "done_at": None} for s in MES_STEPS}

    def _seed(self):
        now = time.time()

        done_steps = {s["id"]: {"done": True, "done_at": now - 165000} for s in MES_STEPS}
        materials_10039 = self._materials_for("WF-WHOLEGRAIN", 1)
        for m in materials_10039:
            m["actual"] = m["standard"]
        materials_10039[1]["actual"] = round(materials_10039[1]["standard"] * 0.95, 2)  # sugar ran a bit light

        partial_steps = self._new_steps()
        partial_steps["IngredientHandling"] = {"done": True, "done_at": now - 2900}
        partial_steps["BatterMixing"] = {"done": True, "done_at": now - 2000}

        self.orders = [
            {
                "id": "PO-10039", "recipe": "WF-WHOLEGRAIN", "batches": 1, "status": "completed",
                "due_at": now - 180000, "created_at": now - 200000, "started_at": now - 170000, "completed_at": now - 160000,
                "steps": done_steps, "materials": materials_10039,
                "yield_good": 1180, "yield_scrap": 14, "note": "Smooth run; sugar hopper ran a little light mid-batch.",
            },
            {
                "id": "PO-10042", "recipe": "WF-CHOC", "batches": 1, "status": "in_progress",
                "due_at": now + 3600, "created_at": now - 3600, "started_at": now - 3000, "completed_at": None,
                "steps": partial_steps, "materials": self._materials_for("WF-CHOC", 1),
                "yield_good": None, "yield_scrap": None, "note": "",
            },
            {
                "id": "PO-10041", "recipe": "WF-CLASSIC", "batches": 2, "status": "scheduled",
                "due_at": now + 7200, "created_at": now - 1800, "started_at": None, "completed_at": None,
                "steps": self._new_steps(), "materials": self._materials_for("WF-CLASSIC", 2),
                "yield_good": None, "yield_scrap": None, "note": "",
            },
            {
                "id": "PO-10043", "recipe": "WF-CLASSIC", "batches": 3, "status": "scheduled",
                "due_at": now + 14400, "created_at": now - 600, "started_at": None, "completed_at": None,
                "steps": self._new_steps(), "materials": self._materials_for("WF-CLASSIC", 3),
                "yield_good": None, "yield_scrap": None, "note": "",
            },
        ]

    def _find(self, order_id):
        return next((o for o in self.orders if o["id"] == order_id), None)

    def start(self, order_id):
        with self.lock:
            o = self._find(order_id)
            if o is None or o["status"] != "scheduled":
                return None
            o["status"] = "in_progress"
            o["started_at"] = time.time()
            return o

    def set_step(self, order_id, step_id, done):
        with self.lock:
            o = self._find(order_id)
            if o is None or o["status"] != "in_progress" or step_id not in o["steps"]:
                return None
            o["steps"][step_id] = {"done": bool(done), "done_at": time.time() if done else None}
            return o

    def complete(self, order_id, actuals, yield_good, yield_scrap, note):
        with self.lock:
            o = self._find(order_id)
            if o is None or o["status"] != "in_progress":
                return None

            def as_num(v):
                try:
                    return max(0.0, round(float(v), 2))
                except (TypeError, ValueError):
                    return None

            actuals = actuals or {}
            for m in o["materials"]:
                if m["material"] in actuals:
                    v = as_num(actuals[m["material"]])
                    if v is not None:
                        m["actual"] = v
            o["yield_good"] = as_num(yield_good)
            o["yield_scrap"] = as_num(yield_scrap) if yield_scrap not in (None, "") else 0.0
            o["note"] = (note or "").strip()[:500]
            o["status"] = "completed"
            o["completed_at"] = time.time()
            return o

    def snapshot(self):
        with self.lock:
            return list(self.orders)


MES = MESLog()


class LiveState:
    """Latest values and the kWh-per-pack KPI, shared with the web server thread."""

    def __init__(self, units):
        self.units = units
        self.lock = threading.Lock()
        self.values = {}
        self.connected = False
        self.timestamp = None
        self.sec_window = deque(maxlen=SMA_WINDOW)
        self.baseline_samples = deque(maxlen=600)
        self.history = deque(maxlen=HISTORY_LENGTH)
        self.agent_history = deque(maxlen=HISTORY_LENGTH)
        self.agent_topic = None
        self.agent_count = 0

    def record_agent_kpi(self, topic, value):
        with self.lock:
            self.agent_topic = topic
            self.agent_count += 1
            self.agent_history.append([int(time.time() * 1000), value])

    def update(self, values, connected, timestamp, anomaly_idle):
        with self.lock:
            self.values = values
            self.connected = connected
            self.timestamp = timestamp
            if values[PACKS] > 0:
                sec = values[POWER] / (values[PACKS] * 60)
                self.sec_window.append(sec)
                if anomaly_idle:
                    self.baseline_samples.append(sec)
                sma = sum(self.sec_window) / len(self.sec_window)
                self.history.append([int(time.time() * 1000), round(sec, 6), round(sma, 6)])

    def snapshot(self, anomaly, units):
        with self.lock:
            baseline = statistics.median(self.baseline_samples) if self.baseline_samples else None
            latest = self.history[-1] if self.history else [None, None, None]
            agent_latest = self.agent_history[-1] if self.agent_history else [None, None]
            now = time.monotonic()
            return {
                "units": {uid: u.status(now) for uid, u in units.items()},
                "agent_kpi": {
                    "topic": AGENT_KPI_TOPIC,
                    "received_topic": self.agent_topic,
                    "value": agent_latest[1],
                    "age": round(time.time() - agent_latest[0] / 1000, 1) if agent_latest[0] else None,
                    "count": self.agent_count,
                    "history": list(self.agent_history),
                },
                "timestamp": self.timestamp,
                "broker": f"{BROKER_HOST}:{BROKER_PORT}",
                "connected": self.connected,
                "values": {p: {"value": v, "unit": self.units[p]} for p, v in self.values.items()},
                "kpi": {
                    "sec": latest[1],
                    "sma": latest[2],
                    "baseline": round(baseline, 6) if baseline else None,
                    "window": SMA_WINDOW,
                    "history": list(self.history),
                },
                "anomaly": anomaly.status(time.monotonic()),
            }


def flatten(tree, prefix=""):
    for name, node in tree.items():
        path = f"{prefix}/{name}" if prefix else name
        if isinstance(node, (Signal, Derived, Counter)):
            yield path, node
        else:
            yield from flatten(node, path)


def make_handler(live, anomaly, units):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index.html"):
                with open(os.path.join(WEB_DIR, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif self.path in ("/modern", "/modern.html"):
                with open(os.path.join(WEB_DIR, "modern.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif self.path in ("/classic", "/classic.html"):
                with open(os.path.join(WEB_DIR, "classic.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif self.path in ("/lab", "/lab.html"):
                with open(os.path.join(WEB_DIR, "lab.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif self.path in ("/maint", "/maint.html"):
                with open(os.path.join(WEB_DIR, "maint.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif self.path in ("/mes", "/mes.html"):
                with open(os.path.join(WEB_DIR, "mes.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            elif self.path == "/api/state":
                self._json(200, live.snapshot(anomaly, units))
            elif self.path == "/api/samples":
                now = time.time()
                for plan in SAMPLING.values():
                    plan.tick(now)
                self._json(200, {"server_time": now, "stages": {s: p.snapshot() for s, p in SAMPLING.items()}})
            elif self.path == "/api/workorders":
                self._json(200, {"meta": WORK_ORDER_META, "orders": WORK_ORDERS.snapshot()})
            elif self.path == "/api/mes":
                self._json(200, {"recipes": RECIPES, "steps": MES_STEPS, "orders": MES.snapshot()})
            elif self.path == "/exports":
                self._send(200, render_exports_page().encode("utf-8"), "text/html; charset=utf-8")
            elif self.path == "/exports/workorders.csv":
                body = b""
                if os.path.exists(WORK_ORDER_CSV):
                    with open(WORK_ORDER_CSV, "rb") as f:
                        body = f.read()
                self._send(200, body, "text/csv; charset=utf-8")
            elif self.path == "/exports/lab_samples.csv":
                body = b""
                if os.path.exists(LAB_SAMPLE_CSV):
                    with open(LAB_SAMPLE_CSV, "rb") as f:
                        body = f.read()
                self._send(200, body, "text/csv; charset=utf-8")
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self):
            if self.path == "/api/anomaly":
                if units["Oven"].state(time.monotonic()) != "running":
                    self._json(409, {"started": False, "error": "Oven is not running", "anomaly": anomaly.status(time.monotonic())})
                    return
                started = anomaly.trigger()
                self._json(202 if started else 409, {"started": started, "anomaly": anomaly.status(time.monotonic())})
                return
            parts = self.path.strip("/").split("/")
            if len(parts) == 4 and parts[:2] == ["api", "units"] and parts[3] in ("start", "stop"):
                unit = units.get(parts[2])
                if unit is None:
                    self._json(404, {"error": "no such unit"})
                    return
                changed = unit.command(parts[3] == "start")
                self._json(202 if changed else 200, {"changed": changed, "unit": unit.status(time.monotonic())})
                return
            if len(parts) == 4 and parts[:2] == ["api", "samples"]:
                target, action = parts[2], parts[3]
                if action == "adhoc" and target in SAMPLING:
                    self._json(201, {"sample": SAMPLING[target].request_adhoc(time.time())})
                    return
                if action == "result":
                    stage = next((s for s, spec in SAMPLE_TYPES.items() if target.startswith(spec["prefix"] + "-")), None)
                    if stage is None:
                        self._json(404, {"error": "unknown sample"})
                        return
                    body = self._read_json()
                    record = SAMPLING[stage].submit(
                        target, body.get("values") or {}, body.get("visualCheck"),
                        body.get("note"), body.get("technician"), time.time(),
                    )
                    self._json(200 if record else 404, {"sample": record} if record else {"error": "sample not open"})
                    return
            if self.path == "/api/workorders":
                self._json(201, {"order": WORK_ORDERS.create(self._read_json())})
                return
            if len(parts) == 4 and parts[:2] == ["api", "workorders"] and parts[3] in ("start", "complete"):
                order_id = parts[2]
                if parts[3] == "start":
                    order = WORK_ORDERS.start(order_id)
                else:
                    body = self._read_json()
                    order = WORK_ORDERS.complete(
                        order_id, body.get("work_performed"), body.get("work_duration_h"),
                        body.get("downtime_duration_h"), body.get("downtime_reason"),
                    )
                self._json(200 if order else 409, {"order": order} if order else {"error": "no such order, or not in the required status"})
                return
            if len(parts) == 5 and parts[:3] == ["api", "mes", "orders"] and parts[4] == "start":
                order = MES.start(parts[3])
                self._json(200 if order else 409, {"order": order} if order else {"error": "order is not schedulable"})
                return
            if len(parts) == 5 and parts[:3] == ["api", "mes", "orders"] and parts[4] == "complete":
                body = self._read_json()
                order = MES.complete(parts[3], body.get("actuals"), body.get("yield_good"), body.get("yield_scrap"), body.get("note"))
                self._json(200 if order else 409, {"order": order} if order else {"error": "order is not in progress"})
                return
            if len(parts) == 6 and parts[:3] == ["api", "mes", "orders"] and parts[4] == "steps":
                body = self._read_json()
                order = MES.set_step(parts[3], parts[5], body.get("done"))
                self._json(200 if order else 409, {"order": order} if order else {"error": "order is not in progress, or unknown step"})
                return
            self._json(404, {"error": "not found"})

        def _read_json(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(length) if length else b""
            try:
                return json.loads(raw) if raw else {}
            except ValueError:
                return {}

        def _json(self, code, body):
            self._send(code, json.dumps(body, ensure_ascii=False).encode(), "application/json; charset=utf-8")

        def _send(self, code, body, content_type):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    return Handler


def parse_number(payload):
    """Accept a bare number (b'1.41') or a JSON object with a "value" field."""
    try:
        data = json.loads(payload.decode("utf-8", "replace"))
    except ValueError:
        return None
    if isinstance(data, dict):
        data = data.get("value")
    if isinstance(data, bool) or not isinstance(data, (int, float)):
        return None
    return float(data)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        log.error("Connection failed: %s", reason_code)
    else:
        log.info("Connected to %s:%s", BROKER_HOST, BROKER_PORT)
        # Subscribing here restores the subscription after every reconnect.
        client.subscribe(AGENT_KPI_SUBSCRIPTION)
        log.info("Subscribed to %s for agent KPI %s", AGENT_KPI_SUBSCRIPTION, AGENT_KPI_TOPIC)


def on_disconnect(client, userdata, flags, reason_code, properties):
    log.warning("Disconnected (%s), paho will reconnect automatically", reason_code)


def main():
    nodes = list(flatten(SIGNALS))
    signals = [(p, n) for p, n in nodes if isinstance(n, Signal)]
    derived = [(p, n) for p, n in nodes if isinstance(n, (Derived, Counter))]
    decimals = {p: n.decimals for p, n in signals}
    log.info("Simulating %d topics every %ss (%d gated to %d equipment units)", len(nodes), PUBLISH_INTERVAL, len(GATES), len(UNITS))

    anomaly = EnergyAnomaly(ANOMALY_RAMP, ANOMALY_HOLD)
    live = LiveState({p: n.unit for p, n in nodes})

    server = ThreadingHTTPServer(("0.0.0.0", WEB_PORT), make_handler(live, anomaly, UNITS))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info("HMI available on port %d", WEB_PORT)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    def on_message(client, userdata, message):
        if message.topic.strip() != AGENT_KPI_TOPIC:
            return
        value = parse_number(message.payload)
        if value is None:
            log.warning("Ignoring non-numeric agent KPI payload: %r", message.payload[:80])
            return
        live.record_agent_kpi(message.topic, value)

    client.on_message = on_message

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    # connect_async + loop_start keeps retrying in the background while the HMI stays up.
    client.connect_async(BROKER_HOST, BROKER_PORT, keepalive=60)
    client.loop_start()

    start = time.monotonic()
    while running:
        now = time.monotonic()
        t = now - start
        offsets = anomaly.offsets(now)
        values = {path: sig.sample(t, offsets.get(path, 0.0)) for path, sig in signals}
        # Gate before derived nodes run, so anything computed from a gated input
        # (mains current, total gas, cartons) settles towards zero/standby with it.
        for path, gate in GATES.items():
            values[path] = round(gate.apply(values[path], PUBLISH_INTERVAL, now), decimals[path])
        for path, node in derived:
            values[path] = node.compute(values)

        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        connected = client.is_connected()
        live.update(values, connected, timestamp, anomaly.status(now)["state"] == "idle")

        if connected:
            for path, value in values.items():
                payload = json.dumps({"value": value, "unit": live.units[path], "timestamp": timestamp}, ensure_ascii=False)
                client.publish(f"{TOPIC_PREFIX}/{path}", payload, qos=QOS)
                client.publish(f"{RAW_TOPIC_PREFIX}/{path.replace('/', '_')}", payload, qos=QOS)
        time.sleep(PUBLISH_INTERVAL)

    log.info("Shutting down")
    server.shutdown()
    client.loop_stop()
    client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
