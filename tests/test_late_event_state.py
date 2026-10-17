"""Regression tests for late-event handling in spark/src/kafka_stream.py.
Loads the real pure-Python functions by AST without importing the Spark module
(which starts a streaming query), then simulates separate micro-batches.
"""
import ast
import json
from pathlib import Path
from typing import Iterator

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "spark" / "src" / "kafka_stream.py"

FEATURE_COLUMNS = [
    "hour", "day_of_week", "ip_clicks_before", "ip_app_clicks_before",
    "device_clicks_before", "app_clicks_before", "os_clicks_before",
    "channel_clicks_before", "app_channel_clicks_before", "app_device_clicks_before",
    "seconds_since_ip_click", "seconds_since_ip_first_seen",
    "distinct_apps_per_ip_before", "distinct_devices_per_ip_before",
    "distinct_channels_per_ip_before", "ip_clicks_last_1min",
    "ip_clicks_last_5min", "ip_clicks_last_10min", "ip_clicks_last_1h",
    "ip_app_clicks_last_1min", "ip_app_clicks_last_5min",
    "ip_app_clicks_last_10min", "ip_app_clicks_last_1h",
]
ROLLING_WINDOWS = {"1min": 60, "5min": 300, "10min": 600, "1h": 3600}
MAX_WINDOW_SECONDS = 3600
SCOPE_IP = "IP"
SCOPE_IP_APP = "IP_APP"
SCOPE_DEVICE = "DEVICE"
SCOPE_APP = "APP"
SCOPE_OS = "OS"
SCOPE_CHANNEL = "CHANNEL"
SCOPE_APP_CHANNEL = "APP_CHANNEL"
SCOPE_APP_DEVICE = "APP_DEVICE"
OUTPUT_COLUMNS = [
    "event_id", "timestamp", "event_time", "ip", "app", "device", "os",
    "channel", "scope_type", "scope_key", "is_late_event", *FEATURE_COLUMNS,
]


class FakeGroupState:
    def __init__(self):
        self._value = None

    @property
    def exists(self):
        return self._value is not None

    @property
    def get(self):
        if self._value is None:
            raise ValueError("State does not exist")
        return self._value

    def update(self, value):
        self._value = value


def load_functions():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    wanted = {
        "_empty_state", "_decode_state", "_encode_state", "_rolling_count",
        "_prune_click_times", "_empty_feature_record", "process_scope",
    }
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in wanted]
    found = {n.name for n in nodes}
    assert found == wanted, f"Missing functions in current Spark source: {sorted(wanted - found)}"
    ns = {
        "json": json, "pd": pd, "Iterator": Iterator, "GroupState": object,
        "FEATURE_COLUMNS": FEATURE_COLUMNS, "ROLLING_WINDOWS": ROLLING_WINDOWS,
        "MAX_WINDOW_SECONDS": MAX_WINDOW_SECONDS, "SCOPE_IP": SCOPE_IP,
        "SCOPE_IP_APP": SCOPE_IP_APP, "SCOPE_DEVICE": SCOPE_DEVICE,
        "SCOPE_APP": SCOPE_APP, "SCOPE_OS": SCOPE_OS,
        "SCOPE_CHANNEL": SCOPE_CHANNEL, "SCOPE_APP_CHANNEL": SCOPE_APP_CHANNEL,
        "SCOPE_APP_DEVICE": SCOPE_APP_DEVICE, "OUTPUT_COLUMNS": OUTPUT_COLUMNS,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), ns)
    return ns


def event(event_id, ts, *, ip=7, app=10, device=100, os_value=1, channel=5):
    return {
        "event_id": event_id, "timestamp": ts,
        "event_time": pd.Timestamp(ts), "ip": ip, "app": app,
        "device": device, "os": os_value, "channel": channel,
    }


def process_one(ns, scope_type, scope_key, state, e):
    pdf = pd.DataFrame([e])
    result = list(ns["process_scope"]((scope_type, str(scope_key)), iter([pdf]), state))[0]
    return result.iloc[0].to_dict()


def test_ip_state_stays_chronological(ns):
    state = FakeGroupState()
    first = process_one(ns, SCOPE_IP, 7, state, event("e1", "2026-10-17 10:05:00"))
    late = process_one(ns, SCOPE_IP, 7, state, event("e2", "2026-10-17 10:03:00"))
    later = process_one(ns, SCOPE_IP, 7, state, event("e3", "2026-10-17 10:06:00"))

    assert first["is_late_event"] is False
    assert late["is_late_event"] is True
    assert later["is_late_event"] is False
    assert later["seconds_since_ip_click"] == 60.0, later["seconds_since_ip_click"]
    persisted = ns["_decode_state"](state)
    assert pd.Timestamp(persisted["last_click"]) == pd.Timestamp("2026-10-17 10:06:00", tz="UTC")
    assert pd.Timestamp(persisted["first_seen"]) == pd.Timestamp("2026-10-17 10:05:00", tz="UTC")


def test_late_event_does_not_update_other_scope(ns):
    ip_state = FakeGroupState()
    app_state = FakeGroupState()

    # IP 7 has an event at 10:05. App 20 has an unrelated event at 10:00.
    process_one(ns, SCOPE_IP, 7, ip_state, event("e1", "2026-10-17 10:05:00", app=10))
    process_one(ns, SCOPE_APP, 20, app_state, event("e0", "2026-10-17 10:00:00", ip=99, app=20))

    late_event = event("e2", "2026-10-17 10:03:00", app=20)
    ip_result = process_one(ns, SCOPE_IP, 7, ip_state, late_event)
    app_result = process_one(ns, SCOPE_APP, 20, app_state, late_event)

    assert ip_result["is_late_event"] is True
    # This currently demonstrates the bug: the APP scope does not know that
    # the IP scope rejected the same event as late.
    assert app_result["is_late_event"] is True, (
        "Cross-scope inconsistency reproduced: event is late for IP but not "
        "for APP; APP scope would update its state. This test should fail "
        "until lateness is decided once per event before scope expansion."
    )


def main():
    ns = load_functions()
    test_ip_state_stays_chronological(ns)
    print("PASS: late event does not move IP state backwards")
    test_late_event_does_not_update_other_scope(ns)
    print("PASS: late-event decision is consistent across scopes")
    print("PASS: late-event regression tests")


if __name__ == "__main__":
    main()