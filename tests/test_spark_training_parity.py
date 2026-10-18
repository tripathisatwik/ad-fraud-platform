"""Compare the actual process_scope implementation in kafka_stream.py with ml.features.
This test executes the real Spark module's pure Python state-processing functions
without creating a SparkSession or connecting to Kafka. It models one complete
micro-batch, so it does not validate late events arriving in later micro-batches.
"""
from __future__ import annotations

import ast
import json
import importlib.util
from pathlib import Path
from typing import Iterator

import pandas as pd

ROOT = Path(__file__).resolve().parent
FEATURE_SOURCE = ROOT / "ml" / "features.py"
if not FEATURE_SOURCE.exists() and (ROOT / "features_parity_fixed.py").exists():
    FEATURE_SOURCE = ROOT / "features_parity_fixed.py"
if not FEATURE_SOURCE.exists() and (ROOT / "features.py").exists():
    FEATURE_SOURCE = ROOT / "features.py"
if not FEATURE_SOURCE.exists():
    raise FileNotFoundError(f"Could not find training feature module at {FEATURE_SOURCE}")
_feature_spec = importlib.util.spec_from_file_location("project_features", FEATURE_SOURCE)
_feature_module = importlib.util.module_from_spec(_feature_spec)
_feature_spec.loader.exec_module(_feature_module)
build_features = _feature_module.build_features
TRAIN_FEATURE_COLUMNS = _feature_module.FEATURE_COLUMNS

SPARK_SOURCE = ROOT / "spark" / "src" / "kafka_stream.py"
if not SPARK_SOURCE.exists() and (ROOT / "kafka_stream.py").exists():
    SPARK_SOURCE = ROOT / "kafka_stream.py"


class FakeGroupState:
    def __init__(self):
        self.exists = False
        self.get = None
        self.updated = None

    def update(self, value):
        self.exists = True
        self.get = value
        self.updated = value


def load_actual_spark_functions(source_path: Path):
    """AST-load constants and pure processing functions, avoiding Spark startup."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    wanted_assignments = {
        "FEATURE_COLUMNS", "ROLLING_WINDOWS", "MAX_WINDOW_SECONDS",
        "SCOPE_IP", "SCOPE_IP_APP", "SCOPE_DEVICE", "SCOPE_APP", "SCOPE_OS",
        "SCOPE_CHANNEL", "SCOPE_APP_CHANNEL", "SCOPE_APP_DEVICE", "OUTPUT_COLUMNS",
    }
    wanted_functions = {
        "_empty_state", "_decode_state", "_encode_state", "_rolling_count",
        "_prune_click_times", "_empty_feature_record", "process_scope",
    }
    selected = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {target.id for target in node.targets if isinstance(target, ast.Name)}
            if names & wanted_assignments:
                selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            selected.append(node)

    namespace = {"pd": pd, "json": json, "Iterator": Iterator, "GroupState": object}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(source_path), "exec"), namespace)
    missing = wanted_assignments - namespace.keys()
    if missing:
        raise AssertionError(f"Could not load Spark constants: {sorted(missing)}")
    return namespace


def scope_key(row, scope_type):
    if scope_type == "IP":
        return str(row.ip)
    if scope_type == "IP_APP":
        return f"{row.ip}|{row.app}"
    if scope_type == "DEVICE":
        return str(row.device)
    if scope_type == "APP":
        return str(row.app)
    if scope_type == "OS":
        return str(row.os)
    if scope_type == "CHANNEL":
        return str(row.channel)
    if scope_type == "APP_CHANNEL":
        return f"{row.app}|{row.channel}"
    if scope_type == "APP_DEVICE":
        return f"{row.app}|{row.device}"
    raise AssertionError(f"Unknown scope: {scope_type}")


def main():
    if not SPARK_SOURCE.exists():
        raise FileNotFoundError(f"Expected Spark source at {SPARK_SOURCE}")

    spark = load_actual_spark_functions(SPARK_SOURCE)
    events = pd.DataFrame([
        {"event_id": "e1", "timestamp": "2026-01-01 00:00:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e2", "timestamp": "2026-01-01 00:00:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e3", "timestamp": "2026-01-01 00:01:00", "ip": 1, "app": 11, "device": 101, "os": 2, "channel": 6},
        {"event_id": "e4", "timestamp": "2026-01-01 00:05:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e5", "timestamp": "2026-01-01 01:00:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e6", "timestamp": "2026-01-01 01:00:00", "ip": 2, "app": 10, "device": 102, "os": 1, "channel": 5},
    ])
    events["event_time"] = pd.to_datetime(events["timestamp"], utc=True)
    training_events = events.rename(columns={"timestamp": "click_time"}).copy()
    training_events["click_time"] = pd.to_datetime(training_events["click_time"])
    expected = build_features(training_events).set_index("event_id")

    scope_types = [
        "IP", "IP_APP", "DEVICE", "APP", "OS", "CHANNEL", "APP_CHANNEL", "APP_DEVICE",
    ]
    partials = []
    for scope_type in scope_types:
        scope_func = spark["process_scope"]
        # Spark invokes the function separately for each scope key.
        key_by_event = {row.event_id: scope_key(row, scope_type) for row in events.itertuples()}
        for key in sorted(set(key_by_event.values())):
            subset_ids = [event_id for event_id, event_key in key_by_event.items() if event_key == key]
            subset = events[events["event_id"].isin(subset_ids)].copy()
            fake_state = FakeGroupState()
            generated = list(scope_func((scope_type, key), [subset], fake_state))
            if generated:
                partials.append(generated[0])

    if not partials:
        raise AssertionError("Spark process_scope generated no feature rows")
    spark_rows = pd.concat(partials, ignore_index=True)
    aggregation = {feature: "max" for feature in spark["FEATURE_COLUMNS"]}
    actual = (
        spark_rows.groupby("event_id", as_index=False).agg(aggregation).set_index("event_id")
    )

    if spark["FEATURE_COLUMNS"] != TRAIN_FEATURE_COLUMNS:
        raise AssertionError(
            "Feature column order/name mismatch:\n"
            f"Spark: {spark['FEATURE_COLUMNS']}\nTraining: {TRAIN_FEATURE_COLUMNS}"
        )

    mismatches = []
    for event_id in expected.index:
        for feature in TRAIN_FEATURE_COLUMNS:
            exp = expected.loc[event_id, feature]
            got = actual.loc[event_id, feature]
            if pd.isna(exp) and pd.isna(got):
                continue
            if pd.isna(exp) != pd.isna(got):
                mismatches.append((event_id, feature, exp, got))
            elif isinstance(exp, (int, float)) and isinstance(got, (int, float)):
                if abs(float(exp) - float(got)) > 1e-9:
                    mismatches.append((event_id, feature, exp, got))
            elif exp != got:
                mismatches.append((event_id, feature, exp, got))

    if mismatches:
        lines = ["FAIL: Spark/training feature mismatches (event, feature, expected, actual):"]
        lines.extend(f"  {item}" for item in mismatches[:30])
        raise AssertionError("\n".join(lines))

    if actual[TRAIN_FEATURE_COLUMNS].isna().any().any():
        nulls = actual[TRAIN_FEATURE_COLUMNS].isna().sum()
        raise AssertionError(f"Null feature values in Spark-aggregated rows:\n{nulls[nulls > 0]}")

    print("PASS: actual Spark process_scope matches training features for all 6 events and 23 features.")
    print("SCOPE: one complete micro-batch; late arrivals across separate micro-batches remain untested.")


if __name__ == "__main__":
    main()
