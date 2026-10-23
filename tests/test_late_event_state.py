"""Regression tests for centralized per-IP late-event validation."""
import ast
from pathlib import Path
from typing import Iterator

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_SOURCE = ROOT / "spark" / "src" / "event_validator.py"
STREAM_SOURCE = ROOT / "spark" / "src" / "kafka_stream.py"


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


def load_validator():
    tree = ast.parse(
        VALIDATOR_SOURCE.read_text(encoding="utf-8"),
        filename=str(VALIDATOR_SOURCE),
    )
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "validate_ip_events"
    ]
    assert len(nodes) == 1, "validate_ip_events() not found"

    # Supply only the names needed by the pure-Python function.
    ns = {
        "pd": pd,
        "Iterator": Iterator,
        "GroupState": object,
        "OUTPUT_SCHEMA": type(
            "Schema",
            (),
            {
                "fields": [
                    type("Field", (), {"name": name})()
                    for name in [
                        "event_id", "timestamp", "ip", "app",
                        "device", "os", "channel", "is_late_event",
                    ]
                ]
            },
        )(),
    }
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]),
                str(VALIDATOR_SOURCE), "exec"),
        ns,
    )
    return ns["validate_ip_events"]


def validator_batch(validate, state, events):
    pdf = pd.DataFrame(events)
    result = list(
        validate((7,), iter([pdf]), state)
    )
    return pd.concat(result, ignore_index=True) if result else pd.DataFrame()


def event(event_id, timestamp, offset):
    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "ip": 7,
        "app": 10,
        "device": 100,
        "os": 1,
        "channel": 5,
        "kafka_offset": offset,
        "event_time": pd.to_datetime(timestamp, utc=True),
    }


def test_validator_rejects_late_event_without_advancing_state():
    validate = load_validator()
    state = FakeGroupState()

    first = validator_batch(
        validate, state,
        [event("e1", "2026-10-17 10:05:00", 1)],
    )
    late = validator_batch(
        validate, state,
        [event("e2", "2026-10-17 10:03:00", 2)],
    )
    later = validator_batch(
        validate, state,
        [event("e3", "2026-10-17 10:06:00", 3)],
    )

    assert first.iloc[0]["is_late_event"] == False
    assert late.iloc[0]["is_late_event"] == True
    assert later.iloc[0]["is_late_event"] == False
    assert state.get[0] == pd.Timestamp(
        "2026-10-17 10:06:00", tz="UTC"
    ).value


def test_validator_uses_arrival_order_within_batch():
    validate = load_validator()
    state = FakeGroupState()

    result = validator_batch(
        validate,
        state,
        [
            event("e1", "2026-10-17 10:05:00", 1),
            event("e2", "2026-10-17 10:03:00", 2),
            event("e3", "2026-10-17 10:06:00", 3),
        ],
    )

    by_id = result.set_index("event_id")["is_late_event"].to_dict()
    assert by_id == {"e1": False, "e2": True, "e3": False}, by_id


def test_feature_stream_filters_before_scope_expansion():
    source = STREAM_SOURCE.read_text(encoding="utf-8")

    assert 'KAFKA_TOPIC = "validated-ad-events"' in source
    assert "accepted_events = events.filter(" in source
    assert 'col("is_late_event") == False' in source

    make_scope_start = source.index("def make_scope(")
    make_scope_end = source.index("\n\nscoped_events", make_scope_start)
    make_scope_source = source[make_scope_start:make_scope_end]

    assert "accepted_events" in make_scope_source
    assert "return (\n        events" not in make_scope_source


def main():
    test_validator_rejects_late_event_without_advancing_state()
    print("PASS: late events do not advance per-IP validator state")

    test_validator_uses_arrival_order_within_batch()
    print("PASS: same-batch events use Kafka arrival order")

    test_feature_stream_filters_before_scope_expansion()
    print("PASS: feature stream filters late events before all scopes")

    print("PASS: centralized late-event regression tests")


if __name__ == "__main__":
    main()