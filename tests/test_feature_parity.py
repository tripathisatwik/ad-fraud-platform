import pandas as pd
from ml.features import build_features, FEATURE_COLUMNS


def main():
    # Includes tied timestamps, exact 60s/300s/3600s boundaries,
    # repeated IP/app combinations, and a second IP.
    events = pd.DataFrame([
        {"event_id": "e1", "click_time": "2026-01-01 00:00:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e2", "click_time": "2026-01-01 00:00:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e3", "click_time": "2026-01-01 00:01:00", "ip": 1, "app": 11, "device": 101, "os": 2, "channel": 6},
        {"event_id": "e4", "click_time": "2026-01-01 00:05:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e5", "click_time": "2026-01-01 01:00:00", "ip": 1, "app": 10, "device": 100, "os": 1, "channel": 5},
        {"event_id": "e6", "click_time": "2026-01-01 01:00:00", "ip": 2, "app": 10, "device": 102, "os": 1, "channel": 5},
    ])
    events["click_time"] = pd.to_datetime(events["click_time"])
    result = build_features(events).set_index("event_id")

    expected = {
        # event: (IP rolling 1m, 5m, 1h; IP+app rolling 1m, 5m, 1h)
        "e1": (0, 0, 0, 0, 0, 0),
        "e2": (1, 1, 1, 1, 1, 1),
        "e3": (0, 2, 2, 0, 0, 0),
        "e4": (0, 1, 3, 0, 0, 2),
        "e5": (0, 0, 2, 0, 0, 1),
        "e6": (0, 0, 0, 0, 0, 0),
    }
    cols = [
        "ip_clicks_last_1min", "ip_clicks_last_5min", "ip_clicks_last_1h",
        "ip_app_clicks_last_1min", "ip_app_clicks_last_5min", "ip_app_clicks_last_1h",
    ]
    for event_id, values in expected.items():
        actual = tuple(int(result.loc[event_id, col]) for col in cols)
        assert actual == values, f"{event_id}: expected {values}, got {actual}"

    assert len(FEATURE_COLUMNS) == 23
    assert result[FEATURE_COLUMNS].notna().all().all(), "Feature columns contain nulls"
    assert result.loc["e1", "seconds_since_ip_click"] == -1
    assert result.loc["e2", "seconds_since_ip_click"] == 0
    assert result.loc["e3", "seconds_since_ip_click"] == 60
    assert result.loc["e1", "distinct_apps_per_ip_before"] == 0
    assert result.loc["e3", "distinct_apps_per_ip_before"] == 1
    print("PASS: rolling boundaries, tied timestamps, counters, distinct counts, and all 23 features.")


if __name__ == "__main__":
    main()
