import pandas as pd
from collections import deque

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Match Spark's deterministic ordering when event_id is available.
    sort_cols = ["click_time"]
    if "event_id" in df.columns:
        sort_cols.append("event_id")
    df = df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    df["hour"] = df["click_time"].dt.hour
    df["day_of_week"] = df["click_time"].dt.dayofweek

    df["ip_clicks_before"] = df.groupby("ip").cumcount()
    df["ip_app_clicks_before"] = df.groupby(["ip", "app"]).cumcount()
    df["device_clicks_before"] = df.groupby("device").cumcount()
    df["app_clicks_before"] = df.groupby("app").cumcount()
    df["os_clicks_before"] = df.groupby("os").cumcount()
    df["channel_clicks_before"] = df.groupby("channel").cumcount()
    df["app_channel_clicks_before"] = df.groupby(["app", "channel"]).cumcount()
    df["app_device_clicks_before"] = df.groupby(["app", "device"]).cumcount()

    previous_ip_click = df.groupby("ip")["click_time"].shift(1)
    df["seconds_since_ip_click"] = (
        df["click_time"] - previous_ip_click
    ).dt.total_seconds().fillna(-1)

    first_ip_click = df.groupby("ip")["click_time"].transform("first")
    df["seconds_since_ip_first_seen"] = (
        df["click_time"] - first_ip_click
    ).dt.total_seconds()

    df["distinct_apps_per_ip_before"] = _distinct_count_before(df, "ip", "app")
    df["distinct_devices_per_ip_before"] = _distinct_count_before(df, "ip", "device")
    df["distinct_channels_per_ip_before"] = _distinct_count_before(df, "ip", "channel")

    df = _add_rolling_time_count(df, ["ip"], 60, "ip_clicks_last_1min")
    df = _add_rolling_time_count(df, ["ip"], 300, "ip_clicks_last_5min")
    df = _add_rolling_time_count(df, ["ip"], 600, "ip_clicks_last_10min")
    df = _add_rolling_time_count(df, ["ip"], 3600, "ip_clicks_last_1h")

    df = _add_rolling_time_count(df, ["ip", "app"], 60, "ip_app_clicks_last_1min")
    df = _add_rolling_time_count(df, ["ip", "app"], 300, "ip_app_clicks_last_5min")
    df = _add_rolling_time_count(df, ["ip", "app"], 600, "ip_app_clicks_last_10min")
    df = _add_rolling_time_count(df, ["ip", "app"], 3600, "ip_app_clicks_last_1h")

    return df


def _add_rolling_time_count(df, group_cols, window_seconds, out_col):
    """Count prior rows with cutoff < prior_timestamp <= current_timestamp.

    This mirrors Spark's stateful algorithm: events at the exact lower
    boundary are excluded; earlier events with the same timestamp are
    included in deterministic row order.
    """
    counts = [0] * len(df)
    for positions in df.groupby(group_cols, sort=False).indices.values():
        history = deque()
        for pos in positions:
            now = df.at[pos, "click_time"]
            cutoff = now - pd.Timedelta(seconds=window_seconds)
            while history and history[0] <= cutoff:
                history.popleft()
            counts[pos] = len(history)
            history.append(now)
    df[out_col] = counts
    return df


def _distinct_count_before(df, group_col, target_col):
    is_first_occurrence = ~df.duplicated(subset=[group_col, target_col])
    cum_including_self = is_first_occurrence.groupby(df[group_col]).cumsum()
    before = cum_including_self - is_first_occurrence.astype(int)
    return before


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
