import pandas as pd

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df = df.sort_values("click_time").reset_index(drop=True)
    
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
    df["seconds_since_ip_click"] = (df["click_time"] - previous_ip_click).dt.total_seconds().fillna(-1)
    
    first_ip_click = df.groupby("ip")["click_time"].transform("first")
    df["seconds_since_ip_first_seen"] = (df["click_time"] - first_ip_click).dt.total_seconds()
    
    df["distinct_apps_per_ip_before"] = _distinct_count_before(df, "ip", "app")
    df["distinct_devices_per_ip_before"] = _distinct_count_before(df, "ip", "device")
    df["distinct_channels_per_ip_before"] = _distinct_count_before(df, "ip", "channel")
    
    df = _add_rolling_time_count(df, ["ip"], "1min", "ip_clicks_last_1min")
    df = _add_rolling_time_count(df, ["ip"], "5min", "ip_clicks_last_5min")
    df = _add_rolling_time_count(df, ["ip"], "10min", "ip_clicks_last_10min")
    df = _add_rolling_time_count(df, ["ip"], "1h", "ip_clicks_last_1h")
    
    df = _add_rolling_time_count(df, ["ip", "app"], "1min", "ip_app_clicks_last_1min")
    df = _add_rolling_time_count(df, ["ip", "app"], "5min", "ip_app_clicks_last_5min")
    df = _add_rolling_time_count(df, ["ip", "app"], "10min", "ip_app_clicks_last_10min")
    df = _add_rolling_time_count(df, ["ip", "app"], "1h", "ip_app_clicks_last_1h")
    
    return df

def _add_rolling_time_count(df, group_cols, window, out_col):
    counts = (
        df.set_index("click_time")
        .groupby(group_cols)[group_cols[0]]
        .rolling(window, closed="left")
        .count()
    )
    counts = counts.reset_index(drop=True)
    counts.index = df.index
    df[out_col] = counts.fillna(0)
    return df

def _distinct_count_before(df, group_col, target_col):
    is_first_occurrence = ~df.duplicated(subset=[group_col, target_col])
    cum_including_self = is_first_occurrence.groupby(df[group_col]).cumsum()
    before = cum_including_self - is_first_occurrence.astype(int)
    return before

FEATURE_COLUMNS = [
    "hour",
    "day_of_week",
    "ip_clicks_before",
    "ip_app_clicks_before",
    "device_clicks_before",
    "app_clicks_before",
    "os_clicks_before",
    "channel_clicks_before",
    "app_channel_clicks_before",
    "app_device_clicks_before",
    "seconds_since_ip_click",
    "seconds_since_ip_first_seen",
    "distinct_apps_per_ip_before",
    "distinct_devices_per_ip_before",
    "distinct_channels_per_ip_before",
    "ip_clicks_last_1min",
    "ip_clicks_last_5min",
    "ip_clicks_last_10min",
    "ip_clicks_last_1h",
    "ip_app_clicks_last_1min",
    "ip_app_clicks_last_5min",
    "ip_app_clicks_last_10min",
    "ip_app_clicks_last_1h",
]