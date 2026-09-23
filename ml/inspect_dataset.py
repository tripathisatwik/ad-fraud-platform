from pathlib import Path

import pandas as pd

DATASET_PATH = Path("data/raw/train_sample.xlsx") 

def main():
    df = pd.read_excel(DATASET_PATH)

    print("\n=== DATASET SHAPE ===")
    print(f"Rows: {df.shape[0]:,}")
    print(f"Columns: {df.shape[1]}")

    print("\n=== COLUMNS ===")
    print(df.columns.tolist())

    print("\n=== DATA TYPES ===")
    print(df.dtypes)

    print("\n=== MISSING VALUES ===")
    print(df.isnull().sum())

    print("\n=== TIME RANGE ===")
    print(f"Start: {df['click_time'].min()}")
    print(f"End:   {df['click_time'].max()}")

    print("\n=== CLICKS BY HOUR ===")
    print(df["click_time"].dt.hour.value_counts().sort_index())

    print("\n=== CLICKS BY DATE ===")
    print(df["click_time"].dt.date.value_counts().sort_index())

    print("\n=== TARGET DISTRIBUTION ===")
    print(df["is_attributed"].value_counts())

    print("\n=== TARGET PERCENTAGES ===")
    print(df["is_attributed"].value_counts(normalize=True) * 100)

    print("\n=== UNIQUE VALUES ===")
    for column in ["ip", "app", "device", "os", "channel"]:
        print(f"{column}: {df[column].nunique():,}")

    print("\n=== TOP 10 IPs BY CLICK COUNT ===")
    print(df["ip"].value_counts().head(10))

    print("\n=== POSITIVE EVENTS: TOP IPS ===")
    positive_df = df[df["is_attributed"] == 1]

    print(positive_df["ip"].value_counts().head(10))

    print("\n=== POSITIVE EVENTS: UNIQUE VALUES ===")
    for column in ["ip", "app", "device", "os", "channel"]:
        print(f"{column}: {positive_df[column].nunique():,}")


    print("\n=== POSITIVE EVENTS: TOP APP + CHANNEL COMBINATIONS ===")
    print(
        positive_df
        .groupby(["app", "channel"])
        .size()
        .sort_values(ascending=False)
        .head(10)
    )

    print("\n=== POSITIVE EVENTS: TOP APP + DEVICE COMBINATIONS ===")
    print(
        positive_df
        .groupby(["app", "device"])
        .size()
        .sort_values(ascending=False)
        .head(10)
    )

    print("\n=== FIRST 5 ROWS ===")
    print(df.head())


if __name__ == "__main__":
    main()