import os
from typing import List

import pandas as pd

from adapters.ieso_adapter import parse_ieso_xml
from weather.weather_fetcher import fetch_weather_for_city


def parse_csv(file_obj) -> pd.DataFrame:
    df = pd.read_csv(file_obj)
    if "timestamp" not in df.columns:
        raise ValueError("CSV must contain a 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def parse_xml(file_obj) -> pd.DataFrame:
    try:
        df = parse_ieso_xml(file_obj)
    except Exception:
        raise ValueError("Invalid XML format. Only IESO-style XML is supported.")
    if "timestamp" not in df.columns:
        raise ValueError("XML must contain a 'timestamp' field.")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def validate_common(df: pd.DataFrame):
    if df.empty:
        raise ValueError("Uploaded file contains no usable data.")

    min_year = int(df["timestamp"].dt.year.min())
    max_year = int(df["timestamp"].dt.year.max())
    if max_year - min_year > 10:
        raise ValueError("Uploaded data cannot span more than 10 years.")

    if "Wind" not in df.columns and "Solar" not in df.columns:
        raise ValueError("File must contain either 'Wind' or 'Solar' column.")

    diffs = df["timestamp"].sort_values().diff().dropna().dt.total_seconds()
    if len(diffs) > 0:
        median_step = diffs.median()
        if abs(median_step - 3600) > 1:
            raise ValueError("Timestamps must be hourly (1-hour intervals).")


def build_user_master(
    upload_mode: str,
    file_format: str,
    files: List,
    output_dir: str,
    city: str,
    timezone: str,
) -> str:
    os.makedirs(output_dir, exist_ok=True)

    if upload_mode == "single":
        file = files[0]
        df = parse_csv(file.file) if file_format == "csv" else parse_xml(file.file)
        validate_common(df)

    elif upload_mode == "multi":
        dfs = []
        for file in files:
            df_part = parse_csv(file.file) if file_format == "csv" else parse_xml(file.file)
            dfs.append(df_part)

        df = pd.concat(dfs).sort_values("timestamp")
        validate_common(df)

    else:
        raise ValueError("upload_mode must be 'single' or 'multi'.")

    weather_df = fetch_weather_for_city(city, timezone)
    merged = pd.merge(df, weather_df, on="timestamp", how="left")

    master_path = os.path.join(output_dir, "upload_master.csv")
    merged.to_csv(master_path, index=False)

    return master_path
