import os
import csv
from datetime import datetime
from adapters.aeso_adapter import load_aeso_generation, build_aeso_wind_solar
from weather.weather_fetcher import geocode_city, fetch_weather

def build_aeso_master(input_dir, output_dir, city, timezone):
    rows = load_aeso_generation(input_dir)
    wind_rows, solar_rows = build_aeso_wind_solar(rows)

    # Align timestamps
    wind_map = {r["timestamp"]: r["Wind"] for r in wind_rows}
    solar_map = {r["timestamp"]: r["Solar"] for r in solar_rows}
    timestamps = sorted(set(wind_map.keys()) & set(solar_map.keys()))
    aeso_rows = [{"timestamp": ts, "Wind": wind_map[ts], "Solar": solar_map[ts]} for ts in timestamps]

    start_date = aeso_rows[0]["timestamp"].split()[0]
    end_date = aeso_rows[-1]["timestamp"].split()[0]

    lat, lon = geocode_city(city)
    weather_rows = fetch_weather(lat, lon, start_date, end_date, timezone=timezone)
    weather_map = {r["timestamp"]: r for r in weather_rows}

    merged = []
    for r in aeso_rows:
        ts = r["timestamp"]
        if ts in weather_map:
            merged.append({**r, **weather_map[ts]})

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "aeso_master.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(merged[0].keys()))
        writer.writeheader()
        writer.writerows(merged)

    return out_path
