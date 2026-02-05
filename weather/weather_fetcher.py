import requests
from datetime import datetime

HOURLY_VARS = [
    "temperature_2m","relativehumidity_2m","dewpoint_2m","apparent_temperature",
    "precipitation","snowfall",
    "cloudcover","cloudcover_low","cloudcover_mid","cloudcover_high",
    "windspeed_10m","windgusts_10m","winddirection_10m",
    "shortwave_radiation","direct_normal_irradiance","diffuse_radiation",
    "terrestrial_radiation","pressure_msl"
]

def geocode_city(city_name: str):
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()
    if not data.get("results"):
        raise ValueError(f"City '{city_name}' not found")
    result = data["results"][0]
    return result["latitude"], result["longitude"]

def fetch_weather(lat, lon, start_date, end_date, timezone: str):
    hourly_vars = ",".join(HOURLY_VARS)
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}"
        f"&hourly={hourly_vars}&timezone={timezone}"
    )
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    times = data["hourly"]["time"]
    hourly_data = {var: data["hourly"].get(var, []) for var in HOURLY_VARS}

    records = []
    for i, t in enumerate(times):
        dt = datetime.strptime(t, "%Y-%m-%dT%H:%M")
        ts = dt.strftime("%Y-%m-%d %H")
        record = {"timestamp": ts}
        for var in HOURLY_VARS:
            record[var] = hourly_data[var][i] if i < len(hourly_data[var]) else None
        records.append(record)
    return records
