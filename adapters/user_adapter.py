"""
adapters/user_adapter.py
User-data ingestion and normalization.

Accepts a raw file object (CSV or IESO-style XML) and returns a DataFrame
with the canonical schema used by all other pipelines:

    timestamp  datetime64[ns]   tz-naive, floored to the hour
    Wind       float64 or NaN
    Solar      float64 or NaN

Column-name inference handles common variations so users don't need to
rename their files before uploading.

Supported CSV shapes
--------------------
Wide   : already has Wind and/or Solar as separate columns
         (e.g., "timestamp,Wind,Solar,..." or "date,wind_mw,solar_mw,...")

Long   : AESO-style, one row per (timestamp, fuel_type, value)
         (e.g., "Date (MST),Fuel Type,Volume,...")
         → pivot_table(aggfunc="sum") merges multiple generators per hour

Supported XML
-------------
IESO   : must conform to http://www.ieso.ca/schema with
         DailyData / HourlyData / FuelTotal structure
"""

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import pandas as pd


# ── Column-name alias dictionaries ────────────────────────────────────────────
# All values are lowercased; matching is done case-insensitively.

TIMESTAMP_ALIASES: set[str] = {
    "timestamp", "date", "datetime", "time",
    "date (mst)", "date (mpt)", "date (et)", "date (est)", "date (utc)",
    "date_time", "datetime_utc", "hour", "date/time", "interval_start",
}

WIND_ALIASES: set[str] = {
    "wind", "wind_mw", "wind generation", "wind output",
    "wind energy", "wind power",
}

SOLAR_ALIASES: set[str] = {
    "solar", "solar_mw", "solar generation", "solar output",
    "solar energy", "solar power",
}

FUEL_TYPE_ALIASES: set[str] = {
    "fuel type", "fuel_type", "type", "fueltype", "fuel",
}

VALUE_ALIASES: set[str] = {
    "volume", "output", "mw", "value", "generation",
    "energy", "power", "amount", "quantity",
}

# Fuel-label values (in the fuel_type column) that map to Wind / Solar
_WIND_LABELS:  set[str] = {"wind", "wind power", "wind energy"}
_SOLAR_LABELS: set[str] = {"solar", "solar power", "solar energy", "photovoltaic", "pv"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_col(columns: list[str], aliases: set[str]) -> str | None:
    """
    Return the first column name (preserving original case) whose lowercased,
    stripped form appears in `aliases`.  Returns None if no match.
    """
    for col in columns:
        if col.strip().lower() in aliases:
            return col
    return None


def _normalise_timestamps(series: pd.Series) -> pd.Series:
    """
    Parse a series of timestamp strings/objects to datetime64[ns], tz-naive,
    floored to the hour boundary.

    Raises ValueError with a human-readable message on parse failure.
    """
    try:
        parsed = pd.to_datetime(series)
    except Exception as exc:
        raise ValueError(f"Cannot parse timestamp column: {exc}") from exc

    # Strip timezone info if present so the result matches tz-naive weather data
    if hasattr(parsed.dt, "tz") and parsed.dt.tz is not None:
        parsed = parsed.dt.tz_localize(None)

    return parsed.dt.floor("h")


# ── CSV parsers ───────────────────────────────────────────────────────────────

def _parse_wide_csv(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Handle CSVs where Wind and/or Solar are already separate columns.
    Renames timestamp and generation columns to canonical names.
    Deduplicates same-hour entries with mean (safe for power readings).
    """
    cols = df_raw.columns.tolist()

    ts_col    = _find_col(cols, TIMESTAMP_ALIASES)
    wind_col  = _find_col(cols, WIND_ALIASES)
    solar_col = _find_col(cols, SOLAR_ALIASES)

    if ts_col is None:
        raise ValueError(
            f"Cannot find a timestamp column in wide-format CSV. "
            f"Columns found: {cols}. "
            f"Expected one of (case-insensitive): {sorted(TIMESTAMP_ALIASES)}"
        )

    df = df_raw.copy()
    df["timestamp"] = _normalise_timestamps(df[ts_col])

    if wind_col:
        df["Wind"] = pd.to_numeric(df[wind_col], errors="coerce")
    if solar_col:
        df["Solar"] = pd.to_numeric(df[solar_col], errors="coerce")

    agg = {}
    if "Wind"  in df.columns: agg["Wind"]  = "mean"
    if "Solar" in df.columns: agg["Solar"] = "mean"

    return df.groupby("timestamp", as_index=False).agg(agg)


def _parse_long_csv(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Handle AESO-style CSVs with one row per (timestamp, fuel_type, value).
    Pivots to wide format; uses sum to aggregate multiple generators per hour.
    """
    cols = df_raw.columns.tolist()

    ts_col    = _find_col(cols, TIMESTAMP_ALIASES)
    fuel_col  = _find_col(cols, FUEL_TYPE_ALIASES)
    value_col = _find_col(cols, VALUE_ALIASES)

    if ts_col is None or fuel_col is None or value_col is None:
        raise ValueError(
            f"Long-format CSV requires a timestamp column, a fuel-type column, "
            f"and a value column. Columns found: {cols}"
        )

    df = df_raw[[ts_col, fuel_col, value_col]].copy()
    df.columns = ["timestamp", "_fuel", "_value"]

    df["timestamp"] = _normalise_timestamps(df["timestamp"])
    df["_fuel"]     = df["_fuel"].astype(str).str.strip().str.lower()
    df["_value"]    = pd.to_numeric(df["_value"], errors="coerce")

    # Keep only Wind and Solar rows
    wind_mask  = df["_fuel"].isin(_WIND_LABELS)
    solar_mask = df["_fuel"].isin(_SOLAR_LABELS)
    df = df[wind_mask | solar_mask].copy()

    if df.empty:
        raise ValueError(
            f"Long-format CSV contains no rows where the fuel-type column "
            f"('{fuel_col}') equals 'WIND' or 'SOLAR' (case-insensitive). "
            f"Distinct values found: {list(df_raw[fuel_col].unique())[:20]}"
        )

    # Normalise fuel labels to WIND / SOLAR for pivot
    df.loc[wind_mask[wind_mask | solar_mask].reindex(df.index, fill_value=False), "_fuel"] = "WIND"
    df.loc[solar_mask[wind_mask | solar_mask].reindex(df.index, fill_value=False), "_fuel"] = "SOLAR"
    # Simpler rewrite without boolean gymnastics:
    df["_fuel"] = df["_fuel"].apply(
        lambda f: "WIND" if f in _WIND_LABELS else "SOLAR"
    )

    pivoted = df.pivot_table(
        index="_value_ignored",  # placeholder; use timestamp
        columns=None,
        values=None,
        aggfunc=None,
    )
    # Use a clean pivot approach:
    pivoted = (
        df.groupby(["timestamp", "_fuel"])["_value"]
          .sum()
          .unstack("_fuel")
          .reset_index()
    )
    pivoted.columns.name = None

    rename = {}
    if "WIND"  in pivoted.columns: rename["WIND"]  = "Wind"
    if "SOLAR" in pivoted.columns: rename["SOLAR"] = "Solar"
    pivoted = pivoted.rename(columns=rename)

    return pivoted


def _parse_csv_obj(file_obj) -> pd.DataFrame:
    """
    Read a CSV file object and detect whether it is wide or long format.

    Detection order:
      1. Wide  — Wind or Solar alias found as a direct column header
      2. Long  — fuel-type alias AND value alias both found
      3. Error — neither detected
    """
    try:
        df_raw = pd.read_csv(file_obj)
    except Exception as exc:
        raise ValueError(f"Could not read CSV file: {exc}") from exc

    if df_raw.empty:
        raise ValueError("Uploaded CSV is empty.")

    cols = df_raw.columns.tolist()

    wind_col  = _find_col(cols, WIND_ALIASES)
    solar_col = _find_col(cols, SOLAR_ALIASES)

    if wind_col or solar_col:
        return _parse_wide_csv(df_raw)

    fuel_col  = _find_col(cols, FUEL_TYPE_ALIASES)
    value_col = _find_col(cols, VALUE_ALIASES)

    if fuel_col and value_col:
        return _parse_long_csv(df_raw)

    raise ValueError(
        f"Cannot determine generation columns from CSV. Columns found: {cols}. "
        "For wide format, include a 'Wind' and/or 'Solar' column (or common aliases). "
        "For long format (AESO-style), include a fuel-type column (e.g. 'Fuel Type') "
        "and a value column (e.g. 'Volume' or 'MW')."
    )


# ── XML parser ────────────────────────────────────────────────────────────────

def _parse_ieso_xml_obj(file_obj) -> pd.DataFrame:
    """
    Parse an IESO-style XML file from a file-like object.
    Schema: http://www.ieso.ca/schema with DailyData/HourlyData/FuelTotal.
    Returns DataFrame[timestamp str, Wind float, Solar float].
    """
    ns = {"ns": "http://www.ieso.ca/schema"}
    try:
        tree = ET.parse(file_obj)
    except ET.ParseError as exc:
        raise ValueError(
            f"Invalid XML: {exc}. Only IESO-style XML is supported."
        ) from exc

    root = tree.getroot()
    rows = []

    for daily in root.findall(".//ns:DailyData", ns):
        day_el = daily.find("ns:Day", ns)
        if day_el is None or not day_el.text:
            continue
        date = day_el.text.strip()

        for hourly in daily.findall("ns:HourlyData", ns):
            hour_el = hourly.find("ns:Hour", ns)
            if hour_el is None or not hour_el.text:
                continue
            try:
                hour_int = int(hour_el.text.strip())
            except ValueError:
                continue

            # IESO uses hour 24 to mean midnight of the next day
            if hour_int == 24:
                dt = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
                ts_str = dt.strftime("%Y-%m-%d 00")
            else:
                ts_str = f"{date} {hour_int:02d}"

            wind_val, solar_val = 0.0, 0.0
            for fuel in hourly.findall("ns:FuelTotal", ns):
                ftype  = fuel.find("ns:Fuel", ns)
                output = fuel.find("ns:EnergyValue/ns:Output", ns)
                if ftype is None or output is None or output.text is None:
                    continue
                label = ftype.text.strip().upper()
                try:
                    val = float(output.text.strip())
                except ValueError:
                    continue
                if label == "WIND":
                    wind_val = val
                elif label == "SOLAR":
                    solar_val = val

            rows.append({"timestamp": ts_str, "Wind": wind_val, "Solar": solar_val})

    if not rows:
        raise ValueError(
            "XML parsed successfully but contained no WIND/SOLAR hourly records."
        )

    df = pd.DataFrame(rows)
    df["timestamp"] = _normalise_timestamps(df["timestamp"])
    return df


# ── Public entry point ────────────────────────────────────────────────────────

def ingest(file_obj, file_format: str) -> pd.DataFrame:
    """
    Ingest an uploaded file and return a canonical normalized DataFrame.

    Parameters
    ----------
    file_obj    : file-like object (e.g. UploadFile.file)
    file_format : "csv" or "xml"

    Returns
    -------
    pd.DataFrame
        Columns: timestamp (datetime64[ns] tz-naive hourly), Wind (float|NaN), Solar (float|NaN)
        One row per unique hour, sorted ascending by timestamp.
        Never has duplicate timestamps.
    """
    if file_format == "csv":
        df = _parse_csv_obj(file_obj)
    elif file_format == "xml":
        df = _parse_ieso_xml_obj(file_obj)
    else:
        raise ValueError(
            f"Unsupported file_format {file_format!r}. Use 'csv' or 'xml'."
        )

    # ── Ensure both generation columns exist with the right dtype ──────────
    for col in ("Wind", "Solar"):
        if col not in df.columns:
            df[col] = float("nan")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Ensure timestamp is fully normalised (tz-naive, hourly) ────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if hasattr(df["timestamp"].dt, "tz") and df["timestamp"].dt.tz is not None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    df["timestamp"] = df["timestamp"].dt.floor("h")

    # ── Final dedup: mean handles duplicate readings for the same hour ──────
    df = (
        df.groupby("timestamp", as_index=False)
          .agg({"Wind": "mean", "Solar": "mean"})
          .sort_values("timestamp")
          .reset_index(drop=True)
    )

    return df[["timestamp", "Wind", "Solar"]]
