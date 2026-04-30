from datetime import datetime, timezone

SENTINEL_EPOCH = 0  # Represents "never opened" stages in DIM_TIME


def normalize_to_seconds(ts):
    """
    Normalize various timestamp formats to Unix seconds (float).
    
    Handles:
        - Unix seconds (10 digits):      1770987536
        - Unix seconds with decimal:      1771305341.447
        - Unix milliseconds (13 digits):  1770987514757
        - Sentinel value -1:              returns None
    """
    if ts is None or ts == -1:
        return None

    ts = float(ts)

    # 13+ digit values are milliseconds
    if ts > 1e12:
        ts = ts / 1000.0

    return ts


def build_dim_time_record(unix_seconds):
    """
    Build a DIM_TIME attribute dict from Unix seconds.
    
    Returns None if input is None.
    
    Example output:
        {
            "full_timestamp": "2026-04-15 13:24:06",
            "minute":         "2026-04-15 13:24:00",
            "hour":           "2026-04-15 13:00:00",
            "day":            "2026-04-15 00:00:00",
            "week":           "2026-16",
            "month":          "2026-04",
            "year":           "2026",
        }
    """
    if unix_seconds is None:
        return None

    dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    iso_year, iso_week, _ = dt.isocalendar()

    return {
        "full_timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "minute": dt.strftime("%Y-%m-%d %H:%M:00"),
        "hour": dt.strftime("%Y-%m-%d %H:00:00"),
        "day": dt.strftime("%Y-%m-%d 00:00:00"),
        "week": f"{iso_year}-{iso_week:02d}",
        "month": dt.strftime("%Y-%m"),
        "year": dt.strftime("%Y"),
    }

def build_sentinel_time_record():
    """
    Build a DIM_TIME record for the sentinel timestamp (epoch 0).
    Used for stages that were never opened.
    
    Result: full_timestamp = "1970-01-01 00:00:00"
    """
    return build_dim_time_record(SENTINEL_EPOCH)

def ts_str_to_unix(ts_str):
    """Convert a DIM_TIME full_timestamp string back to Unix seconds (UTC)."""
    if ts_str is None:
        return None
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.timestamp()