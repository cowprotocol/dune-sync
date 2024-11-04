"""Prefect Deployment for Order Rewards Data"""
from datetime import datetime, timedelta, timezone


def get_last_monday_midnight_utc() -> int:
    """Get the timestamp of last monday at midnight UTC"""
    now = datetime.now(timezone.utc)
    current_weekday = now.weekday()
    days_since_last_monday = current_weekday if current_weekday != 0 else 7
    last_monday = now - timedelta(days=days_since_last_monday)
    last_monday_midnight = last_monday.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    timestamp = int(last_monday_midnight.timestamp())
    return timestamp
