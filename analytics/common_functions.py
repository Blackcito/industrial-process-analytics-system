#common_functions.py
import logging
from datetime import date

logger = logging.getLogger(__name__)

def format_date(dt: date) -> str:
    """Formats a date to 'YYYY-MM-DD'"""
    return dt.strftime('%Y-%m-%d')


def calculate_stddev(column_name: str) -> str:
    """Returns custom STDDEV_POP function for MariaDB"""
    return f"STDDEV_POP({column_name})"
