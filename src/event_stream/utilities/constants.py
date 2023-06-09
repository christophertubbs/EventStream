"""
Defines constants to be used throughout the application
"""
import os
import typing
import re


CLOSE_KEYWORDS = ["close", "disconnect"]
DATETIME_FORMAT: str = os.environ.get(
    "EVENTSTREAM_DATETIME_FORMAT",
    os.environ.get("DATETIME_FORMAT", "%Y-%m-%d %H:%M:%S %Z")
)
WAIT_DAYS = 3
WAIT_HOURS = WAIT_DAYS * 24
WAIT_MINUTES = WAIT_HOURS * 60
WAIT_SECONDS = WAIT_MINUTES * 60

WAIT_MILLISECONDS = WAIT_SECONDS * 1000
"""The amount of milliseconds to wait for a new message to come through a redis stream"""

INTEGER_PATTERN = re.compile(r"^-?\d+$")
FLOATING_POINT_PATTERN = re.compile(r"^-?\d+\.\d*$")