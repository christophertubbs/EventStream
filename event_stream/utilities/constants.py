"""
Defines constants to be used throughout the application
"""
import os
import re
import string


CLOSE_KEYWORDS = ["close", "disconnect"]
WAIT_DAYS = 3
WAIT_HOURS = WAIT_DAYS * 24
WAIT_MINUTES = WAIT_HOURS * 60
WAIT_SECONDS = WAIT_MINUTES * 60

WAIT_MILLISECONDS = WAIT_SECONDS * 1000
"""The amount of milliseconds to wait for a new message to come through a redis stream"""

TRUE_VALUES = (
    1,
    True,
    "1",
    "t",
    "T",
    "True",
    "TRUE",
    "true",
    "yes",
    "Y",
    "y",
    "Yes",
    "YES",
    "on",
    "On",
    "ON"
)
"""Values that may be considered as `True`"""

IDENTIFIER_SAMPLE_SET = string.hexdigits
"""A collection of values that may be used to form a unique id"""

IDENTIFIER_LENGTH = 5
"""The length of a common generated identifier"""

INTEGER_PATTERN = re.compile(r"^-?\d+$")
"""A regex pattern that matches on a string representing an integer"""

FLOATING_POINT_PATTERN = re.compile(r"^-?\d+\.\d*$")
"""A regex pattern that matches on a string representing a floating point value"""