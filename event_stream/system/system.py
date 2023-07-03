"""
@TODO: Put a module wide description here
"""
import typing
import os

from datetime import timedelta
from pathlib import Path

from pydantic import BaseModel
from pydantic import Field
from pydantic import root_validator
from pydantic import validator

from event_stream.utilities.constants import TRUE_VALUES

DEFAULT_SYSTEM_CONFIG_PATH = Path(os.environ.get("EVENT_BUS_SYSTEM_CONFIG_PATH", "system_settings.json"))
DEFAULT_APPLICATION_NAME = os.environ.get("EVENT_BUS_APPLICATION_NAME", "EventBus")
DEFAULT_DATETIME_FORMAT = os.environ.get("EVENT_BUS_DATETIME_FORMAT", "%Y-%m-%d %H:%M:%S%z")

DEFAULT_REDIS_HOST = os.environ.get("EVENT_BUS_REDIS_HOST", "localhost")
DEFAULT_REDIS_PORT = int(os.environ.get("EVENT_BUS_REDIS_PORT", 6379))
DEFAULT_REDIS_USER = os.environ.get("EVENT_BUS_REDIS_USER", None)
DEFAULT_REDIS_PASSWORD = os.environ.get("EVENT_BUS_REDIS_PASSWORD", None)
DEFAULT_REDIS_DB = int(os.environ.get("EVENT_BUS_REDIS_DB", 0))
DEFAULT_INBOX_CONSUMER_NAME = os.environ.get("EVENT_BUS_SENTINEL_CONSUMER_NAME", "inbox")
DEFAULT_MASTER_STREAM = os.environ.get("EVENT_BUS_MASTER_STREAM", "MASTER")
DEFAULT_MAX_LENGTH = int(float(os.environ.get("EVENT_BUS_MAX_LENGTH", 100)))
LOG_DIRECTORY = Path(os.environ.get("EVENT_BUS_LOG_DIRECTORY", "../"))

KEY_SEPARATOR = os.environ.get("EVENT_BUS_KEY_SEPARATOR", ":")
KEY_LIFETIME_SECONDS = timedelta(seconds=int(os.environ.get("EVENT_BUS_LIFETIME_SECONDS", 60 * 60 * 2)))
DEBUG = os.environ.get("DEBUG_EVENT_BUS", "True").lower() in TRUE_VALUES

MAX_IDLE_TIME_MS = int(os.environ.get("EVENT_BUS_IDLE_TIME_MS", 1000 * 60 * 10))


class _SystemSettings(BaseModel):
    application_name: typing.Optional[str] = Field(default=DEFAULT_APPLICATION_NAME)
    key_prefix: typing.Optional[str] = Field(default=None)
    key_lifetime_seconds: typing.Optional[timedelta] = Field(default=KEY_LIFETIME_SECONDS)
    key_separator: typing.Optional[str] = Field(default=KEY_SEPARATOR)
    datetime_format: typing.Optional[str] = Field(default=DEFAULT_DATETIME_FORMAT)
    debug: typing.Optional[bool] = Field(default=DEBUG)
    log_directory: typing.Optional[typing.Union[str, Path]] = Field(default=LOG_DIRECTORY)
    consumer_inbox_name: typing.Optional[str] = Field(default=DEFAULT_INBOX_CONSUMER_NAME)
    master_stream: typing.Optional[str] = Field(default=DEFAULT_MASTER_STREAM)
    max_idle_time: typing.Optional[int] = Field(default=MAX_IDLE_TIME_MS)
    approximate_max_stream_length: typing.Optional[int] = Field(default=DEFAULT_MAX_LENGTH)

    default_redis_host: typing.Optional[str] = Field(default=DEFAULT_REDIS_HOST)
    default_redis_port: typing.Optional[int] = Field(default=DEFAULT_REDIS_PORT)
    default_redis_user: typing.Optional[str] = Field(default=DEFAULT_REDIS_USER)
    default_redis_password: typing.Optional[str] = Field(default=DEFAULT_REDIS_PASSWORD)
    default_redis_db: typing.Optional[int] = Field(default=DEFAULT_REDIS_DB)

    @validator('*', pre=True)
    def _assign_environment_variables(cls, value):
        """
        Check to see if a value might be an environment variable - if so, evaluate it to get the actual value

        Args:
            value: The value to transform

        Returns:
            The updated value if it was found, the original value otherwise
        """
        value_to_alter = value
        found_value = None

        while isinstance(value_to_alter, str) and value.startswith("$"):
            value_to_alter = os.environ.get(value_to_alter[1:])

            if value_to_alter in os.environ:
                value_to_alter = os.environ.get(value_to_alter)
                found_value = value_to_alter

        return found_value or value

    @root_validator
    def _ensure_defaults(cls, values):
        if not values.get("key_prefix"):
            values['key_prefix'] = values['application_name']

        return values


def initialize(system_config_path: typing.Union[str, Path] = None, data: typing.Union[_SystemSettings, str, bytes, dict] = None):
    global settings

    system_config_path = Path(system_config_path) if system_config_path else None

    if system_config_path is None or not system_config_path.exists():
        system_config_path = DEFAULT_SYSTEM_CONFIG_PATH

    if data and isinstance(data, str):
        settings = _SystemSettings.parse_raw(data)
    elif data and isinstance(data, dict):
        settings = _SystemSettings.parse_obj(data)
    elif system_config_path.exists():
        settings = _SystemSettings.parse_file(system_config_path)
    else:
        raise ValueError(f"Valid input was not available for the creation of system data")


settings = _SystemSettings()