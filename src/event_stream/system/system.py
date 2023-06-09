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

DEFAULT_SYSTEM_CONFIG_PATH = Path(os.environ.get("EVENT_BUS_SYSTEM_CONFIG_PATH", "system_settings.json"))
DEFAULT_APPLICATION_NAME = os.environ.get("EVENT_BUS_APPLICATION_NAME", "EventBus")
DEFAULT_DATETIME_FORMAT = os.environ.get("EVENT_BUS_DATETIME_FORMAT", "%Y-%m-%d %H:%M:%S%z")
LOG_DIRECTORY = Path(os.environ.get("EVENT_BUS_LOG_DIRECTORY", "../"))

KEY_SEPARATOR = os.environ.get("EVENT_BUS_KEY_SEPARATOR", "::")
KEY_LIFETIME_SECONDS = timedelta(seconds=int(os.environ.get("EVENT_BUS_LIFETIME_SECONDS", 60 * 60 * 2)))
DEBUG = os.environ.get("DEBUG_EVENT_BUS", "True").lower() in ("t","y", "true", "on", "1")


class _SystemSettings(BaseModel):
    application_name: typing.Optional[str] = Field(default=DEFAULT_APPLICATION_NAME)
    key_prefix: typing.Optional[str] = Field(default=None)
    key_lifetime_seconds: typing.Optional[timedelta] = Field(default=KEY_LIFETIME_SECONDS)
    key_separator: typing.Optional[str] = Field(default=KEY_SEPARATOR)
    datetime_format: typing.Optional[str] = Field(default=DEFAULT_DATETIME_FORMAT)
    debug: typing.Optional[bool] = Field(default=DEBUG)
    log_directory: typing.Optional[typing.Union[str, Path]] = Field(default=LOG_DIRECTORY)

    @root_validator
    def _ensure_defaults(cls, values):
        if not values.get("application_name"):
            values['application_name'] = DEFAULT_APPLICATION_NAME

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