"""
@TODO: Put a module wide description here
"""
import os
import typing
import random
import string

from datetime import datetime

from pydantic import BaseModel
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import root_validator
from pydantic import validator

from . import redis
from .parts import CodeDesignation
from event_stream.utilities.common import get_environment_variable


class EventBusConfiguration(BaseModel):
    """
    The required configuration settings representing an event bus
    """
    name: str = Field(
        description="The name of the Event Bus used to identify its purpose"
    )
    stream: typing.Optional[str] = Field(
        description="A bus specific name for what stream to listen to. Defaults to the globally configured stream"
    )
    handlers: typing.Dict[str, typing.List[CodeDesignation]] = Field(
        description="Lists of event handlers mapped to their event name"
    )
    redis_configuration: typing.Optional[redis.RedisConfiguration] = Field(
        description="A Bus specific configuration for how to communicate with Redis"
    )

    __group: str = PrivateAttr(default=None)

    @classmethod
    @validator('name', 'stream', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.name.startswith("$"):
            self.name = get_environment_variable(self.name)

        if self.stream is not None and self.stream.startswith("$"):
            self.stream = get_environment_variable(self.stream)

    def get_handlers(self, event_name: str) -> typing.List[CodeDesignation]:
        return self.handlers.get(event_name, list())

    def get_group(self):
        return self.__group

    def set_group(self, application_name: str):
        self.__group = f"{application_name}.{self.name}"

    def __str__(self):
        return f"{self.name} => {self.stream if self.stream else '<global stream>'}:{self.__group}"


def update_bus(
    bus: EventBusConfiguration,
    redis_configuration: redis.RedisConfiguration,
    stream: str,
    application_name: str
):
    if bus.redis_configuration is None and redis_configuration is not None:
        bus.redis_configuration = redis_configuration

    if bus.stream is None and stream is None:
        raise ValueError(
            f"There is no stream defined for the '{bus.name}' bus and a global stream name was not provided. "
            f"Defined busses are not valid."
        )
    elif bus.stream is None:
        bus.stream = stream

    bus.set_group(application_name=application_name)


def generate_application_name(name: str):
    if name is None:
        name = "EventBus"

    parts = [name, datetime.now().strftime("%Y%m%d_%H%M%S")]

    group_name = '.'.join(parts)

    return group_name


class EventBusConfigurations(BaseModel):
    """
    A set of different event busses for different channels
    """
    redis_configuration: typing.Optional[redis.RedisConfiguration] = Field(
        description="A globally defined configuration for how to connect to redis."
    )
    busses: typing.List[EventBusConfiguration] = Field(
        min_items=1,
        unique_items=True,
        description="The busses to launch that will be accepting messages"
    )
    stream: str = Field(
        default="EVENTS",
        description="The name of the default stream to use for all busses unless specifically stated"
    )
    name: typing.Optional[str] = Field(
        default="EventBus",
        description="A name for the event bus that may serve as the basis for how to identify all busses"
    )
    _application_name: str = PrivateAttr(None)

    @validator('stream', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            environment_variable = value[1:]
            try:
                value = os.environ[environment_variable]
            except KeyError:
                raise ValueError(
                    f"The global stream for this event bus was supposed to be the value of the "
                    f"`{environment_variable}`, but it was not defined. Event Bus configuration cannot be created."
                )

        return value

    @property
    def application_name(self) -> str:
        if self._application_name is None:
            self._application_name = generate_application_name(self.name)

        return self._application_name

    @root_validator
    def _ensure_bus_integrity(cls, values):
        for bus in values.get("busses", []):
            update_bus(
                bus=bus,
                redis_configuration=values.get("redis_configuration"),
                stream=values.get("stream"),
                application_name=generate_application_name(values.get("name", "EventBus"))
            )
        return values

    def add_bus(self, bus: EventBusConfiguration):
        update_bus(
            bus=bus,
            redis_configuration=self.redis_configuration,
            stream=self.stream,
            application_name=self.application_name
        )

        self.busses.append(bus)

    def __iter__(self):
        return iter(self.busses)
