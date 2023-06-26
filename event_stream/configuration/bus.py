"""
Defines configuration objects defining how a Bus listener and a collection of listeners should behave
"""
from __future__ import annotations
import os
import typing

from pydantic import BaseModel
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import root_validator
from pydantic import validator

from . import redis
from . import group
from .communication import ListenerConfiguration
from .parts import CodeDesignation
from event_stream.utilities.common import generate_identifier
from event_stream.system import settings


class EventBusConfiguration(ListenerConfiguration):
    """
    The required configuration settings representing an event bus
    """

    def handles_event(self, event_name: str) -> bool:
        return event_name in self.handlers

    def get_tracker_ids(self, event_name: str) -> typing.Iterable[str]:
        if event_name in self.handlers:
            return [handler.tracker_id for handler in self.get_handlers(event_name=event_name)]
        else:
            return list()

    @classmethod
    def get_parent_collection_name(cls) -> str:
        return "busses"

    handlers: typing.Dict[str, typing.List[CodeDesignation]] = Field(
        description="Lists of event handlers mapped to their event name"
    )

    def get_handlers(self, event_name: str) -> typing.List[CodeDesignation]:
        return self.handlers.get(event_name, list())

    def __str__(self):
        return f"{self.name} => {self.stream if self.stream else '<global stream>'}:{self.group}"

    def gather_handler_errors(self) -> typing.Optional[typing.Union[typing.Sequence[str], str]]:
        errors: typing.List[str] = list()
        if self.handlers is None or len(self.handlers) == 0:
            errors.append(
                f"No configurations for handlers for the {self.__class__.__name__} named {self.name} could be found"
            )
            return errors

        for handler in self.handlers:  # type: CodeDesignation
            try:
                if handler.loaded_function is None:
                    errors.append(
                        f"The following handler for the {self.__class__.__name__} named {self.name} "
                        f"could not be found: '{str(handler)}'"
                    )
            except BaseException as exception:
                errors.append(
                    f"The following handler for the {self.__class__.__name__} named {self.name} "
                    f"could not be loaded: '{str(handler)} ({str(exception)})'"
                )

        return errors


def update_bus(
    bus: EventBusConfiguration,
    redis_configuration: redis.RedisConfiguration,
    stream: str,
    application_name: str,
    application_identifier: str
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

    bus.set_application_name(application_name=application_name)
    bus.set_instance_identifier(instance_identifier=application_identifier)


def update_handler_group(
    handler_group: group.HandlerGroup,
    redis_configuration: redis.RedisConfiguration,
    stream: str,
    application_name: str,
    application_identifier: str
):
    if handler_group.redis_configuration is None and redis_configuration is not None:
        group.redis_configuration = redis_configuration

    if handler_group.stream is None and stream is None:
        raise ValueError(
            f"There is no stream defined for the '{handler_group.name}' bus and a global stream name was not provided. "
            f"Defined busses are not valid."
        )
    elif handler_group.stream is None:
        handler_group.stream = stream

    handler_group.set_application_name(application_name)
    handler_group.set_instance_identifier(application_identifier)


class EventBusConfigurations(BaseModel):
    """
    A set of different event busses for different channels
    """
    @classmethod
    def from_listener(cls, listener: ListenerConfiguration) -> EventBusConfigurations:
        configuration = {
            "redis_configuration": listener.redis_configuration,
            "name": listener.get_application_name(),
            "stream": listener.stream,
            listener.get_parent_collection_name(): [listener]
        }

        bus_configurations = cls.parse_obj(configuration)
        bus_configurations.set_application_identifier(listener.get_instance_identifier())
        return bus_configurations

    redis_configuration: typing.Optional[redis.RedisConfiguration] = Field(
        description="A globally defined configuration for how to connect to redis."
    )
    busses: typing.Optional[typing.List[EventBusConfiguration]] = Field(
        unique_items=True,
        description="The busses to launch that will be accepting messages"
    )
    handlers: typing.Optional[typing.List[group.HandlerGroup]] = Field(
        unique_items=True,
        description="Handlers that conduct their own atomic asynchronous operations"
    )
    stream: str = Field(
        default="EVENTS",
        description="The name of the default stream to use for all busses unless specifically stated"
    )
    name: typing.Optional[str] = Field(
        default=settings.application_name,
        description="A name for the event bus that may serve as the basis for how to identify all busses"
    )
    _application_name: str = PrivateAttr(None)
    _application_identifier: typing.ClassVar[str] = generate_identifier()

    @validator('stream', "name", pre=True)
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
            self._application_name = self.name or settings.application_name

        return self._application_name

    @property
    def application_identifier(self) -> str:
        return self._application_identifier

    def set_application_identifier(self, identifier: str):
        self.__class__._application_identifier = identifier

    @root_validator
    def _ensure_bus_integrity(cls, values):
        if values.get("busses") is None and values.get("handlers") is None:
            raise Exception(
                "Either a series of bus objects ('busses') or handler objects ('handlers') must be defined. "
                "Neither could be found."
            )

        for bus in values.get("busses", []):  # type: EventBusConfiguration
            update_bus(
                bus=bus,
                redis_configuration=values.get("redis_configuration"),
                stream=values.get("stream"),
                application_name=values.get("name", "EventBus"),
                application_identifier=cls._application_identifier
            )

        for handler_group in values.get("handlers", []):
            update_handler_group(
                handler_group=handler_group,
                redis_configuration=values.get("redis_configuration"),
                stream=values.get("stream"),
                application_name=values.get("name", "EventBus"),
                application_identifier=cls._application_identifier
            )

        return values

    @root_validator
    def _ensure_handlers_are_present(cls, values: dict) -> dict:
        errors: typing.List[str] = list()

        for bus in values.get("busses", []):  # type: EventBusConfiguration
            bus_errors = bus.gather_handler_errors

            if isinstance(bus_errors, (str, bytes)):
                errors.append(bus_errors)
            elif isinstance(bus_errors, typing.Iterable):
                errors.extend(bus_errors)

        for handler in values.get("handlers", []):  # type: group.HandlerGroup
            handler_errors = handler.gather_handler_errors()
            if isinstance(handler_errors, (str, bytes)):
                errors.append(handler_errors)
            elif isinstance(handler_errors, typing.Iterable):
                errors.extend(handler_errors)

        if len(errors) > 0:
            message = f"Invalid event handlers were found:{os.linesep}    "
            message += f"{os.linesep}    ".join(errors)
            raise ValueError(message)

        return values

    def add_bus(self, bus: EventBusConfiguration):
        update_bus(
            bus=bus,
            redis_configuration=self.redis_configuration,
            stream=self.stream,
            application_name=self.application_name,
            application_identifier=self.application_identifier
        )

        self.busses.append(bus)

    def add_handler(self, handler: group.HandlerGroup):
        update_handler_group(
            handler_group=handler,
            redis_configuration=self.redis_configuration,
            stream=self.stream,
            application_name=self.name,
            application_identifier=self.application_identifier
        )

        self.handlers.append(handler)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for bus in self.busses:
            bus.set_parent(self)

        for handler in self.handlers:
            handler.set_parent(self)
