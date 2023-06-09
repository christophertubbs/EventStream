"""
Defines base classes for messages
"""
from __future__ import annotations

import logging
import abc
import json
import inspect
import os
import sys
import typing
import socket

from datetime import datetime

from typing import (
    Union,
    Any,
    Mapping,
    Final,
    Tuple,
    Type,
    ClassVar,
    Optional,
    Dict
)

from pydantic import BaseModel
from pydantic import Field
from pydantic import FilePath
from pydantic import Json
from pydantic import PrivateAttr
from pydantic import validator
from pydantic import ValidationError

from redis.asyncio import Redis

from event_stream.utilities.common import get_by_path
from event_stream.utilities.common import get_current_function_name
from event_stream.utilities.types import INCLUDE_EXCLUDE_TYPES
from event_stream.system.system import settings

ACCEPTABLE_INPUT_TYPES: Final[Tuple[Type, ...]] = (
    dict,
    FilePath,
    str,
    bytes,
)

EVENT_LITERAL_ADJUSTER = 100
MAX_STACK_SIZE = 5


def extra_calculation(calculation: classmethod) -> classmethod:
    setattr(calculation, "is_weight_calculation", True)
    return calculation


def is_weight_calculation(member) -> bool:
    if not (getattr(member, "is_weight_calculation", False) and isinstance(member, typing.Callable)):
        return False

    parameters = [parameter for parameter in inspect.signature(member).parameters.values()]

    return len(parameters) == 1 and parameters[0].name == 'cls'


class ParseableModel(abc.ABC, BaseModel):
    @classmethod
    def parse(
        cls,
        data: Union[ACCEPTABLE_INPUT_TYPES],
        *,
        content_type: str = None,
        allow_pickle: bool = None
    ):
        if allow_pickle is None:
            allow_pickle = False

        if not isinstance(data, ACCEPTABLE_INPUT_TYPES):
            raise TypeError(
                f"'{type(data)}' is not a supported input format for EventStream Messages. "
                f"Acceptable formats are: "
                f"{', '.join([str(acceptable_type) for acceptable_type in ACCEPTABLE_INPUT_TYPES])}"
            )

        if isinstance(data, FilePath):
            model = cls.parse_file(data, content_type=content_type, allow_pickle=allow_pickle)
        elif isinstance(data, dict):
            model = cls.parse_obj(data)
        elif isinstance(data, (str, bytes)):
            model = cls.parse_raw(data, content_type=content_type, allow_pickle=allow_pickle)
        else:
            raise NotImplementedError(f"The parsing logic for '{type(data)}' data has not yet been implemented")

        return model


class WeightedModel(ParseableModel, abc.ABC):
    _weight: ClassVar[Optional[int]] = None
    _extra_weight_calculations: ClassVar[typing.List[typing.Union[str, typing.Callable[[WeightedModel], int]]]] = list()

    @classmethod
    def get_weight(cls) -> float:
        if cls._weight is None:
            required_fields = {
                key: field
                for key, field in cls.__fields__.items()
                if field.required
            }

            weight = len(inspect.getmro(cls))

            for key, field in required_fields.items():
                if isinstance(field.type_, WeightedModel):
                    weight += field.type_.get_weight()
                else:
                    weight += 1

            weight += cls._perform_extra_weight_calculations()

            cls._weight = weight

        return cls._weight

    @classmethod
    def _get_extra_weight_calculations(cls) -> typing.List[typing.Union[str, typing.Callable[[WeightedModel], int]]]:
        calculations = cls._extra_weight_calculations.copy()
        members = inspect.getmembers(
            cls,
            predicate=is_weight_calculation
        )

        calculations.extend([function for name, function in members])

        return calculations

    @classmethod
    def _perform_extra_weight_calculations(cls) -> float:
        extra_weight = 0

        for function_or_name in cls._get_extra_weight_calculations():
            if isinstance(function_or_name, typing.Callable):
                extra_weight += function_or_name(cls)
            else:
                weight_function = getattr(cls, function_or_name, None)

                if weight_function is None:
                    raise ValueError(f"'{function_or_name}' is not an available function to use for weight calculations")

                extra_weight += weight_function()

        return extra_weight


class StackInfo(BaseModel):
    @classmethod
    def create(cls, frame: inspect.FrameInfo):
        return cls(
            line_number=frame.lineno,
            function=frame.function,
            code=frame.code_context[0].strip(),
            file=frame.filename
        )

    @classmethod
    def create_full_stack(cls) -> typing.List[StackInfo]:
        full_stack = list()

        for index, frame in enumerate(inspect.stack()):
            full_stack.insert(
                0,
                cls.create(frame)
            )

            if frame.function.strip() == "<module>" or index >= MAX_STACK_SIZE:
                break

        return full_stack

    line_number: int
    function: str
    code: str
    file: str

    def __str__(self):
        return f"""{self.file}
    {self.function}
        {self.line_number}. {self.code}"""


class HeaderInfo(BaseModel):
    @classmethod
    def create(cls, include_stack: bool = None):
        if include_stack is None:
            include_stack = settings.debug

        args = {
            "caller_application": sys.argv[0],
            "caller_function": get_current_function_name(),
            "caller": socket.getfqdn(socket.gethostname()),
            "platform": str(sys.implementation),
            "date": datetime.now().astimezone().strftime(settings.datetime_format),
            "host": socket.gethostbyname(socket.gethostname())
        }

        if include_stack:
            args['trace'] = StackInfo.create_full_stack()

        return cls(**args)

    caller_application: str
    caller_function: str
    caller: str
    platform: str
    address: str
    date: datetime
    host: str

    trace: typing.Optional[Json[typing.List[StackInfo]]]


class Message(WeightedModel, Mapping):
    event: str = Field(description="The event that created this message")
    header: typing.Optional[Json[HeaderInfo]]
    __extra_data: dict = PrivateAttr(default_factory=dict)

    @classmethod
    @extra_calculation
    def _adjust_weight_for_literal_event(cls):
        signature = inspect.signature(cls)
        class_parameters = signature.parameters
        event_parameter_annotation = class_parameters['event'].annotation

        if event_parameter_annotation != str and event_parameter_annotation != signature.empty:
            return EVENT_LITERAL_ADJUSTER
        else:
            return 0

    def get_event(self) -> str:
        return self.event

    def get_data(self) -> dict:
        return self.__extra_data.copy()

    def get(self, *keys, default=None):
        if len(keys) == 1 and keys[0] in self.__fields__:
            return getattr(self, keys[0], default)
        elif len(keys) == 1:
            return self.__extra_data.get(keys[0], default)
        return get_by_path(self.__extra_data, *keys, default)

    async def send(self, connection: Redis, stream_name: str, include_header: bool = None, **kwargs):
        if include_header is None:
            include_header = True

        if include_header and self.header is None:
            self.header = HeaderInfo.create(include_stack=kwargs.get("include_stack"))

        key_value_pairs = self.__extra_data.copy()
        signature = inspect.signature(self.__class__)

        for field_name in self.__fields__.keys():  # types: str
            field_value = getattr(self, field_name)
            parameter = signature.parameters.get(field_name)

            if parameter and parameter.annotation != parameter.empty:
                field_annotation = parameter.annotation
            else:
                field_annotation = type(name="NotJSONWrapper", bases=tuple(), dict={})

            if field_annotation.__name__ == "JsonWrapperValue" and hasattr(field_annotation, 'inner_type'):
                if isinstance(field_value, BaseModel):
                    key_value_pairs[field_name] = field_value.json()
                else:
                    key_value_pairs[field_name] = json.dumps(field_value)
            else:
                key_value_pairs[field_name] = field_value

        await connection.xadd(stream_name, fields=key_value_pairs)

    def dict(
        self,
        *,
        include: INCLUDE_EXCLUDE_TYPES = None,
        exclude: INCLUDE_EXCLUDE_TYPES = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        dictionary_representation = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none
        )

        for key, value in self.get_data():
            if key not in dictionary_representation:
                dictionary_representation[key] = value

        return dictionary_representation

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        for key, value in kwargs.items():
            if key in self.__fields__:
                continue

            self.__extra_data[key] = value

    def __getitem__(self, key):
        if key in self.__fields__:
            return getattr(self, key)
        return self.__extra_data[key]

    def __setitem__(self, key, value):
        if key in self.__fields__:
            setattr(self, key, value)
        elif key not in self.__extra_data:
            raise KeyError(f"{self.__class__.__name__} does not have a field named '{key}'")
        else:
            self.__extra_data[key] = value

    def __len__(self) -> int:
        return len(self.__extra_data)

    def keys(self) -> typing.Sequence[str]:
        data = [
            key
            for key in self.__extra_data.keys()
            if key not in self.__fields__
        ]

        data.extend(self.__fields__.keys())

        return data

    def values(self) -> typing.Sequence[typing.Any]:
        data = [
            value
            for key, value in self.__extra_data.items()
            if key not in self.__fields__
        ]

        for field_name in self.__fields__:
            data.append(getattr(self, field_name))

        return data

    def items(self) -> typing.Sequence[typing.Tuple[str, typing.Any]]:
        data: typing.List[typing.Tuple[str, typing.Any]] = [
            (key, value)
            for key, value in self.__extra_data.items()
            if key not in self.__fields__
        ]

        for field_name in self.__fields__:
            data.append((field_name, getattr(self, field_name)))

        return data


class JSONIntListMessage(Message):
    data: Json[typing.List[int]]


class GenericMessage(Message):
    data = Json[dict]


class ExampleMessage(Message, abc.ABC):
    @abc.abstractmethod
    def proof_of_example(self):
        ...

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logging.warning(
            f"An '{self.__class__.__name__}' `Message` implementation is being created - "
            "this should only be used for example purposes"
        )


class ExampleEvent(ExampleMessage):
    """
    A simple example showing how to implement a body containing json data within an 'example_data' field.
    """

    def proof_of_example(self):
        return True

    example_data: str

    @validator('example_data')
    def ensure_json(cls, value):
        try:
            json.loads(value)
        except Exception as exception:
            raise ValidationError(f"'example_data' did not contain a valid json document") from exception

        return value


class ValueEvent(ExampleMessage):
    """
    Another example body classes used to show variance in parsing
    """

    def proof_of_example(self):
        return True

    example_body_value: int


class TypedJSONMessage(ExampleMessage):

    def proof_of_example(self):
        return True

    data: Json[ValueEvent]