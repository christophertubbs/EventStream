"""
Examples for how to build messages
"""
import abc
import json
import typing

from pydantic import Json
from pydantic import ValidationError
from pydantic import validator

from messages import Message
from system import logging


class ExampleMessage(Message, abc.ABC):
    @classmethod
    def is_an_example(cls):
        return True

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
    example_body_value: int


class TypedJSONMessage(ExampleMessage):
    data: typing.Union[Json[ValueEvent], ValueEvent]
