"""
Provides base classes and mixins for Pydantic Models
"""
import json
import typing
import os

from pydantic import BaseModel
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import validator

from redis.asyncio import Redis

import event_stream.utilities.types as types
from event_stream.utilities.common import get_code
from event_stream import messages

from event_stream.system import logging


class PasswordEnabled:
    """
    A mixin for adding functionality for retrieving a password
    """
    password: typing.Optional[str] = None
    password_file: typing.Optional[str] = None
    password_env_variable: typing.Optional[str] = None

    @validator('*', pre=True)
    def _assign_environment_variables_to_passwords(self, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    def get_password(self):
        password = None

        if self.password:
            password = self.password
        elif self.password_env_variable:
            password = os.environ.get(self.password_env_variable, None)
            if password is None:
                logging.warning(
                    f"The password environment variable named '{self.password_env_variable}' was not set. "
                    f"Defaulting to 'None'"
                )
        elif self.password_file:
            if os.path.isfile(self.password_file):
                with open(self.password_file, 'r') as password_file:
                    password = password_file.read().rstrip()
                if password == "":
                    password = None
            else:
                logging.warning(
                    f"No file could be found at '{self.password_file}' that might contain a password. "
                    f"Defaulting to None."
                )

        return password


class MessageDesignation(BaseModel):
    """
    Represents information used to find a message type
    """
    module_name: str
    name: str

    __found_message_type: typing.Type[messages.Message] = PrivateAttr(None)
    """
    The found/loaded message type used to parse a message. Not brought in via a validator since it should be loaded 
    after everything else has been imported elsewhere within the code
    """

    @validator('module_name', 'name', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    def parse(self, data: typing.Union[str, bytes, typing.Mapping]) -> messages.Message:
        if self.__found_message_type is None:
            self.__found_message_type = get_code(self, messages.Message)

        return self.__found_message_type.parse(data)

    def __hash__(self):
        return hash((self.module_name, self.name))

    def __str__(self):
        return f"{self.module_name}.{self.name}"


class CodeDesignation(BaseModel, types.HANDLER_FUNCTION):
    """
    Represents information used to find a function to call
    """
    module_name: str = Field(description="What module contains the code of interest")
    name: str = Field(description="The name of the code to be invoked")
    kwargs: typing.Optional[typing.Dict[str, typing.Any]] = Field(default_factory=dict)
    message_type: MessageDesignation = Field(default=None)

    __loaded_function: types.HANDLER_FUNCTION = PrivateAttr(None)

    @property
    def identifier(self) -> str:
        return str(self)

    @validator('module_name', 'name', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    def __call__(self, connection: Redis, bus: types.BusProtocol, **kwargs):
        if self.__loaded_function is None:
            self.__loaded_function = get_code(self)
            types.enforce_handler(self.__loaded_function)

        keyword_arguments = self.kwargs.copy()
        keyword_arguments.update(kwargs)

        if self.message_type is None:
            message = messages.parse(kwargs)
        else:
            message = self.message_type.parse(kwargs)

        return self.__loaded_function(connection, bus, message, **self.kwargs)

    def __hash__(self):
        kwargs = json.dumps(self.kwargs) if self.kwargs else None
        return hash((self.module_name, self.name, kwargs, self.message_type))

    def __str__(self):
        arguments = ["connection", "bus", "event"]

        if self.message_type is None:
            arguments.append("Most Appropriate Message")
        else:
            arguments.append(str(self.message_type))

        arguments.extend(
            [
                f"{key}={value}"
                for key, value in self.kwargs.items()
            ]
        )

        if self.message_type is None:
            arguments.append("**kwargs")

        return f"{self.module_name}.{self.name}({', '.join(arguments)})"

    def __repr__(self):
        return self.__str__()



