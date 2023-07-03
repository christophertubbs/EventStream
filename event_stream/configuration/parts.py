"""
Provides base classes and mixins for Pydantic Models
"""
import inspect
import json
import typing
import os
import typing_extensions

from pydantic import BaseModel
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import validator

from redis.asyncio import Redis

import event_stream.utilities.types as types
from event_stream import messages

from event_stream.system import logging


class PasswordEnabled:
    """
    A mixin for adding functionality for retrieving a password
    """
    password: typing.Optional[str] = None
    password_file: typing.Optional[str] = None
    password_env_variable: typing.Optional[str] = None

    @validator('password', 'password_file', pre=True)
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

    __found_message_type: typing.Type[types.MessageProtocol] = PrivateAttr(None)
    """
    The found/loaded message type used to parse a message. Not brought in via a validator since it should be loaded 
    after everything else has been imported elsewhere within the code
    """

    @validator('module_name', 'name', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    @classmethod
    def from_message_type(cls, message_type: typing.Type):
        if not issubclass(message_type, types.MessageProtocol):
            raise ValueError(f"A '{str(message_type)}' is not a valid message type")

        module_name = inspect.getmodule(message_type).__name__
        name = message_type.__name__

        designation = cls(module_name=module_name, name=name)
        designation.set_message_type(message_type)

        return designation

    def parse(self, data: typing.Union[str, bytes, typing.Mapping]) -> messages.Message:
        if self.__found_message_type is None:
            self.__found_message_type = types.get_code(self, types.MessageProtocol)

        return self.__found_message_type.parse(data=data)

    def set_message_type(self, message_type: typing.Type):
        if isinstance(message_type, types.MessageProtocol):
            self.__found_message_type = message_type
        else:
            raise ValueError(f"A '{str(message_type)}' is not a valid message type")

    def __hash__(self):
        return hash((self.module_name, self.name))

    def __str__(self):
        return f"{self.module_name}.{self.name}"


class CodeDesignation(BaseModel, types.HANDLER_FUNCTION):
    """
    Represents information used to find a function to call
    """
    @classmethod
    def from_function(
        cls,
        handler_function: types.HANDLER_FUNCTION,
        message_type: typing.Union[typing.Type, MessageDesignation] = None,
        **kwargs
    ) -> typing_extensions.Self:
        types.enforce_handler(handler_function)
        module_name = inspect.getmodule(handler_function).__name__
        name = handler_function.__name__

        designation = cls(module_name=module_name, name=name, kwargs=kwargs)

        if type(message_type) == type:
            designation.message_type = MessageDesignation.from_message_type(message_type=message_type)
        elif isinstance(message_type, MessageDesignation):
            designation.message_type = message_type

        designation.set_function(handler_function, already_checked=True)

        return designation

    module_name: str = Field(description="What module contains the code of interest")
    name: str = Field(description="The name of the code to be invoked")
    kwargs: typing.Optional[typing.Dict[str, typing.Any]] = Field(default_factory=dict)
    message_type: MessageDesignation = Field(default=None)

    __loaded_function: types.HANDLER_FUNCTION = PrivateAttr(None)

    @property
    def aliases(self) -> typing.Sequence[str]:
        operation_aliases: typing.List[str] = list()

        for alias in getattr(self.loaded_function, "aliases", list()):
            operation_aliases.append(alias)

        return operation_aliases

    @property
    def tracker_id(self) -> str:
        id_parts = [
            self.module_name,
            self.name
        ]

        for name, value in self.kwargs.items():
            id_parts.append(f"{str(name)}={str(value)}")

        if self.message_type:
            id_parts.append(f"{self.message_type.module_name}.{self.message_type.name}")

        return ":".join(id_parts)

    @property
    def loaded_function(self) -> types.HANDLER_FUNCTION:
        if self.__loaded_function is None:
            function = types.get_code(self)
            types.enforce_handler(function)
            self.__loaded_function = function

        return self.__loaded_function

    @property
    def identifier(self) -> str:
        return str(self)

    @validator('module_name', 'name', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    def set_function(self, handler_function: types.HANDLER_FUNCTION, *, already_checked: bool = None):
        if already_checked is None:
            already_checked = False

        if not already_checked:
            types.enforce_handler(handler_function)

        self.__loaded_function = handler_function
        self.module_name = inspect.getmodule(handler_function).__name__
        self.name = handler_function.__name__

    def __call__(self, connection: Redis, reader: types.ReaderProtocol, **kwargs):
        keyword_arguments = self.kwargs.copy()
        keyword_arguments.update(kwargs)

        if self.message_type is None:
            message = messages.parse(kwargs)
        else:
            message = self.message_type.parse(kwargs)

        return self.loaded_function(connection, reader, message, **self.kwargs)

    def __hash__(self):
        kwargs = json.dumps(self.kwargs) if self.kwargs else None
        return hash((self.module_name, self.name, kwargs, self.message_type))

    def __str__(self):
        arguments = ["connection", "reader"]

        if self.message_type is None:
            arguments.append("message")
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



