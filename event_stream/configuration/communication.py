"""
@TODO: Put a module wide description here
"""
from __future__ import annotations
import typing
import abc

from pydantic import BaseModel
from pydantic import Field
from pydantic import PrivateAttr
from pydantic import root_validator

from event_stream.configuration import RedisConfiguration
from event_stream.system import settings
from event_stream.utilities.common import get_environment_variable
from event_stream.utilities.common import generate_group_name


class ListenerConfiguration(BaseModel, abc.ABC):
    __root_environment_variable_fields: typing.ClassVar[typing.Sequence[str]] = ["name", "stream"]
    _environment_variable_fields: typing.ClassVar[typing.Sequence[str]] = list()
    name: str = Field(description="The name of the listener")
    stream: typing.Optional[str] = Field(description="The name of the stream to read from")
    unique: typing.Optional[bool] = Field(
        default=False,
        description="Whether this listener in this application should be considered unique across all instances. "
                    "Non-unique avoid duplicate activity while Unique activities should run on every application "
                    "instance"
    )
    redis_configuration: typing.Optional[RedisConfiguration] = Field(
        description="Unique instructions for how to connect to Redis"
    )

    _application_name: str = PrivateAttr(None)
    _instance_identifier: str = PrivateAttr(None)
    _group_name: str = PrivateAttr(None)
    _consumer_name: str = PrivateAttr(None)
    _parent = PrivateAttr(None)

    @classmethod
    def _get_environment_variable_fields(cls) -> typing.Sequence[str]:
        fields = [str(field) for field in cls.__root_environment_variable_fields]

        if cls._environment_variable_fields is not None and len(cls._environment_variable_fields) > 0:
            fields.extend([
                str(field)
                for field in cls._environment_variable_fields
                if str(field) not in fields
            ])
        return fields

    @classmethod
    @abc.abstractmethod
    def get_parent_collection_name(cls) -> str:
        ...

    @root_validator
    def _assign_environment_variables(cls, values):
        for field_name in cls._get_environment_variable_fields():
            if field_name in values:
                values[field_name] = get_environment_variable(values[field_name])

        return values

    @abc.abstractmethod
    def handles_event(self, event_name: str) -> bool:
        pass

    @abc.abstractmethod
    def gather_handler_errors(self) -> typing.Optional[typing.Union[typing.Sequence[str], str]]:
        ...

    @abc.abstractmethod
    def get_tracker_ids(self, event_name: str) -> typing.Iterable[str]:
        ...

    def set_application_name(self, application_name: str):
        self._application_name = application_name

    def get_application_name(self, include_instance: bool = None) -> str:
        if include_instance is None:
            include_instance = self.unique

        application_name = self._application_name

        if include_instance:
            application_name += settings.key_separator
            application_name += self.get_instance_identifier()

        return application_name

    def set_instance_identifier(self, instance_identifier: str):
        self._instance_identifier = instance_identifier

    def get_instance_identifier(self) -> str:
        return self._instance_identifier

    @property
    def parent(self):
        return self._parent

    def set_parent(self, parent):
        self._parent = parent

    @property
    def group(self) -> str:
        if self._group_name is None:
            self._group_name = generate_group_name(
                stream_name=self.stream,
                application_name=self.get_application_name(),
                class_name=self.__class__.__name__,
                listener_name=self.name
            )

        return self._group_name

    @property
    def consumer_name(self) -> str:
        if self._consumer_name is None:
            parts = [
                self.stream,
                self.get_application_name(include_instance=True),
                self.__class__.__name__,
                self.name
            ]
            self._consumer_name = settings.key_separator.join(parts)

        return self._consumer_name

    @abc.abstractmethod
    def __str__(self):
        ...
