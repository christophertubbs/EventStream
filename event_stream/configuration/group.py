"""
Defines a listener that handles exactly one type of event
"""
from __future__ import annotations
import typing

from pydantic import Field

from .parts import MessageDesignation
from .parts import CodeDesignation
from .communication import ListenerConfiguration

from event_stream.system import settings


class HandlerGroup(ListenerConfiguration):
    def get_tracker_ids(self, event_name: str) -> typing.Iterable[str]:
        return [self.handler.tracker_id]

    @property
    def group(self) -> str:
        """
        Override for the group name logic

        Returns:
            the group name - typically something like: 'Application:EVENTS:HandlerGroup:event_stream.handlers.do_something(connection, bus, message, something=other)'
        """
        if self._group_name is None:
            parts = [
                self.stream,
                self.get_application_name(),
                self.__class__.__name__,
                str(self.handler)
            ]
            self._group_name = settings.key_separator.join(parts)

        return self._group_name

    @property
    def consumer_name(self) -> str:
        if self._consumer_name is None:
            parts = [
                self.stream,
                self.get_application_name(include_instance=True),
                self.__class__.__name__,
                str(self.handler)
            ]
            self._consumer_name = settings.key_separator.join(parts)

        return self._consumer_name

    @classmethod
    def get_parent_collection_name(cls) -> str:
        return "handlers"

    def gather_handler_errors(self) -> typing.Optional[typing.Union[typing.Sequence[str], str]]:
        error: typing.Optional[str] = None

        try:
            if self.handler.loaded_function is None:
                error = f"A handler could not be loaded for a {self.__class__.__name__}: " \
                        f"{self.name}: {str(self.handler)}"
        except BaseException as exception:
            error = f"A handler could not be loaded for a {self.__class__.__name__}: " \
                        f"{self.name}: {str(self.handler)}. {str(exception)}"

        return error

    _environment_variable_fields = ["event"]
    event: str = Field(description="The name of the event to handle")
    handler: CodeDesignation = Field(description="What will handle the incoming message")
    message_type: typing.Optional[MessageDesignation] = Field(
        description="A specific specification for how to parse incoming messages"
    )

    def __str__(self):
        return f"Call {self.handler} when the '{self.event}' event is found in the '{self.stream}' stream."
