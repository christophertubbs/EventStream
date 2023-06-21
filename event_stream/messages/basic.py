"""
@TODO: Put a module wide description here
"""
import typing

from pydantic import Field
from pydantic import Json

from event_stream.messages import Message
from event_stream.messages.base import GenericMessage


class ForwardingMessage(Message):
    message: Json[GenericMessage]
    target_stream: str
    include_header: typing.Optional[bool] = Field(True)