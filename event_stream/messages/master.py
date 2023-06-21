"""
@TODO: Put a module wide description here
"""
import typing

from pydantic import SecretStr
from pydantic import Field

from event_stream.messages import Message
from event_stream.messages.mixins import UserMixin


class CloseMessage(Message, UserMixin):
    token: SecretStr
    application_name: str
    application_instance: str


class TrimMessage(Message):
    event: typing.Literal["trim"] = "trim"

    count: typing.Optional[int]
    save_output: typing.Optional[bool] = Field(default=False)
    output_path: typing.Optional[str]
    filename: typing.Optional[str]
    date_format: typing.Optional[str]


class PurgeMessage(Message):
    stream: str
    group: str
    consumer: typing.Optional[str]
    force: typing.Optional[bool] = Field(False)