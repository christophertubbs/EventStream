"""
@TODO: Put a module wide description here
"""
import typing

from redis.asyncio import Redis


from event_stream.messages.basic import ForwardingMessage
from utilities.types import ReaderProtocol


async def forward_message(
    connection: Redis,
    reader: ReaderProtocol,
    message: ForwardingMessage,
    include_header: bool = None,
    **kwargs
):
    await message.message.send(connection, message.target_stream, include_header=include_header, **kwargs)