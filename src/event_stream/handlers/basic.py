"""
@TODO: Put a module wide description here
"""
import typing

from redis.asyncio import Redis


from event_stream.messages.basic import ForwardingMessage
from event_stream.utilities.types import BusProtocol


async def forward_message(
    connection: Redis,
    bus: BusProtocol,
    message: ForwardingMessage,
    include_header: bool = None,
    **kwargs
):
    await message.message.send(connection, message.target_stream, include_header=include_header, **kwargs)