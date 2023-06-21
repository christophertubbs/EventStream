"""
Defines functions related to interpretting and manipulating data about application instances
"""
from redis.asyncio import Redis

from event_stream.utilities.types import BusProtocol
from event_stream.messages.base import Message


def print_instance_info(connection: Redis, bus: BusProtocol, message: Message, **kwargs):
    """
    Prints information about a message's given application name and instance

    The main use case is to list available applications/instances on the given stream to identify what can be closed

    Args:
        connection:
        bus:
        message:
        **kwargs:
    """
    print(f"Received data about an event stream application instance:")
    print()
    print(f"Application: {message.application_name}")
    print(f"Instance: {message.application_instance}")
    print()