"""
@TODO: Put a module wide description here
"""
import typing

from redis.asyncio import Redis

from event_stream.utilities.types import BusProtocol
from event_stream.messages import Message


def echo_message(connection: Redis, bus: BusProtocol, message: Message, **kwargs) -> Message:
    print(f"The '{message.event}' event has been triggered on a(n) '{bus.__class__.__name__}' named '{bus.name}'!")
    print(f"Message Type: {type(message)}")
    print(f"Fields:")
    for field_name, field_metadata in message.__fields__.items():
        print(f"    {field_name}: {str(getattr(message, field_name))}")

    print()

    print("Data:")
    for field_name, field_value in message.items():
        print(f"    {field_name}: {str(field_value)}")

    print()

    if kwargs:
        print("Received Keyword Arguments:")
        for index, key_and_value in enumerate(kwargs.items()):
            print(f"    {index + 1}. {str(key_and_value[0])} => {str(key_and_value[1])}")

    if kwargs.get("transmit_response", False):
        response = message.create_response(
            application_instance=bus.get_instance_identifier(),
            application_name=bus.get_application_name()
        )
    else:
        response = None

    return response