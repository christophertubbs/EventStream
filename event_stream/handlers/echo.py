"""
Defines the most basic event handler - one that just prints data to stdout
"""

from redis.asyncio import Redis

from event_stream.messages import Message
from utilities.types import ReaderProtocol
from utilities.types import event_handler


@event_handler(aliases="echo")
def echo_message(connection: Redis, reader: ReaderProtocol, message: Message, **kwargs) -> Message:
    print(f"The '{message.event}' event has been triggered on a(n) '{reader.__class__.__name__}' named '{reader.name}'!")
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
            application_instance=reader.configuration.get_instance_identifier(),
            application_name=reader.configuration.get_application_name()
        )
    else:
        response = None

    return response
