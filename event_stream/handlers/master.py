"""
Handlers for the master bus
"""
import json
import os
import typing
from pathlib import Path

from datetime import datetime

from redis.asyncio import Redis

from event_stream.messages import Message
from event_stream.messages.master import PurgeMessage
from event_stream.messages.master import TrimMessage
from event_stream.utilities.common import decode_stream_message
from event_stream.utilities.communication import transfer_messages_to_inbox
from event_stream.messages.master import CloseMessage
from event_stream.system import logging
from streams.reader import EventStreamReader
from utilities.types import event_handler

DEFAULT_STREAM_RECORD_LOCATION = Path(os.environ.get("DEFAULT_EVENT_BUS_RECORD_DIRECTORY", "event_records"))
DEFAULT_MAX_STREAM_LENGTH = int(os.environ.get("DEFAULT_MAX_STREAM_LENGTH", "500"))


async def trim_streams(connection: Redis, bus: EventStreamReader, message: TrimMessage, **kwargs):
    """
    This should get the records beyond a given point, write those events out to an agreed upon location,
    and remove the records from the stream

    Args:
        connection:
        bus:
        message:
        **kwargs:

    Returns:

    """
    count = message.count or DEFAULT_MAX_STREAM_LENGTH

    if message.save_output:
        output_path: typing.Union[str, Path] = message.output_path or DEFAULT_STREAM_RECORD_LOCATION

        filename = message.filename

        if not filename:
            date_format = message.date_format or "%Y-%m-%d_%H%M"

            filename = f"{bus.configuration.stream}.{datetime.utcnow().strftime(date_format)}.txt"

        current_length = await connection.xlen(bus.configuration.stream)

        amount_to_write = current_length - count

        if amount_to_write > 0:
            data_to_write = await connection.xrange(bus.configuration.stream, count=amount_to_write)
            data_to_write = {
                key.decode(): decode_stream_message(value)
                for key, value in data_to_write
            }

            output_path = output_path / filename

            with output_path.open(mode='w') as log_file:
                json.dump(data_to_write, log_file)

    await connection.xtrim(bus.configuration.stream, maxlen=count, approximate=True)


async def purge_consumers(connection: Redis, bus: EventStreamReader, message: PurgeMessage, **kwargs):
    stream_exists = await connection.exists(message.stream)

    if not stream_exists:
        return

    # TODO: Return if the group isn't present

    if bus.can_make_executive_decisions():
        if message.consumer is not None:
            await transfer_messages_to_inbox(
                connection=connection,
                stream_name=message.stream,
                group_name=message.group,
                source_consumer=message.consumer
            )
            try:
                await connection.xgroup_delconsumer(
                    name=message.stream,
                    groupname=message.group,
                    consumername=message.consumer
                )
            except BaseException as exception:
                logging.error(
                    f"Could not remove the '{message.consumer}' consumer from the '{message.group}' "
                    f"group from the stream named '{message.stream}'", exception=exception
                )

        current_groups = [
            group['name'].decode()
            for group in await connection.xinfo_groups(name=message.stream)
        ]

        if message.group not in current_groups:
            logging.warning(
                f"Cannot remove the '{message.group}' group from the '{message.stream}' stream - "
                f"there is no group by that name"
            )
            return

        if len(await connection.xpending(name=message.stream, groupname=message.group)) == 0:
            try:
                await connection.xgroup_destroy(name=message.stream, groupname=message.group)
            except BaseException as exception:
                logging.error("Could not remove stream group", exception)
        elif message.force:
            logging.warning(
                f"Received a request to force the removal of a stream group. "
                f"Removing the group even though there are pending messages"
            )
            try:
                await connection.xgroup_destroy(name=message.stream, groupname=message.group)
            except BaseException as exception:
                logging.error("Could not remove stream group", exception)

    else:
        logging.error(
            f"The '{bus.name}' {bus.__class__.__name__} got a request to remove consumers, "
            f"but does not have the authority to do so"
        )


@event_handler(aliases="info")
async def get_instance(connection: Redis, bus: EventStreamReader, message: Message, **kwargs) -> Message:
    """
    Transmit basic information about this available application instance

    Args:
        connection:
        bus:
        message:
        **kwargs:

    Returns:

    """
    return message.create_response(
        bus.configuration.get_application_name(include_instance=False),
        bus.configuration.get_instance_identifier()
    )


@event_handler(aliases="close")
async def close_streams(
    connection: Redis,
    bus: EventStreamReader,
    message: CloseMessage,
    **kwargs
):
    """
    This should end the operations of all streams. All streams end when one ends,
    so all this has to do is close its bus' listen function

    Args:
        connection:
        bus:
        message: The message informing the handler that it needs to disrupt the event bus
        **kwargs:
    """
    # TODO: Use the incoming message object to determine if the user can even request the close
    message_applies_here = bus.configuration.get_application_name(include_instance=False) == message.application_name
    message_applies_here = message_applies_here and bus.configuration.get_instance_identifier() == message.application_instance

    if message_applies_here and bus.can_make_executive_decisions():
        bus.stop_polling()
    elif message_applies_here:
        logging.error(
            f"The '{bus.name}' bus got a request to end all bus operations, but does not have the authority to do so"
        )
    elif bus.verbose:
        logging.info(
            f"Close operations are not being called - they apply to a different instance"
        )