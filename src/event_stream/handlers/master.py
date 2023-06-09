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
from event_stream.messages.master import TrimMessage
from event_stream.utilities.common import decode_stream_message
from event_stream.utilities.common import get_by_path
from event_stream.utilities.types import BusProtocol
from event_stream.messages.master import CloseMessage
from event_stream.system import logging


DEFAULT_STREAM_RECORD_LOCATION = Path(os.environ.get("DEFAULT_EVENT_BUS_RECORD_DIRECTORY", "event_records"))
DEFAULT_MAX_STREAM_LENGTH = int(os.environ.get("DEFAULT_MAX_STREAM_LENGTH", "500"))


async def trim_streams(connection: Redis, bus: BusProtocol, message: TrimMessage, **kwargs):
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


async def close_streams(
    connection: Redis,
    bus: BusProtocol,
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
    if bus.is_allowed_to_close():
        bus.stop_polling()
    else:
        logging.error(
            f"The '{bus.name}' bus got a request to end all bus operations, but does not have the authority to do so"
        )