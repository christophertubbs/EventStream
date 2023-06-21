"""
@TODO: Put a module wide description here
"""
import typing
from collections import OrderedDict
from asyncio import sleep

import redis as synchronous_redis
from redis import asyncio
from redis.lock import Lock

import event_stream.system.logging as logging
from event_stream.configuration import RedisConfiguration
from event_stream.utilities.common import is_true
from event_stream.utilities.types import STREAMS
from event_stream.utilities.common import generate_identifier
from event_stream.system import settings


EXIT_INDICATOR: typing.Literal["END"] = "END"
DEFAULT_BLOCK_MILLISECONDS = 100 * 1000

UNPROCESSED_VALUE = "UNPROCESSED"


def create_lock(connection: asyncio.Redis, stream_name: str, group_name: str, message_id: str = None) -> Lock:
    key = f"{group_name}"

    if not isinstance(connection, synchronous_redis.Redis):
        # A synchronous connection MUST be used - async version does not await
        connection = synchronous_redis.Redis(**connection.connection_pool.connection_kwargs)
    return Lock(redis=connection, name=key, blocking=True)


async def transfer_messages_to_inbox(
    connection: asyncio.Redis,
    stream_name: str,
    group_name: str,
    source_consumer: str,
    inbox_name: str = None
):
    if inbox_name is None:
        inbox_name = settings.consumer_inbox_name

    with create_lock(connection=connection, stream_name=stream_name, group_name=group_name):
        pending_messages: typing.Sequence[dict] = await connection.xpending_range(
            name=stream_name,
            groupname=group_name,
            consumername=source_consumer,
            min="-",
            max="+",
            count=9999999
        )

        message_ids = [
            message['message_id']
            for message in pending_messages
        ]

        messages_that_changed_ownership = list()

        if len(message_ids) > 0:
            messages_that_changed_ownership = await connection.xclaim(
                name=stream_name,
                groupname=group_name,
                consumername=inbox_name,
                min_idle_time=100,
                message_ids=message_ids
            )


class GroupConsumer:
    """
    Encapsulates logic and attributes related to a stream group within redis and the application's ability
    to communicate through it
    """
    def __init__(
        self,
        connection: asyncio.Redis,
        stream_name: str,
        group_name: str,
        consumer_name: str = None
    ):
        """
        Constructor

        Args:
            connection: The connection to the Redis instance
            stream_name: The name of the stream that the group should exist upon
            group_name: The name of the group that the consumer will exist within
            consumer_name: An optional hardcoded name for the consumer
        """
        self.__connection = connection
        self.__stream_name = stream_name
        self.__group_name = group_name
        self.__active = False
        self.__last_processed_message = None

        # TODO: Would it make more sense to name the consumer after the application instance
        #  and not just a random identifier?
        self.__consumer_name = consumer_name or f"{group_name}:{generate_identifier(length=4)}"

    @property
    def is_active(self) -> bool:
        """
        Whether the consumer is actively connected to a redis stream and group
        """
        return self.__active

    @property
    def stream_name(self) -> str:
        """
        The name of the stream the consumer is connected to
        """
        return self.__stream_name

    @property
    def group_name(self) -> str:
        """
        The name of the group that the consumer is a member of
        """
        return self.__group_name

    @property
    def consumer_name(self) -> str:
        """
        The unique name for this consumer
        """
        return self.__consumer_name

    @property
    def last_processed_message(self) -> typing.Optional[str]:
        """
        The ID of the last message that was read by the consumer
        """
        return self.__last_processed_message

    @property
    def connection(self) -> asyncio.Redis:
        """
        The connection to the redis instance
        """
        return self.__connection

    async def create_consumer(self):
        """
        Create a new consumer within the redis instance
        """
        # Lock the group to ensure that other instances of the application cannot hinder this creation process
        with create_lock(connection=self.connection, stream_name=self.stream_name, group_name=self.group_name):

            # First ensure that the group exists
            # If there's no key, there's no stream and if there's no stream there's no group
            stream_exists = await self.connection.exists(self.stream_name)

            # If it was determined that the stream is present, check to see if the group is present
            if stream_exists:
                # Search for a group within the stream
                matching_group = [
                    group
                    for group in await self.__connection.xinfo_groups(self.stream_name)
                    if group.get("name") == self.group_name.encode()
                ]

                # If there isn't a matching group, we need to create one
                create_group = len(matching_group) == 0
            else:
                # If the stream doesn't exist, we need to create a group to register the stream
                create_group = True

            if create_group:
                try:
                    await self.connection.xgroup_create(name=self.stream_name, groupname=self.group_name, mkstream=True)

                    # Create the inbox - this will be used to store messages that haven't completed processing
                    # within the group
                    await self.connection.xgroup_createconsumer(
                        name=self.stream_name,
                        groupname=self.group_name,
                        consumername=settings.consumer_inbox_name
                    )
                except asyncio.ResponseError as response_error:
                    if "BUSYGROUP" not in str(response_error):
                        raise
                    # This stream and group got created, so there's no need to worry

            # Now that we're sure there is a group, go ahead and create the consumer

            # We stay locked to ensure that the group is still present for when the consumer is to be constructed
            #   There's a slight risk that an external entity might destroy the group in the VERY brief moment
            #   between creating the group and creating the consumer since there aren't any consumers in the group
            await self.connection.xgroup_createconsumer(
                name=self.stream_name,
                groupname=self.group_name,
                consumername=self.consumer_name
            )

        self.__active = True

    async def read(self, block_ms: int = None) -> typing.Mapping[str, typing.Dict[str, str]]:
        """
        Read data from the stream into the group and assign it to the consumer

        Example:
            >>> read_data = self.read()
            {"channel_name": {"message-id-0": {"value1": 1, "value2": 2}, "message-id-1": {"value1": 3, "value2": 4}}}

        Args:
            block_ms: The number of milliseconds to wait for a response

        Returns:
            The data that was read organized into an easy-to-read structure
        """
        # No need to lock - redis handles the first-come-first-serve process for acquiring messages
        return await read_from_stream(
            connection=self.connection,
            stream=self.stream_name,
            group=self.group_name,
            consumer=self.consumer_name,
            block_ms=block_ms
        )

    async def remove_consumer(self):
        """
        Remove the consumer from the group attached to the stream
        """
        # No need to block - there shouldn't be any other similar consumers
        try:
            # Make sure to move all currently owned messages back to the inbox so that new instances may read them
            await transfer_messages_to_inbox(
                connection=self.__connection,
                stream_name=self.stream_name,
                group_name=self.group_name,
                source_consumer=self.consumer_name
            )
        except Exception as exception:
            logging.error(
                "Could not move all pending messages from a closing redis consumer to a stand in inbox",
                exception
            )

        # Now remove the consumer from the group
        await self.__connection.xgroup_delconsumer(
            name=self.stream_name,
            groupname=self.group_name,
            consumername=self.consumer_name
        )

        # TODO: It might be worth checking to see if the group should go ahead and be removed here
        self.__active = False

    async def mark_message_processed(self, message_id: typing.Union[str, bytes]) -> bool:
        """
        Set the given message as processed and kick it out of the group so that processing on it may conclude

        Args:
            message_id: The ID of the message to kick out of the group

        Returns:
            Whether the marking completed successfully
        """
        # No need to lock - this will be contained within the context of this consumer
        marked_as_complete = await mark_message_as_complete(
            connection=self.connection,
            stream_name=self.stream_name,
            group_name=self.group_name,
            consumer_name=self.consumer_name,
            message_id=message_id
        )

        # If the message was successfully marked as complete, we can inform this consumer that this was the
        # last message to be processed by this consumer
        if marked_as_complete:
            self.__last_processed_message = message_id

        return marked_as_complete

    async def give_up_message(self, message_id: typing.Union[str, bytes], *, give_to: str = None):
        """
        Release the message to another consumer for processing. Hands the message back to the inbox unless
        specified otherwise

        Args:
            message_id: The ID of the message to release
            give_to: Whom to hand the message off to. The message is given to the inbox if not given

        Returns:

        """
        return await return_message_to_inbox(
            connection=self.connection,
            message_id=message_id,
            stream_name=self.stream_name,
            group_name=self.group_name,
            give_to=give_to
        )

    async def __aenter__(self):
        await self.create_consumer()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.remove_consumer()

    def __str__(self):
        return f"{self.group_name}.{self.consumer_name}: "


async def return_message_to_inbox(
    connection: asyncio.Redis,
    message_id: typing.Union[str, bytes],
    stream_name: str,
    group_name: str,
    give_to: str = None
):
    if give_to is None:
        give_to = settings.consumer_inbox_name

    return await connection.xclaim(
        name=stream_name,
        groupname=group_name,
        consumername=give_to,
        min_idle_time=0,
        message_ids=[message_id]
    )


def get_redis_connection_from_configuration(configuration: RedisConfiguration) -> asyncio.Redis:
    if configuration is None:
        return asyncio.Redis()

    return configuration.connect()


def get_id_from_message(message: typing.Tuple[bytes, typing.Dict[bytes, bytes]]) -> bytes:
    if len(message) < 2 or not isinstance(message, tuple):
        logging.error(f"The input data used to get message ids ({str(message)}) was incorrect. Prepare for a failure. ")
    message_id, data = message
    return message_id


def organize_stream_messages(
    incoming_messages: STREAMS
) -> typing.Mapping[str, typing.Dict[str, typing.Dict]]:
    return {
        stream.decode() if isinstance(stream, bytes) else stream: organize_messages(messages)
        for stream, messages in incoming_messages
    }


def organize_messages(messages: typing.Iterable[typing.Tuple[bytes, dict]]) -> typing.Dict:
    organized_messages = OrderedDict()

    for message_id, payload in sorted(messages, key=get_id_from_message):
        key = message_id.decode() if isinstance(message_id, bytes) else message_id
        value = payload.decode() if isinstance(payload, bytes) else payload
        organized_messages[key] = value

    return organized_messages


async def read_from_stream(
    connection: asyncio.Redis,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    message_id: str = None,
    block_ms: int = None
) -> typing.Mapping[str, typing.Dict]:
    if message_id is None:
        message_id = ">"

    while True:
        if block_ms is None:
            block_ms = DEFAULT_BLOCK_MILLISECONDS

        messages = await get_dead_messages(
            connection=connection,
            stream=stream,
            group=group,
            consumer=consumer
        )

        if len(messages) > 0:
            return messages

        incoming_messages: STREAMS = await connection.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: message_id},
            block=block_ms
        )

        messages = organize_stream_messages(incoming_messages)

        messages_to_return = messages.get(stream if isinstance(stream, str) else stream.decode())

        if messages_to_return is not None and len(messages_to_return) > 0:
            return messages_to_return

        await sleep(1)


async def get_messages_from_inbox(
    connection: asyncio.Redis,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    inbox: str = None
) -> typing.Mapping[str, typing.Dict]:
    if inbox is None:
        inbox = settings.consumer_inbox_name

    with create_lock(connection=connection, stream_name=stream, group_name=group):
        raw_message_information: typing.List[typing.Dict] = await connection.xpending_range(
            name=stream,
            groupname=group,
            min="-",
            max="+",
            count=999,
            consumername=inbox
        )

        message_ids = [
            message_information['message_id']
            for message_information in raw_message_information
        ]

        if len(message_ids) == 0:
            return dict()

        raw_messages = await connection.xclaim(
            name=stream,
            groupname=group,
            consumername=consumer,
            min_idle_time=0,
            message_ids=message_ids
        )

    return organize_messages(raw_messages)


async def get_idle_messages(
    connection: asyncio.Redis,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    exclude: typing.Sequence[typing.Union[bytes, str]] = None,
    idle_time: int = None
) -> typing.Mapping[str, typing.Dict]:
    if idle_time is None:
        idle_time = settings.max_idle_time

    if exclude is None:
        exclude = list()
    else:
        exclude = [
            entry.encode() if isinstance(entry, str) else entry
            for entry in exclude
        ]

    # TODO: Start out by checking for any sort of messages that belong exclusively to this consumer. While the future check for pending messages MAY find the messages to process, start with this consumer since they w

    with create_lock(connection=connection, stream_name=stream, group_name=group):
        stale_messages_information = await connection.xpending_range(
            name=stream,
            groupname=group,
            min="-",
            max="+",
            count=99,
            idle=idle_time
        )

        message_ids_to_claim = [
            information['message_id']
            for information in stale_messages_information
            if information['message_id'] not in exclude
        ]

        if len(message_ids_to_claim) == 0:
            return dict()

        newly_claimed_messages = await connection.xclaim(
            name=stream,
            groupname=group,
            consumername=consumer,
            min_idle_time=idle_time,
            message_ids=message_ids_to_claim
        )

    return organize_messages(newly_claimed_messages)


async def get_dead_messages(
    connection: asyncio.Redis,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    inbox: str = None
) -> typing.Mapping[str, typing.Dict]:
    # Locking is performed by functions
    # First, check the inbox for messages
    messages = await get_messages_from_inbox(
        connection=connection,
        stream=stream,
        group=group,
        consumer=consumer,
        inbox=inbox
    )

    if len(messages) > 0:
        return messages

    return await get_idle_messages(
        connection=connection,
        stream=stream,
        group=group,
        consumer=consumer
    )


def default_should_exit(data: dict) -> bool:
    return_values: typing.List[bool] = [
        bool(value)
        for key, value in data.items()
        if key.lower() == "return"
    ]
    return True in return_values


async def get_all_consumers(connection: asyncio.Redis, stream_name: str, group_name: str):
    consumers: typing.Sequence[dict] = await connection.xinfo_consumers(name=stream_name, groupname=group_name)
    return [
        {
            key: value.decode() if isinstance(value, bytes) else value
            for key, value in consumer.items()
        }
        for consumer in consumers
    ]


async def apply_message_to_all(connection: asyncio.Redis, stream_name: str, group_name: str, message_id: str):
    consumers: typing.Sequence[dict] = await get_all_consumers(
        connection=connection,
        stream_name=stream_name,
        group_name=group_name
    )

    key = f"{group_name}:{message_id}"
    entries: typing.Dict[str, bool] = dict()

    with create_lock(connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        for consumer in consumers:
            await connection.hsetnx(name=key, key=consumer['name'], value=int(False))
            entries[consumer['name']] = is_true(await connection.hget(name=key, key=consumer['name']))

    return entries


async def message_is_applied_to_all(
    connection: asyncio.Redis,
    stream_name: str,
    group_name: str,
    message_id: str
) -> bool:
    key = f"{group_name}:{message_id}"
    with create_lock(connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        return is_true(await connection.exists(key))


async def get_universal_message_status(
    connection: asyncio.Redis,
    stream_name: str,
    group_name: str,
    message_id: str
) -> typing.Dict[str, bool]:
    key = f"{group_name}:{message_id}"

    with create_lock(connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        status = {
            key.decode() if isinstance(key, bytes) else key: is_true(value)
            for key, value in (await connection.hgetall(name=key)).items()
        }

    return status


async def mark_message_as_complete(
    connection: asyncio.Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    message_id: str
) -> bool:
    """
    Mark the processing of this particular message by this particular consumer as complete

    If there are records of the message id needing to be consumed by other consumers this will release

    Args:
        connection:
        stream_name:
        group_name:
        consumer_name:
        message_id:

    Returns:

    """
    key = f"{group_name}:{message_id}"

    with create_lock(connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        await connection.hset(name=key, key=consumer_name, value=int(True))

        number_of_incomplete_processes = sum(
            1
            for complete in (await connection.hgetall(key)).values()
            if not is_true(complete)
        )

        if number_of_incomplete_processes == 0:
            await connection.delete(key)
            await connection.xack(stream_name, group_name, message_id)
            return True
        else:
            await return_message_to_inbox(
                connection=connection,
                message_id=message_id,
                stream_name=stream_name,
                group_name=group_name
            )
            return False