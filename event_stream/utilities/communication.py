"""
@TODO: Put a module wide description here
"""
from __future__ import annotations

import typing
from collections import OrderedDict
from asyncio import sleep
from asyncio import gather
import asyncio as asyncpy
import threading
import multiprocessing
import sys

from typing import (
    Literal,
    Dict,
    Any,
    Sequence,
    List,
    Union
)

import redis as synchronous_redis
import redis.exceptions
from redis import asyncio
from redis.exceptions import NoScriptError
from redis.lock import Lock

import event_stream.system.logging as logging
from event_stream.configuration import RedisConfiguration
from event_stream.utilities.common import is_true
from event_stream.utilities.types import STREAMS
from event_stream.utilities.common import generate_identifier
from event_stream.system import settings
from event_stream.utilities.common import fulfill_method
from event_stream.utilities.types import STREAM_MESSAGES
from event_stream.utilities import common

EXIT_INDICATOR: Literal["END"] = "END"
DEFAULT_BLOCK_MILLISECONDS = 100 * 1000

UNPROCESSED_VALUE = "UNPROCESSED"

T = typing.TypeVar("T")
CT = Union[T, typing.Coroutine[Any, Any, T]]

REDIS_CONNECTION = Union[asyncio.Redis, synchronous_redis.Redis]


class LuaSafeLock(Lock):
    """
    A Redis lock that handles the situation where a lock cannot be unlocked due to missing Lua scripts
    (generally seen when mocking)
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__lock_stack: typing.Optional[List[dict]] = None

    @classmethod
    def lock(
        cls,
        connection: REDIS_CONNECTION,
        stream_name: str,
        group_name: str,
        message_id: str = None,
        connection_type: typing.Type[synchronous_redis.Redis] = None
    ) -> LuaSafeLock:
        if connection_type is None:
            connection_type = synchronous_redis.Redis
        if stream_name in group_name:
            name = group_name
        else:
            name = f"{stream_name}:{group_name}"

        if message_id:
            name += f":{message_id}"

        name += f":LOCK"

        if not isinstance(connection, synchronous_redis.Redis):
            connection = connection_type(**connection.connection_pool.connection_kwargs)

        return cls(redis=connection, name=name, blocking=True)

    def _get_internal_trace(self):
        """
        Returns:
            An identifiable list of dictionaries describing where a lock occurred
        """
        # Don't retrieve data with 'code' or 'lineno' attached - you want to accurately match up an enter with an exit.
        # Including 'code' and 'lineno' will create unique values where 'lineno' differs on both. 'code' will also
        # lead to conflicts due to an exit not having a value of 'with lock:', for instance
        # It also needs to cut off at the last instance of the encountered __enter__ and __exit__.
        # We know an __enter__ matches an __exit__ if the stack prior matches
        return common.get_stack_trace(
            cut_off_at=lambda frame: frame['function'] in ("__enter__", "__aenter__", "__exit__", "__aexit__"),
            exclude_fields=['code', 'lineno']
        )

    def acquire(
        self,
        blocking: bool = None,
        blocking_timeout: typing.Union[int, float] = None,
        token: typing.Union[str, bytes] = None
    ) -> bool:
        """
        Override of the parent's 'acquire' function that provides functionality to enable nested locks.

        Args:
            blocking:
            blocking_timeout:
            token:

        Returns:
            True if the lock is owned by this instance
        """
        if self.locked() and self.owned():
            return True
        # Create a record of the stack prior to the acquire. This should match the stack of the release.
        self.__lock_stack = self._get_internal_trace()
        return super().acquire(blocking, blocking_timeout, token)

    def release(self) -> None:
        """
        Override of the release function to follow up with release logic if the encountered trace matches
        that of the lock. This will allow nested locks to unlock without breaking prior locks
        """
        current_trace = self._get_internal_trace()

        if self.__lock_stack is None or len(self.__lock_stack) == 0:
            raise Exception(f"Cannot unlock {self.name} - there is no recorded stack")
        elif current_trace == self.__lock_stack:
            super().release()
        elif len(current_trace) < len(self.__lock_stack) and self.__lock_stack[:len(current_trace)] == current_trace:
            logging.warning(
                f"The {self.name} lock has been released late - "
                f"unlock within the same context as the locking or risk deadlocks"
            )
            return super().release()
        elif current_trace[:len(self.__lock_stack)] == self.__lock_stack:
            return
        else:
            super().release()

    def do_release(self, expected_token: Union[str, bytes]) -> None:
        try:
            super().do_release(expected_token)
        except (ImportError, NoScriptError, redis.exceptions.LockError):
            # Ignore this error - it means that Lua cannot be invoked and that the commands used to
            # remove the lock must be performed manually
            self.redis.delete(self.name)

        self.__lock_stack = None


def secure_lock(
    stream_name: str,
    group_name: str,
    main_connection: REDIS_CONNECTION,
    message_id: str = None,
    lock_connection: synchronous_redis.Redis = None,
    lock: Lock = None,
    lock_type: typing.Type[LuaSafeLock] = None
) -> LuaSafeLock:
    """
    Ensures that a lock is created if it doesn't exist

    Args:
        stream_name: The stream that forms the base of the lock name
        group_name: The group that forms part of the lock name
        main_connection: The primary connection used by the caller
        message_id: An optional message name to attach to the end of the lock name
        lock_connection: An optional dedicated connection for creating locks
        lock: An optional lock that has already been created. Used if given
        lock_type: The type of `LuaSafeLock` to build if a lock doesn't exist

    Returns:

    """
    if lock_type is None:
        lock_type = LuaSafeLock

    if lock is None and lock_connection is None and main_connection is None:
        raise Exception("A lock cannot be created")
    elif lock is None:
        lock = lock_type.lock(
            connection=lock_connection or main_connection,
            stream_name=stream_name,
            group_name=group_name,
            message_id=message_id
        )

    return lock


async def transfer_messages_to_inbox(
    connection: REDIS_CONNECTION,
    stream_name: str,
    group_name: str,
    source_consumer: str,
    inbox_name: str = None,
    lock_connection: synchronous_redis.Redis = None,
    lock: LuaSafeLock = None
) -> typing.Mapping[str, typing.Mapping[bytes, bytes]]:
    """
    Move all messages from a consumer to its group's inbox

    Args:
        connection: The connection to perform the calls on
        stream_name: The name of the stream that the group belongs on
        group_name: The name of the group that owns the consumer
        source_consumer: The name of the consumer whose messages need to move
        inbox_name: A non-standard name of the inbox to use
        lock_connection: A dedicated connection used for creating locks
        lock: A lock that has already been created

    Returns:
        All messages that have been moved to the inbox
    """
    # Default to the system-wide consumer name for consistency
    if inbox_name is None:
        inbox_name = settings.consumer_inbox_name

    # Ensure that the group is locked so data isn't moved around during reassignment
    with secure_lock(stream_name, group_name, main_connection=connection, lock_connection=lock_connection, lock=lock):
        pending_messages: Sequence[dict] = await fulfill_method(
            connection.xpending_range,
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

        messages_that_changed_ownership = dict()

        if len(message_ids) > 0:
            claimed_messages = await fulfill_method(
                connection.xclaim,
                name=stream_name,
                groupname=group_name,
                consumername=inbox_name,
                min_idle_time=0,
                message_ids=message_ids
            )
            messages_that_changed_ownership = organize_messages(claimed_messages)

        return messages_that_changed_ownership


async def get_consumer_messages(
    connection: REDIS_CONNECTION,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    lock_connection: synchronous_redis.Redis = None
) -> Dict[str, Dict[str, Any]]:
    """
    Get all messages currently assigned to a specific user

    Args:
        connection: A connection used to communicate with the redis instance
        stream_name:  The name of the stream that contains the consumer's data
        group_name: The group that the consumer is assigned to
        consumer_name: The name of the consumer
        lock_connection: An optional connection to used for locking

    Returns:
        A mapping of all the message ids to their payload for a specific consumer
    """
    with secure_lock(stream_name, group_name, lock_connection or connection):
        consumption_records = await fulfill_method(
            connection.xpending_range,
            name=stream_name,
            groupname=group_name,
            consumername=consumer_name,
            min="-",
            max="+",
            count=999999
        )

        messages: Dict[str, typing.Dict[str, Any]] = dict()

        for record in consumption_records:
            message_records = await fulfill_method(
                connection.xrange,
                name=stream_name,
                min=record['message_id'],
                max=record['message_id']
            )

            if not message_records:
                continue

            organized_record = organize_messages(message_records)
            messages.update(organized_record)

        return messages


def connection_is_valid(connection: synchronous_redis.Redis) -> bool:
    try:
        return connection.ping()
    except redis.exceptions.ConnectionError:
        return False


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
        with secure_lock(main_connection=self.connection, stream_name=self.stream_name, group_name=self.group_name):
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
    connection: REDIS_CONNECTION,
    message_id: typing.Union[str, bytes],
    stream_name: str,
    group_name: str,
    give_to: str = None
):
    if give_to is None:
        give_to = settings.consumer_inbox_name

    claimed_data = await fulfill_method(
        connection.xclaim,
        name=stream_name,
        groupname=group_name,
        consumername=give_to,
        min_idle_time=0,
        message_ids=[message_id]
    )

    return organize_messages(claimed_data)


def get_redis_connection_from_configuration(configuration: RedisConfiguration = None) -> asyncio.Redis:
    if configuration is None:
        configuration = RedisConfiguration.default()

    return configuration.connect()


def get_id_from_message(message: typing.Tuple[bytes, typing.Dict[bytes, bytes]]) -> bytes:
    if len(message) < 2 or not isinstance(message, tuple):
        logging.error(f"The input data used to get message ids ({str(message)}) was incorrect. Prepare for a failure. ")
    message_id, data = message
    return message_id


def organize_stream_messages(
    incoming_messages: STREAMS
) -> typing.Mapping[str, typing.Dict[str, typing.Dict]]:
    """
    Convert data from the natural list of tuples of list of tuples of bytes format to a more
    palatable mapping of message ids to their payloads

    Examples:
        >>> data: STREAMS = [
                (b'EVENTS', [(b'2382394823-0', {b'key': b'value'})]),
            ]
        >>> organize_stream_messages(data)
        {
            "EVENTS": {
                "2382394823-0": {
                    "key": "value"
                }
            }
        }

    Args:
        incoming_messages: The data to convert

    Returns:
        A dictionary of stream names mapped to their message ids that are mapped to their payloads
    """
    return {
        stream.decode() if isinstance(stream, bytes) else stream: organize_messages(messages)
        for stream, messages in incoming_messages
    }


def organize_messages(messages: typing.Iterable[typing.Tuple[bytes, dict]]) -> typing.Dict:
    """
    Format data in the STREAM_MESSAGES format into an easier format, that being a dictionary where the key is the
    ID of the message and the value being the payload

    Examples:
        >>> data: STREAM_MESSAGES = [(b'2382394823-0', {b'key': b'value'})]
        >>> organize_messages(data)
        {
            "2382394823-0": {
                "key": "value"
            }
        }

    Args:
        messages: A list of message ids and their payloads

    Returns:
        A dictionary of message ids mapped to payloads
    """
    organized_messages = OrderedDict()

    for message_id, payload in sorted(messages, key=get_id_from_message):
        key = message_id.decode() if isinstance(message_id, bytes) else message_id
        value = payload.decode() if isinstance(payload, bytes) else payload
        organized_messages[key] = value

    return organized_messages


async def read_from_stream(
    connection: REDIS_CONNECTION,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    message_id: str = None,
    block_ms: int = None
) -> typing.Mapping[str, typing.Dict]:
    """
    Read all indicated data from a stream and assign it to the given consumer

    Will try to claim messages from the inbox prior to getting new messages from the stream

    Args:
        connection: The connection used to communicate with redis
        stream: The name of the stream to query
        group: The group to add the message to
        consumer: The consumer that will 'own' the message in the group
        message_id: The exclusive minimum message to retrieve. Defaults to '>' for all messages
        block_ms: The amount of milliseconds to block, waiting for a message to come through

    Returns:
        All retrieved messages
    """
    if message_id is None:
        message_id = ">"

    if block_ms is None:
        block_ms = DEFAULT_BLOCK_MILLISECONDS

    # Loop until at least one message is retrieved. This is where the polling occurs
    while True:
        # First try to get unused messages within the group
        messages = await get_dead_messages(
            connection=connection,
            stream=stream,
            group=group,
            consumer=consumer
        )

        # If unused messages are claimed, retrieve those for processing
        if len(messages) > 0:
            return messages

        # Try to capture new messages from the stream
        incoming_messages: STREAMS = await fulfill_method(
            connection.xreadgroup,
            groupname=group,
            consumername=consumer,
            streams={stream: message_id},
            block=block_ms
        )

        # format the messagges to be easier to query
        messages = organize_stream_messages(incoming_messages)

        # Try to extract all messages that belong to the stream here
        messages_to_return = messages.get(stream if isinstance(stream, str) else stream.decode())

        # If messages from the stream were claimed, return those for processing
        if messages_to_return is not None and len(messages_to_return) > 0:
            return messages_to_return

        # If no messages are found, wait a little bit and try again
        await sleep(1)


async def get_messages_from_inbox(
    connection: REDIS_CONNECTION,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    inbox: str = None
) -> typing.Mapping[str, typing.Dict]:
    if inbox is None:
        inbox = settings.consumer_inbox_name

    with secure_lock(stream_name=stream, group_name=group, main_connection=connection):
        raw_message_information: typing.List[typing.Dict] = await fulfill_method(
            connection.xpending_range,
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

        raw_messages = await fulfill_method(
            connection.xclaim,
            name=stream,
            groupname=group,
            consumername=consumer,
            min_idle_time=0,
            message_ids=message_ids
        )

    return organize_messages(raw_messages)


async def get_idle_messages(
    connection: REDIS_CONNECTION,
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

    # TODO: Start out by checking for any sort of messages that belong exclusively to this consumer. While the future
    #  check for pending messages MAY find the messages to process, start with this consumer since they w

    with secure_lock(stream_name=stream, group_name=group, main_connection=connection):
        stale_messages_information = await fulfill_method(
            connection.xpending_range,
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

        newly_claimed_messages = await fulfill_method(
            connection.xclaim,
            name=stream,
            groupname=group,
            consumername=consumer,
            min_idle_time=idle_time,
            message_ids=message_ids_to_claim
        )

    return organize_messages(newly_claimed_messages)


async def get_dead_messages(
    connection: REDIS_CONNECTION,
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


async def get_all_consumers(connection: REDIS_CONNECTION, stream_name: str, group_name: str) -> Sequence[Dict[str, Any]]:
    consumers: typing.Sequence[dict] = await fulfill_method(
        connection.xinfo_consumers,
        name=stream_name,
        groupname=group_name
    )
    return [
        {
            key: value.decode() if isinstance(value, bytes) else value
            for key, value in consumer.items()
        }
        for consumer in consumers
    ]


async def add_consumer(
    connection: REDIS_CONNECTION,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    lock_connection: synchronous_redis.Redis = None,
    lock: LuaSafeLock = None
) -> typing.Tuple[bool, bool]:
    """
    Adds a consumer to a group for a stream

    Args:
        connection:
        stream_name:
        group_name:
        consumer_name:
        lock_connection:
        lock:

    Returns:
        A 2-tuple of booleans, the first stating whether the consumer was created, 2nd being if the group was created
    """
    consumer_created = False

    lock = secure_lock(
        stream_name=stream_name,
        group_name=group_name,
        main_connection=connection,
        lock_connection=lock_connection,
        lock=lock
    )

    with lock:
        group_created = await add_group(
            connection=connection,
            stream_name=stream_name,
            group_name=group_name,
            lock_connection=lock_connection,
            lock=lock
        )

        try:
            await fulfill_method(
                connection.xgroup_createconsumer,
                name=stream_name,
                groupname=group_name,
                consumername=consumer_name
            )
            consumer_created = True
        except BaseException as response_error:
            if "BUSYGROUP" in str(response_error):
                consumer_created = False
            else:
                raise

        updated_consumer_info: List[Dict] = await fulfill_method(
            connection.xinfo_consumers,
            name=stream_name,
            groupname=group_name
        )

        consumer_names = [
            consumer['name'].decode() if isinstance(consumer['name'], bytes) else consumer['name']
            for consumer in updated_consumer_info
        ]

        if consumer_name not in consumer_names:
            raise Exception(
                f"The '{consumer_name}' consumer for the '{group_name}' group could not "
                f"be attached to the '{stream_name}' stream"
            )

    return consumer_created, group_created


async def add_group(
    connection: REDIS_CONNECTION,
    stream_name: str,
    group_name: str,
    lock_connection: synchronous_redis.Redis = None,
    lock: LuaSafeLock = None
):
    lock = secure_lock(
        stream_name=stream_name,
        group_name=group_name,
        main_connection=lock_connection or connection,
        lock=lock
    )
    with lock:
        try:
            creation_result = await fulfill_method(
                connection.xgroup_create,
                name=stream_name,
                groupname=group_name,
                mkstream=True
            )
        except BaseException as response_error:
            if "BUSYGROUP" in str(response_error):
                creation_result = False
            else:
                raise

        creation_result = is_true(creation_result)

        try:
            await fulfill_method(
                connection.xgroup_createconsumer,
                name=stream_name,
                groupname=group_name,
                consumername=settings.consumer_inbox_name
            )
        except BaseException as response_error:
            if "BUSYGROUP" not in str(response_error):
                raise
                # This stream and group got created, so there's no need to worry

        return creation_result


async def remove_groups(connection: REDIS_CONNECTION, stream_name: str):
    group_names = [
        group['name']
        for group in await fulfill_method(connection.xinfo_groups, name=stream_name)
    ]

    return await gather(
        *[
            fulfill_method(connection.xgroup_destroy, name=stream_name, groupname=group_name)
            for group_name in group_names
        ]
    )


async def remove_stream(connection: REDIS_CONNECTION, stream_name: str):
    await remove_groups(connection, stream_name)
    await fulfill_method(connection.delete, stream_name)


async def add_record_to_consumer(
    connection: REDIS_CONNECTION,
    group_name: str,
    consumer_name: str,
    message_id: str,
    record: dict
):
    name = f"{group_name}:{message_id}"
    await fulfill_method(connection.hsetnx, name=name, key=consumer_name, value=int(False))
    record[consumer_name] = is_true(await fulfill_method(connection.hget, name=name, key=consumer_name))


async def apply_message_to_all(connection: REDIS_CONNECTION, stream_name: str, group_name: str, message_id: str):
    entries: typing.Dict[str, bool] = dict()

    with secure_lock(stream_name=stream_name, group_name=group_name, main_connection=connection, message_id=message_id):
        consumers: typing.Sequence[dict] = await get_all_consumers(
            connection=connection,
            stream_name=stream_name,
            group_name=group_name
        )

        await gather(
            *[
                add_record_to_consumer(
                    connection=connection,
                    group_name=group_name,
                    consumer_name=consumer['name'],
                    message_id=message_id,
                    record=entries
                )
                for consumer in consumers
            ]
        )

    return entries


async def message_is_applied_to_all(
    connection: REDIS_CONNECTION,
    stream_name: str,
    group_name: str,
    message_id: str
) -> bool:
    key = f"{group_name}:{message_id}"
    with secure_lock(main_connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        return is_true(await fulfill_method(connection.exists, key))


async def get_universal_message_status(
    connection: REDIS_CONNECTION,
    stream_name: str,
    group_name: str,
    message_id: str
) -> typing.Dict[str, bool]:
    key = f"{group_name}:{message_id}"

    with secure_lock(main_connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        status = {
            key.decode() if isinstance(key, bytes) else key: is_true(value)
            for key, value in (await fulfill_method(connection.hgetall, name=key)).items()
        }

    return status


async def mark_message_as_complete(
    connection: REDIS_CONNECTION,
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
        True if the record is truly removed, False if it was just moved to the inbox
    """
    key = f"{group_name}:{message_id}"

    with secure_lock(main_connection=connection, stream_name=stream_name, group_name=group_name, message_id=message_id):
        await fulfill_method(connection.hset, name=key, key=consumer_name, value=int(True))

        number_of_incomplete_processes = sum(
            1
            for complete in (await fulfill_method(connection.hgetall, key)).values()
            if not is_true(complete)
        )

        if number_of_incomplete_processes == 0:
            await fulfill_method(connection.delete, key)
            await fulfill_method(connection.xack, stream_name, group_name, message_id)
            return True
        else:
            await return_message_to_inbox(
                connection=connection,
                message_id=message_id,
                stream_name=stream_name,
                group_name=group_name
            )
            return False