"""
@TODO: Put a module wide description here
"""
import typing
from collections import OrderedDict
from asyncio import sleep
from uuid import uuid1

from redis import asyncio

from event_stream.configuration import RedisConfiguration
from event_stream.utilities.types import STREAMS


EXIT_INDICATOR: typing.Literal["END"] = "END"
DEFAULT_BLOCK_MILLISECONDS = 100 * 1000


class GroupConsumer:
    def __init__(
        self,
        connection: asyncio.Redis,
        stream_name: str,
        group_name: str,
        consumer_name: str = None
    ):
        self.__connection = connection
        self.__stream_name = stream_name
        self.__group_name = group_name
        self.__active = False
        self.__consumer_name = consumer_name or str(uuid1())

    @property
    def is_active(self) -> bool:
        return self.__active

    @property
    def stream_name(self) -> str:
        return self.__stream_name

    @property
    def group_name(self) -> str:
        return self.__group_name

    @property
    def consumer_name(self) -> str:
        return self.__consumer_name

    @property
    def connection(self) -> asyncio.Redis:
        return self.__connection

    async def create_consumer(self):
        if not await self.connection.exists(self.stream_name):
            try:
                await self.connection.xgroup_create(name=self.stream_name, groupname=self.group_name, mkstream=True)
            except asyncio.ResponseError as response_error:
                if "BUSYGROUP" not in str(response_error):
                    raise
                # This stream and group got created, so there's no need to worry

        matching_group = [
            group
            for group in await self.__connection.xinfo_groups(self.stream_name)
            if group.get("name") == self.group_name.encode()
        ]

        if not matching_group:
            try:
                await self.connection.xgroup_create(name=self.stream_name, groupname=self.group_name, mkstream=True)
            except asyncio.ResponseError as response_error:
                if "BUSYGROUP" not in str(response_error):
                    raise
                # This stream and group got created, so there's no need to worry

        await self.connection.xgroup_createconsumer(
            name=self.stream_name,
            groupname=self.group_name,
            consumername=self.consumer_name
        )

        self.__active = True

    async def read(self, block_ms: int = None) -> typing.Mapping[str, typing.Dict[str, str]]:
        return await read_from_stream(
            connection=self.connection,
            stream=self.stream_name,
            group=self.group_name,
            consumer=self.consumer_name,
            block_ms=block_ms
        )

    async def remove_consumer(self):
        # TODO: xack all of its messages
        await self.__connection.xgroup_delconsumer(
            name=self.stream_name,
            groupname=self.group_name,
            consumername=self.consumer_name
        )

    async def acknowledge_message_processed(self, message_id: typing.Union[str, bytes]):
        return await self.__connection.xack(self.stream_name, self.group_name, message_id)

    async def __aenter__(self):
        await self.create_consumer()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.remove_consumer()

    def __str__(self):
        return f"{self.stream_name}.{self.group_name}.{self.consumer_name}: "


def get_redis_connection_from_configuration(configuration: RedisConfiguration) -> asyncio.Redis:
    if configuration is None:
        return asyncio.Redis()

    additional_parameters = dict()

    if configuration.ssl_configuration is not None:
        additional_parameters['ssl'] = True

        if configuration.ssl_configuration.ca_file:
            additional_parameters['ssl_certfile'] = configuration.ssl_configuration.ca_file

        if configuration.ssl_configuration.key_file:
            additional_parameters['ssl_keyfile'] = configuration.ssl_configuration.key_file

        if configuration.ssl_configuration.ca_path:
            additional_parameters['ssl_ca_path'] = configuration.ssl_configuration.ca_path

        if configuration.ssl_configuration.password:
            additional_parameters['ssl_password'] = configuration.ssl_configuration.get_password()

        if configuration.ssl_configuration.ca_certs:
            additional_parameters['ssl_ca_certs'] = configuration.ssl_configuration.ca_certs

    return asyncio.Redis(
        host=configuration.host,
        port=configuration.port,
        db=configuration.db,
        username=configuration.username,
        password=configuration.get_password(),
        **additional_parameters
    )


def get_id_from_message(message: typing.Tuple[bytes, typing.Dict[bytes, bytes]]) -> bytes:
    message_id, data = message
    return message_id


def organize_stream_messages(
    incoming_messages: STREAMS
) -> typing.Mapping[bytes, typing.Mapping[str, typing.Dict[str, str]]]:
    return {
        stream: OrderedDict({
            message_id.decode(): {
                key.decode(): value.decode()
                for key, value in message_data.items()
            }
            for message_id, message_data in sorted(messages, key=get_id_from_message)
        }) for stream, messages in incoming_messages
    }


async def read_from_stream(
    connection: asyncio.Redis,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    block_ms: int = None
) -> typing.Mapping[str, typing.Dict[str, str]]:
    while True:
        if block_ms is None:
            block_ms = DEFAULT_BLOCK_MILLISECONDS

        incoming_messages: STREAMS = await connection.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            block=block_ms
        )

        messages = organize_stream_messages(incoming_messages)

        messages_to_return = messages.get(stream if isinstance(stream, bytes) else stream.encode())

        if messages_to_return is not None:
            return messages_to_return

        await sleep(1)


async def get_messages_from_pel(
    connection: asyncio.Redis,
    stream: typing.Union[str, bytes],
    group: typing.Union[str, bytes],
    consumer: typing.Union[str, bytes],
    block_ms: int
) -> typing.Dict[str, typing.Dict[str, str]]:
    return dict()


def default_should_exit(data: dict) -> bool:
    return data.get('return', False) or data.get('RETURN', False)


async def read_redis_stream(
        connection: asyncio.Redis,
        stream: typing.Union[str, bytes],
        group: typing.Union[str, bytes],
        consumer: typing.Union[str, bytes],
        block_ms: int,
        should_exit: typing.Callable[[dict], bool] = None
) -> typing.AsyncGenerator:
    if isinstance(group, str):
        group = group.encode()

    if not isinstance(should_exit, typing.Callable):
        should_exit = default_should_exit

    # First look at the PEL for dead messages
    # If no dead messages were found, yield new messages from XREADGROUP
    while True:
        pending_messages = await get_messages_from_pel(
            connection=connection,
            stream=stream,
            group=group,
            consumer=consumer,
            block_ms=block_ms
        )

        for message_id, message in pending_messages.items():
            if should_exit(message):
                return

            yield message

        messages = await read_from_stream(
            connection=connection,
            stream=stream,
            group=group,
            consumer=consumer,
            block_ms=block_ms
        )

        if messages is None or len(messages) == 0:
            continue

        print("Received messages!")

        for message_id, message in messages.items():
            if should_exit(message):
                return

            yield message


class RedisStreamIterator(typing.AsyncIterator[dict]):
    def __init__(
        self,
        connection: asyncio.Redis,
        stream: typing.Union[str, bytes],
        group: typing.Union[str, bytes],
        consumer: typing.Union[str, bytes],
        block_ms: int
    ):
        self.__connection = connection
        self.__stream = stream if isinstance(stream, bytes) else stream.encode()
        self.__group = group if isinstance(group, bytes) else group.encode()
        self.__consumer = consumer if isinstance(consumer, bytes) else consumer.encode()
        self.__block_ms = block_ms
        self.__pending_messages = OrderedDict()
        self.__messages = OrderedDict()

    async def _get_messages_from_pel(self):
        return OrderedDict()

    async def _read_from_stream(self):
        incoming_messages: STREAMS = await self.__connection.xreadgroup(
            groupname=self.__group,
            consumername=self.__consumer,
            streams={self.__stream: ">"},
            block=self.__block_ms
        )

        messages = organize_stream_messages(incoming_messages)

        return messages.get(self.__stream, dict())

    async def __anext__(self) -> dict:
        while True:
            if not self.__messages:
                new_messages = await self._read_from_stream()

                self.__messages.update(
                    new_messages
                )

            if self.__messages:
                return self.__messages.popitem(last=False)[0]

            if not self.__pending_messages:
                self.__pending_messages.update(
                    await self._get_messages_from_pel()
                )

            if self.__pending_messages:
                return self.__pending_messages.popitem(last=False)[0]