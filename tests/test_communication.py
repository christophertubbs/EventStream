"""
@TODO: Put a module wide description here
"""
from __future__ import annotations

import dataclasses
import typing

from asyncio import gather
from asyncio import sleep

import unittest

from fakeredis.aioredis import FakeRedis as AsyncRedis
from fakeredis import FakeRedis
from redis import asyncio
from redis import Redis

from event_stream.utilities import communication
from event_stream.utilities.communication import GroupConsumer
from event_stream.utilities import common
from event_stream.system import settings
from tests.mocks import get_connection
from tests.mocks import get_async_connection
from utilities.types import STREAMS

STREAM_NAME = "UNITTEST"
GROUP_NAME = f"{STREAM_NAME}:Group"
CONSUMER_NAME = f"{GROUP_NAME}:Consumer"
APPLICATION_NAME = "UnitTest"
LISTENER_NAME = "Test"


class StreamData:
    def __init__(self):
        self.__stream_name = common.generate_identifier(length=5)

    @property
    def stream_name(self) -> str:
        return self.__stream_name

    @property
    def group_name(self) -> str:
        return f"{self.stream_name}:Group"

    @property
    def consumer_name(self) -> str:
        return f"{self.group_name}:Consumer"

    @property
    def competing_consumer_name(self) -> str:
        return f"{self.group_name}:Competitor"

    @property
    def consumer_names(self) -> typing.Set[str]:
        return {
            self.consumer_name,
            self.competing_consumer_name
        }



class TestCommunication(unittest.IsolatedAsyncioTestCase):
    async_connection: typing.Union[AsyncRedis, asyncio.Redis]
    connection: typing.Union[Redis, FakeRedis]
    consumer: GroupConsumer
    streams: typing.Set[str]

    def setUp(self) -> None:
        self.async_connection = get_async_connection()
        self.connection = get_connection()
        self.consumer = GroupConsumer(
            connection=self.async_connection,
            stream_name=STREAM_NAME,
            group_name=GROUP_NAME
        )
        self.streams = set()

    async def asyncTearDown(self) -> None:
        await gather(
            *[
                communication.remove_stream(connection=self.async_connection, stream_name=stream_name)
                for stream_name in self.streams
            ]
        )

    def get_stream_name(self) -> str:
        stream_name = common.generate_identifier(length=5)
        self.streams.add(stream_name)
        return stream_name

    async def get_stream(self) -> StreamData:
        stream = StreamData()
        added_consumer = await communication.add_consumer(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            consumer_name=stream.consumer_name
        )
        added_competitor = await communication.add_consumer(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            consumer_name=stream.competing_consumer_name
        )
        self.streams.add(stream.stream_name)

        existing_groups: typing.Sequence[dict] = await self.async_connection.xinfo_groups(name=stream.stream_name)

        self.assertEqual(len(existing_groups), 1)
        self.assertEqual(existing_groups[0]['name'].decode(), stream.group_name)
        self.assertEqual(existing_groups[0]['consumers'], 3)
        self.assertEqual(existing_groups[0]['pending'], 0)
        return stream

    async def test_create_lock(self):
        lock = communication.secure_lock(
            main_connection=self.connection,
            stream_name=STREAM_NAME,
            group_name=GROUP_NAME,
            message_id="example"
        )
        self.assertIsNone(self.connection.get(lock.name))

        with lock:
            self.assertIsNotNone(self.connection.get(lock.name))

        self.assertIsNone(self.connection.get(lock.name))

        lock.acquire()
        self.assertIsNotNone(self.connection.get(lock.name))

        lock.release()

    async def insert_messages(self, stream_name: str) -> dict:

        data: typing.Dict[str, typing.Dict[bytes, bytes]] = dict()

        for _ in range(5):
            key = common.generate_identifier(length=8).encode()
            value = common.generate_identifier(length=4, group_count=2, separator="-").encode()
            input_value = {key: value}
            message_id: bytes = await self.async_connection.xadd(stream_name, {key: value})
            data[message_id.decode()] = input_value

        return data

    async def claim_messages(self, stream_name: str, group_name: str, consumer_name: str) -> typing.Mapping:
        messages_read: STREAMS = await self.async_connection.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: ">"}
        )

        return communication.organize_stream_messages(messages_read)

    async def test_transfer_to_inbox(self):
        stream = await self.get_stream()

        data: typing.Dict[str, typing.Dict[bytes, bytes]] = await self.insert_messages(
            stream_name=stream.stream_name
        )

        # Add data to consumer prior to transferring it
        messages_read: STREAMS = await self.async_connection.xreadgroup(
            groupname=stream.group_name,
            consumername=stream.consumer_name,
            streams={stream.stream_name: ">"}
        )

        organized_messages_read = communication.organize_stream_messages(messages_read)

        messages: STREAMS = await self.async_connection.xread({stream.stream_name: 0})
        organized_messages = communication.organize_stream_messages(messages)

        self.assertEqual(organized_messages, organized_messages_read)

        self.assertEqual(len(organized_messages), 1)
        self.assertIn(stream.stream_name, organized_messages)

        self.assertEqual(data, organized_messages[stream.stream_name])

        changed_messages = await communication.transfer_messages_to_inbox(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            source_consumer=stream.consumer_name,
            lock_connection=self.connection
        )

        transferred_messages = await communication.get_consumer_messages(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            consumer_name=settings.consumer_inbox_name,
            lock_connection=self.connection
        )

        self.assertEqual(changed_messages, transferred_messages)

        pending_inbox_messages = await self.async_connection.xpending_range(
            name=stream.stream_name,
            groupname=stream.group_name,
            consumername=settings.consumer_inbox_name,
            min="-",
            max="+",
            count=99
        )

        inbox_message_ids = {
            message['message_id'].decode()
            for message in pending_inbox_messages
        }

        self.assertEqual(len(transferred_messages), len(pending_inbox_messages))

        for message_id in inbox_message_ids:
            self.assertIn(message_id, transferred_messages)

        consumer_messages = await self.async_connection.xpending_range(
            name=stream.stream_name,
            groupname=stream.group_name,
            consumername=stream.consumer_name,
            min="-",
            max="+",
            count=99
        )
        self.assertEqual(len(consumer_messages), 0)

    async def test_return_message_to_inbox(self):
        stream = await self.get_stream()

        data: typing.Dict[str, typing.Dict[bytes, bytes]] = await self.insert_messages(
            stream_name=stream.stream_name
        )

        popped_key, popped_value = data.popitem()

        await self.claim_messages(
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            consumer_name=stream.consumer_name
        )

        claimed_messages = await communication.return_message_to_inbox(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            message_id=popped_key
        )

        consumer_messages = await communication.get_consumer_messages(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            consumer_name=stream.consumer_name,
            lock_connection=self.connection
        )

        self.assertEqual(claimed_messages, {popped_key: popped_value})
        self.assertEqual(consumer_messages, data)

    async def test_get_redis_connection_from_configuration(self):
        self.assertTrue(isinstance(communication.get_redis_connection_from_configuration(), asyncio.Redis))

    async def test_get_id_from_message(self):
        message = (b'1687976170774-0', {b'2b76bcF6': b'D5FF-310c'})
        message_id = communication.get_id_from_message(message)
        self.assertEqual(message_id, b'1687976170774-0')

    async def test_organize_stream_messages(self):
        stream_messages: STREAMS = [
            (
                b'3bCE0',
                [
                    (b'1687976170774-0', {b'2b76bcF6': b'D5FF-310c'}),
                    (b'1687976170775-0', {b'Dea37CD7': b'ce7b-22C9'}),
                    (b'1687976170775-1', {b'6d01239b': b'9f40-cF7B'}),
                    (b'1687976170776-0', {b'f3FBA4D5': b'AFF9-32fE'}),
                    (b'1687976170777-0', {b'Eba1Ca56': b'fEc9-6C7E'})
                ]
            )
        ]

        expected_messages = {
            "3bCE0": {
                '1687976170774-0': {b'2b76bcF6': b'D5FF-310c'},
                '1687976170775-0': {b'Dea37CD7': b'ce7b-22C9'},
                '1687976170775-1': {b'6d01239b': b'9f40-cF7B'},
                '1687976170776-0': {b'f3FBA4D5': b'AFF9-32fE'},
                '1687976170777-0': {b'Eba1Ca56': b'fEc9-6C7E'}
            }
        }

        organized_stream_messages = communication.organize_stream_messages(stream_messages)

        self.assertEqual(expected_messages, organized_stream_messages)

    async def test_organize_messages(self):
        messages = [
            (b'1687976170774-0', {b'2b76bcF6': b'D5FF-310c'}),
            (b'1687976170775-0', {b'Dea37CD7': b'ce7b-22C9'}),
            (b'1687976170775-1', {b'6d01239b': b'9f40-cF7B'}),
            (b'1687976170776-0', {b'f3FBA4D5': b'AFF9-32fE'}),
            (b'1687976170777-0', {b'Eba1Ca56': b'fEc9-6C7E'})
        ]

        expected_messages = {
            '1687976170774-0': {b'2b76bcF6': b'D5FF-310c'},
            '1687976170775-0': {b'Dea37CD7': b'ce7b-22C9'},
            '1687976170775-1': {b'6d01239b': b'9f40-cF7B'},
            '1687976170776-0': {b'f3FBA4D5': b'AFF9-32fE'},
            '1687976170777-0': {b'Eba1Ca56': b'fEc9-6C7E'}
        }

        organized_messages = communication.organize_messages(messages)

        self.assertEqual(expected_messages, organized_messages)

    async def test_read_from_stream(self):
        stream = await self.get_stream()

        data: typing.Dict[str, typing.Dict[bytes, bytes]] = await self.insert_messages(
            stream_name=stream.stream_name
        )

        read_data = await communication.read_from_stream(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.consumer_name
        )

        self.assertEqual(data, read_data)

    async def test_get_messages_from_inbox(self):
        stream = await self.get_stream()

        original_data: typing.Dict[str, typing.Dict[bytes, bytes]] = await self.insert_messages(
            stream.stream_name
        )

        consumer_data = await communication.read_from_stream(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.consumer_name
        )

        self.assertEqual(original_data, consumer_data)

        inbox_messages = await communication.transfer_messages_to_inbox(
            connection=self.async_connection,
            stream_name=stream.stream_name,
            group_name=stream.group_name,
            source_consumer=stream.consumer_name
        )

        self.assertEqual(consumer_data, inbox_messages)

        claimed_messages = await communication.get_messages_from_inbox(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.consumer_name
        )

        self.assertEqual(consumer_data, claimed_messages)

    async def test_get_idle_messages(self):
        stream = await self.get_stream()

        inserted_messages = await self.insert_messages(
            stream_name=stream.stream_name
        )

        claimed_messages = await communication.read_from_stream(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.consumer_name
        )

        self.assertEqual(inserted_messages, claimed_messages)

        desired_idle_seconds = 3

        wait_time = desired_idle_seconds * 1.5

        print(f"Waiting for {wait_time}s to generate idle messages")
        await sleep(wait_time)

        won_messages = await communication.get_idle_messages(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.competing_consumer_name,
            idle_time=desired_idle_seconds * 1_000
        )

        self.assertEqual(claimed_messages, won_messages)

    async def test_get_dead_messages(self):
        stream = await self.get_stream()

        inserted_messages = await self.insert_messages(
            stream_name=stream.stream_name,
        )

        claimed_messages = await communication.read_from_stream(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.consumer_name
        )

        self.assertEqual(inserted_messages, claimed_messages)

        desired_idle_seconds = 3

        wait_time = desired_idle_seconds * 1.5

        print(f"Waiting for {wait_time}s to generate idle messages")
        await sleep(wait_time)

        won_messages = await communication.get_idle_messages(
            connection=self.async_connection,
            stream=stream.stream_name,
            group=stream.group_name,
            consumer=stream.competing_consumer_name,
            idle_time=desired_idle_seconds * 1_000
        )

        self.assertEqual(claimed_messages, won_messages)

    async def test_get_all_consumers(self):
        stream_data = await self.get_stream()
        all_consumers = await communication.get_all_consumers(
            self.async_connection,
            stream_name=stream_data.stream_name,
            group_name=stream_data.group_name
        )
        pass

    async def test_apply_message_to_all(self):
        pass

    async def test_message_is_applied_to_all(self):
        pass

    async def test_get_universal_message_status(self):
        pass

    async def test_mark_message_as_complete(self):
        pass