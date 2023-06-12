"""
@TODO: Put a module wide description here
"""
import asyncio
import os
import typing

import redis.asyncio as async_redis
from redis.asyncio.lock import Lock as AsyncRedisLock
from redis.client import Pipeline

from datetime import timedelta

from configuration.bus import EventBusConfiguration
from configuration.bus import update_bus
from event_stream.configuration import EventBusConfigurations
from event_stream.configuration.parts import CodeDesignation
from event_stream.utilities.communication import GroupConsumer

from event_stream.system import logging

from utilities.communication import get_redis_connection_from_configuration
from utilities.common import decode_stream_message
from utilities.common import fulfill_method


MAX_HANDLER_ATTEMPTS = int(os.environ.get("MAX_HANDLER_ATTEMPTS", 5))
KEY_LIFETIME_SECONDS = timedelta(seconds=int(os.environ.get("HANDLER_KEY_LIFETIME_SECONDS", 60 * 60 * 2)))


def create_progress_key(consumer: GroupConsumer, message_id: str):
    return f"{message_id}::{consumer.group_name}::progress"


async def set_and_retrieve_required_handlers(
    consumer: GroupConsumer,
    message_id: str,
    handlers: typing.Sequence[CodeDesignation]
) -> typing.Sequence[CodeDesignation]:
    progress_key = create_progress_key(consumer=consumer, message_id=message_id)
    async with AsyncRedisLock(redis=consumer.connection, name=message_id) as lock:
        with consumer.connection.pipeline(transaction=True) as pipeline:  # type: Pipeline
            for handler in handlers:
                pipeline.hsetnx(progress_key, handler.identifier, 0)
            pipeline.expire(progress_key, KEY_LIFETIME_SECONDS)
            pipeline.hgetall(progress_key)
            handler_ids_and_attempts = await pipeline.execute()

        left_over_handler_ids = [
            handler_id.decode()
            for handler_id, attempts in handler_ids_and_attempts.items()
            if attempts < MAX_HANDLER_ATTEMPTS
        ]

        return [
            handler
            for handler in handlers
            if handler.identifier in left_over_handler_ids
        ]


async def set_progress(consumer: GroupConsumer, message_id: str, handlers: typing.Sequence[CodeDesignation]):
    progress_key = create_progress_key(consumer=consumer, message_id=message_id)
    if not await consumer.connection.exists(progress_key):
        handler_mapping = {
            handler.identifier: 0
            for handler in handlers
        }
        await consumer.connection.hset(name=progress_key, mapping=handler_mapping)
    await consumer.connection.expire(progress_key, KEY_LIFETIME_SECONDS)


async def get_leftover_handler_ids(consumer: GroupConsumer, message_id: str) -> typing.Sequence[str]:
    progress_key = create_progress_key(consumer=consumer, message_id=message_id)

    handler_ids = [
        handler_key
        for handler_key, attempts in await consumer.connection.hgetall(progress_key).items()
        if attempts < MAX_HANDLER_ATTEMPTS
    ]
    await consumer.connection.expire(progress_key, KEY_LIFETIME_SECONDS)
    return handler_ids


async def update_handler_completion(consumer: GroupConsumer, message_id: str, handler: CodeDesignation):
    progress_key = create_progress_key(consumer=consumer, message_id=message_id)

    await consumer.connection.hset(name=progress_key, key=handler.identifier, value=True)
    await consumer.connection.expire(progress_key, KEY_LIFETIME_SECONDS)


async def update_handler_failure(consumer: GroupConsumer, message_id: str, handler: CodeDesignation):
    progress_key = create_progress_key(consumer=consumer, message_id=message_id)

    await consumer.connection.hincrby(name=progress_key, key=handler.identifier, amount=1)
    await consumer.connection.expire(progress_key, KEY_LIFETIME_SECONDS)


async def clear_handler_records(self, consumer: GroupConsumer, message_id: str):
    progress_key = create_progress_key(consumer=consumer, message_id=message_id)

    await consumer.connection.delete(progress_key)
    await consumer.connection.expire(progress_key, KEY_LIFETIME_SECONDS)


class EventBus:
    def __init__(self, bus_configuration: EventBusConfiguration, verbose: bool = False):
        self.__configuration = bus_configuration
        self.__verbose = bool(verbose)
        self.keep_polling = True
        self.can_close = False
        self.current_operation: typing.Optional[asyncio.Task] = None

    def is_allowed_to_close(self):
        return self.can_close

    async def close(self):
        connection: async_redis.Redis = await get_redis_connection_from_configuration(
            self.__configuration.redis_configuration
        )

        pending_messages = connection.xpending(self.__configuration.stream, self.__configuration.get_group())

        await connection.xgroup_destroy(self.__configuration.stream, self.__configuration.get_group())

    @property
    def name(self) -> str:
        return self.__configuration.name

    @property
    def verbose(self) -> bool:
        return self.__verbose

    @property
    def configuration(self) -> EventBusConfiguration:
        return self.__configuration

    def stop_polling(self):
        self.keep_polling = False

    def launch(self) -> asyncio.Task:
        task = asyncio.create_task(self.listen(), name=self.name)
        self.current_operation = task
        return task

    async def process_response(
        self,
        consumer: GroupConsumer,
        handler: CodeDesignation,
        message_id: str,
        result: typing.Any
    ):
        pass

    async def process_message(
        self,
        consumer: GroupConsumer,
        message_id: str,
        payload: typing.Dict[str, typing.Any]
    ):
        event_name = payload.get("event")
        processed = False
        results: typing.List[typing.Hashable] = list()

        if event_name:
            event_handled = False
            event_defined = event_name in self.__configuration.handlers

            for handler in self.__configuration.get_handlers(event_name):
                event_handled = True
                result = None
                result_created = False

                try:
                    result = await fulfill_method(handler, consumer.connection, self, **payload)
                    result_created = True
                    if isinstance(result, typing.Hashable):
                        results.append(result)
                except BaseException as exception:
                    logging.error(str(exception), exception=exception)

                if result_created:
                    try:
                        await self.process_response(
                            consumer=consumer,
                            handler=handler,
                            message_id=message_id,
                            result=result
                        )
                    except BaseException as exception:
                        logging.error(str(exception), exc_info=exception)

                processed = True

            if event_defined and not event_handled:
                logging.warning(
                    f"There were no handlers for the '{event_name}' event."
                )

        else:
            logging.warning(
                f"No event name was passed in message '{message_id}' "
                f"in the '{self.__configuration.stream}' stream"
            )

        if processed:
            await consumer.acknowledge_message_processed(message_id)

    async def listen(self):
        connection: async_redis.Redis = await get_redis_connection_from_configuration(
            self.__configuration.redis_configuration
        )

        self.keep_polling = True

        async with connection:
            if self.verbose:
                logging.info(
                    f"The '{self.__configuration.name}' bus is currently listening for messages on the "
                    f"'{self.__configuration.stream}' channel"
                )

            consumer = GroupConsumer(
                connection=connection,
                stream_name=self.__configuration.stream,
                group_name=self.__configuration.get_group()
            )

            async with consumer:
                while self.keep_polling:
                    # TODO: Place a lock on the returned message or have 'read()' return a lock to ensure that
                    #  other busses' aren't playing with the message
                    messages = await consumer.read()

                    # TODO: Figure out why `consumer.read()` is returning None instead of continuing to
                    #  wait for a message
                    if messages is None:
                        logging.error(f"Something went wrong when reading from the stream - waiting and trying again")
                        await asyncio.sleep(1)
                        continue

                    for message_id, payload in messages.items():
                        payload = decode_stream_message(payload)
                        await self.process_message(consumer, message_id, payload)

                if self.verbose:
                    logging.info(
                        f"The '{self.__configuration.name}' bus is no longer listening for messages"
                    )

    def __str__(self):
        return str(self.configuration)

    def __repr__(self):
        return str(self.configuration)


class MasterBus(EventBus):
    """
    A bus used to control and monitor available busses

    The master bus has a few important operations:

    - close => Ends the processing of all messages across all busses
    - trim => clears out messages beyond a certain point for maintenance
    """
    def __init__(self, all_configurations: EventBusConfigurations, verbose: bool = False):
        self.can_close = True
        master_configuration: EventBusConfiguration = EventBusConfiguration.parse_obj(
            {
                "name": "Master",
                "handlers": {
                    "close": [
                        {
                            "module_name": "event_stream.handlers.master",
                            "name": "close_streams"
                        }
                    ],
                    "trim": [
                        {
                            "module_name": "event_stream.handlers.master",
                            "name": "trim_streams"
                        }
                    ]
                },
                "stream": all_configurations.stream or "MASTER"
            }
        )
        update_bus(
            master_configuration,
            all_configurations.redis_configuration,
            all_configurations.stream,
            all_configurations.application_name
        )
        super().__init__(bus_configuration=master_configuration, verbose=verbose)

    def is_allowed_to_close(self):
        return True