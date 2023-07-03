"""
Fake out a configuration for redis where the connection generated is to a fake redis instance
"""
from __future__ import annotations

import typing

from fakeredis.aioredis import FakeRedis as FakeAsyncRedis
from fakeredis import FakeRedis

from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from event_stream.configuration.redis import RedisConfiguration
from event_stream.utilities import communication

class FakeRedisConfiguration(RedisConfiguration):
    def connect(self) -> Redis:
        return Redis()


def get_async_connection(logical_database: int = None) -> typing.Union[FakeAsyncRedis, AsyncRedis]:
    if logical_database is None:
        logical_database = 5

    connection = Redis(db=logical_database)

    if communication.connection_is_valid(connection):
        return AsyncRedis(**connection.connection_pool.connection_kwargs)

    return FakeAsyncRedis(db=logical_database)


def get_connection(logical_database: int = None) -> typing.Union[FakeRedis, Redis]:
    if logical_database is None:
        logical_database = 5

    connection = Redis(db=logical_database)

    if communication.connection_is_valid(connection):
        return connection

    return FakeRedis(db=logical_database)
