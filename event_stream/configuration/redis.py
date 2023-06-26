"""
Defines the configuration model for how to connect to redis
"""
import typing
import os

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from redis.asyncio import Redis

from event_stream.utilities.common import get_environment_variable
from event_stream.configuration.parts import PasswordEnabled
from event_stream.configuration.ssl import SSLConfiguration


class RedisConfiguration(BaseModel, PasswordEnabled):
    """
    Represents the settings needed to connect to a Redis instance and a helpful function used to create a
    connection to it
    """
    host: str = Field(
        default="127.0.0.1",
        description="The web address of the machine hosting the redis instance"
    )
    port: typing.Optional[typing.Union[int, str]] = Field(
        default=6379,
        description="The port on the redis host to connect to"
    )
    db: typing.Optional[int] = Field(
        default=0,
        description="The redis schema/db to communicate with"
    )
    username: typing.Optional[str] = Field(
        default=None,
        description="The name of the user to connect to the redis instance as"
    )

    ssl_configuration: typing.Optional[SSLConfiguration] = None

    @classmethod
    @validator('host', 'port', 'db', 'username')
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value

    def connect(self) -> Redis:
        additional_parameters = dict()

        if self.ssl_configuration is not None:
            additional_parameters['ssl'] = True

            if self.ssl_configuration.ca_file:
                additional_parameters['ssl_certfile'] = self.ssl_configuration.ca_file

            if self.ssl_configuration.key_file:
                additional_parameters['ssl_keyfile'] = self.ssl_configuration.key_file

            if self.ssl_configuration.ca_path:
                additional_parameters['ssl_ca_path'] = self.ssl_configuration.ca_path

            if self.ssl_configuration.password:
                additional_parameters['ssl_password'] = self.ssl_configuration.get_password()

            if self.ssl_configuration.ca_certs:
                additional_parameters['ssl_ca_certs'] = self.ssl_configuration.ca_certs

        return Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            username=self.username,
            password=self.get_password(),
            **additional_parameters
        )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.host.startswith("$"):
            self.host = get_environment_variable(self.host)

        if isinstance(self.port, str):
            if self.port.startswith("$"):
                pass
            elif len(self.port) < 4:
                pass
            elif self.port.isdigit():
                self.port = int(self.port)
            else:
                raise ValueError(f"'{self.port}' is not a valid port number")