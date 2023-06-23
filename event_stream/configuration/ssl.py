"""
Defines settings that may be used to define how SSL should be handled
"""
import typing
import os

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from .parts import PasswordEnabled


class SSLConfiguration(BaseModel, PasswordEnabled):
    """
    Configuration for SSL Protection
    """
    ca_path: typing.Optional[str] = Field(
        default=None,
        description="The path to a directory containing several CA certificates in PEM format. Defaults to None."
    )
    ca_file: typing.Optional[str] = Field(
        default=None,
        description="Path to an ssl certificate. Defaults to None"
    )
    key_file: typing.Optional[str] = Field(
        default=None,
        description="Path to an ssl private key. Defaults to None"
    )
    ca_certs: typing.Optional[str] = Field(
        default=None,
        description="The path to a file of concatenated CA certificates in PEM format. Defaults to None."
    )

    @validator('*', pre=True)
    def _assign_environment_variables(cls, value):
        if isinstance(value, str) and value.startswith("$"):
            value = os.environ.get(value[1:])

        return value