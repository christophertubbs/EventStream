"""
@TODO: Put a module wide description here
"""
import typing

from pydantic import SecretStr


class UserMixin:
    username: str
    password: SecretStr
