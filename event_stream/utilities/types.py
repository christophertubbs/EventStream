"""
@TODO: Put a module wide description here
"""
from __future__ import annotations
import asyncio
import inspect
import typing

from typing import (
    Union,
    Any,
    Set,
    Mapping,
    Tuple,
    Type,
    Optional,
    Dict,
    List,
    SupportsBytes,
    SupportsInt,
    SupportsFloat,
    Protocol,
    Callable,
    runtime_checkable,
    TypeVar
)

from typing_extensions import ParamSpec

from redis.asyncio import Redis

INCLUDE_EXCLUDE_TYPES = Union[Set[Union[int, str]], Mapping[Union[int, str], Any]]

HASHABLE_TYPES = (
    SupportsInt,
    SupportsFloat,
    SupportsBytes,
    str,
    bytes,
    tuple
)


class CodeDesignationProtocol(Protocol):
    module_name: str
    name: str


class RedisConfigurationProtocol(Protocol):
    host: str
    port: Optional[int]
    db: Optional[int]
    username: str

    async def get_connection(self) -> Redis:
        ...


class BusConfigurationProtocol(Protocol):
    name: str
    stream: Optional[str]
    group: str
    handlers: Dict[str, List[CodeDesignationProtocol]]
    redis_configuration: Optional[RedisConfigurationProtocol]

    def get_application_name(self, include_instance: bool = False) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def set_instance_identifier(self, instance_identifier: str):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def get_instance_identifier(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def parent(self):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def set_parent(self, parent):
        ...

    @property
    def group(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def consumer_name(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")


class BusProtocol(Protocol):
    @property
    def configuration(self) -> BusConfigurationProtocol:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def verbose(self) -> bool:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def name(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def launch(self) -> asyncio.Task:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def get_application_name(self) -> str:
        ...

    def get_instance_identifier(self) -> str:
        ...

    def stop_polling(self):
        ...

    def can_make_executive_decisions(self) -> bool:
        ...


@runtime_checkable
class WeightedProtocol(Protocol):
    @classmethod
    def get_weight(cls) -> int:
        ...

    def dict(self) -> dict:
        ...


@runtime_checkable
class MessageProtocol(Protocol):
    def get_event(self) -> str:
        ...

    def get_data(self) -> dict:
        ...

    @classmethod
    def parse(cls, data: typing.Union[str, bytes, typing.Mapping]):
        ...

    def dict(
        self,
        *,
        include: INCLUDE_EXCLUDE_TYPES = None,
        exclude: INCLUDE_EXCLUDE_TYPES = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False
    ) -> dict:
        ...


HANDLER_FUNCTION = Callable[
    [
        Redis,
        BusProtocol,
        MessageProtocol,
        Any,
        ...
    ],
    typing.Optional[MessageProtocol]
]
"""
A function meant to operate as an event handler.
Functions should look like:
    >>> def func(connection: Redis, bus: EventBus, message: Message, **kwargs) -> typing.Optional[Message]
"""


STREAM_MESSAGE = Union[int, Tuple[bytes, Dict[bytes, bytes]]]
STREAM_MESSAGES = List[STREAM_MESSAGE]
STREAMS = List[Tuple[bytes, STREAM_MESSAGES]]

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")

PAYLOAD = Union[Dict[str, str], Dict[bytes, bytes]]


def type_matches_special_form(
    encountered_type: Type,
    expected_type: Union,
    check_origin: bool = None
) -> bool:
    if check_origin is None:
        check_origin = True

    if check_origin:
        origin = typing.get_origin(expected_type)

        if origin is None:
            raise Exception(
                f"`type_matches_special_form` is called incorrectly - the expected type does not identify an origin"
            )

    if typing.get_origin(encountered_type) is not None:
        for member_type in typing.get_args(encountered_type):
            if type_matches_special_form(member_type, expected_type, False):
                return True
        return False

    expected_arguments = typing.get_args(expected_type)

    for expected_argument in expected_arguments:
        if expected_type == Any:
            return True
        argument_origin = typing.get_origin(expected_argument)

        if argument_origin:
            subtype_matches = type_matches_special_form(encountered_type, expected_argument)
            if subtype_matches:
                return True
        else:
            if encountered_type == expected_argument:
                return True

    return False


def enforce_handler(possible_handler: typing.Callable) -> HANDLER_FUNCTION:
    """
    Checks a given function to see if it is a valid event handler

    Throws errors if and when the given function does not match the definition of a handler

    Args:
        possible_handler: The handler to check

    Returns:
        The original handler
    """
    # TODO: This should be able to be tacked onto functions and fail if their signature doesn't comply
    if not isinstance(possible_handler, Callable):
        raise ValueError(
            f"The given handler is not valid - "
            f"{str(possible_handler)} ({type(possible_handler)}) is not a callable object"
        )

    required_parameters, return_type = typing.get_args(HANDLER_FUNCTION)

    signature = inspect.signature(possible_handler)
    function_parameters: List[inspect.Parameter] = [parameter for parameter in signature.parameters.values()]

    function_kwargs = [
        parameter
        for parameter in function_parameters
        if parameter.kind == parameter.VAR_KEYWORD
    ]

    if Ellipsis in required_parameters and not function_kwargs:
        raise ValueError(
            f"The given handler is not valid - "
            f"variable keyword arguments are required and {str(possible_handler)} doesn't accept them."
        )

    for index, required_parameter in enumerate(required_parameters):  # type: int, Type
        if required_parameter == Any:
            continue

        matching_parameter = function_parameters[index] if index < len(function_parameters) else None
        if matching_parameter:
            varying_kinds = (matching_parameter.VAR_POSITIONAL, matching_parameter.VAR_KEYWORD)
        else:
            varying_kinds = tuple()

        parameter_is_variable = matching_parameter is not None and matching_parameter.kind in varying_kinds
        if required_parameter == Ellipsis and (matching_parameter is None or parameter_is_variable):
            break

        matching_parameter = function_parameters[index]
        annotated_type = matching_parameter.annotation

        if matching_parameter.kind in (matching_parameter.VAR_POSITIONAL, matching_parameter.VAR_KEYWORD):
            break

        if annotated_type == matching_parameter.empty:
            continue

        if not (isinstance(annotated_type, required_parameter) or issubclass(annotated_type, required_parameter)):
            raise ValueError(
                f"The given handler is not valid - "
                f"Parameter {index} needs to be a {str(required_parameter)} "
                f"but expects a {str(matching_parameter.annotation)}"
            )

        if typing.get_origin(required_parameter) is not None and not type_matches_special_form(annotated_type, required_parameter):
            raise ValueError(
                f"The given handler is not valid - "
                f"Parameter {index} needs to match {str(required_parameter)} but was a {str(annotated_type)}"
            )

    signature_has_return = signature.return_annotation != signature.empty

    if typing.get_origin(return_type) is None and typing.get_origin(signature.return_annotation) is None:
        return_values_match = isinstance(signature.return_annotation, return_type)
        return_value_is_a_subclass = issubclass(signature.return_annotation, return_type)
    elif typing.get_origin(signature.return_annotation) is None:
        # The return type is a generic and the annotation is not,
        # so this needs to be compared with 'type_matches_special_form'
        return_values_match = isinstance(signature.return_annotation, typing.get_args(return_type))
        return_value_is_a_subclass = issubclass(signature.return_annotation, typing.get_args(return_type))
    elif typing.get_origin(return_type) is None:
        # The annotation is generic but the return type is not. A judgement call needs to be made on whether
        # this is valid. If the given function returns a string or int and the expected return is an int,
        # this is not correct
        return_values_match = False
        return_value_is_a_subclass = False
    else:
        # Both are generic and this requires more care - we want it to be true if both return 'optional[int]'
        # but false if one returns 'Final[int]'
        encountered_origin = typing.get_origin(signature.return_annotation)
        expected_origin = typing.get_origin(return_type)
        if encountered_origin != expected_origin:
            return_values_match = False
            return_value_is_a_subclass = False
        else:
            encountered_arguments = typing.get_args(signature.return_annotation)
            expected_arguments = typing.get_args(return_type)
            return_values_match = False
            return_value_is_a_subclass = False

            for encountered_argument in encountered_arguments:
                if isinstance(encountered_argument, expected_arguments):
                    return_values_match = True
                if issubclass(encountered_argument, expected_arguments):
                    return_value_is_a_subclass = True

    if signature_has_return and not (return_values_match or return_value_is_a_subclass):
        raise ValueError(
            f"The given handler is not valid - it must return some form of {return_type.__name__} "
            f"but the given handler instead returns a {signature.return_annotation.__name__}"
        )

    return possible_handler
