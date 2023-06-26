"""
@TODO: Put a module wide description here
"""
from __future__ import annotations
import asyncio
import inspect
import typing
import importlib
from types import ModuleType

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
    Protocol,
    Callable,
    runtime_checkable,
    TypeVar
)

from typing_extensions import ParamSpec

from redis.asyncio import Redis

T = TypeVar("T")
"""Indicates a general type of object"""

R = TypeVar("R")
"""Indicates a general type of object to be returned"""

P = ParamSpec("P")
"""Indicates *args and **kwargs parameters"""

_IMPORTED_LIBRARIES: typing.Dict[str, ModuleType] = dict()


@runtime_checkable
class ConsumerProtocol(Protocol):

    @property
    def is_active(self) -> bool:
        """
        Whether the consumer is actively connected to a redis stream and group
        """
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def stream_name(self) -> str:
        """
        The name of the stream the consumer is connected to
        """
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def group_name(self) -> str:
        """
        The name of the group that the consumer is a member of
        """
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def consumer_name(self) -> str:
        """
        The unique name for this consumer
        """
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def last_processed_message(self) -> typing.Optional[str]:
        """
        The ID of the last message that was read by the consumer
        """
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def connection(self) -> Redis:
        """
        The connection to the redis instance
        """
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    async def create_consumer(self):
        """
        Create a new consumer within the redis instance
        """
        ...

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
        ...

    async def remove_consumer(self):
        """
        Remove the consumer from the group attached to the stream
        """
        # No need to block - there shouldn't be any other similar consumers
        ...

    async def mark_message_processed(self, message_id: typing.Union[str, bytes]) -> bool:
        """
        Set the given message as processed and kick it out of the group so that processing on it may conclude

        Args:
            message_id: The ID of the message to kick out of the group

        Returns:
            Whether the marking completed successfully
        """
        # No need to lock - this will be contained within the context of this consumer
        ...

    async def give_up_message(self, message_id: typing.Union[str, bytes], *, give_to: str = None):
        """
        Release the message to another consumer for processing. Hands the message back to the inbox unless
        specified otherwise

        Args:
            message_id: The ID of the message to release
            give_to: Whom to hand the message off to. The message is given to the inbox if not given

        Returns:

        """
        ...

    async def __aenter__(self):
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        ...


class DesignationProtocol(Protocol):
    module_name: str
    name: str


class CodeDesignationProtocol(DesignationProtocol, Protocol):
    """Indicates the basic expectations for what a CodeDesignation will contain"""
    kwargs: typing.Optional[typing.Dict[str, typing.Any]]
    message_type: DesignationProtocol

    @property
    def identifier(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def loaded_function(self) -> HANDLER_FUNCTION:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def set_function(self, handler_function: HANDLER_FUNCTION, *, already_checked: bool = None):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def __call__(self, connection: Redis, bus: ReaderProtocol, message: MessageProtocol, **kwargs):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")


class RedisConfigurationProtocol(Protocol):
    host: str
    port: Optional[int]
    db: Optional[int]
    username: str

    def connection(self) -> Redis:
        ...


@runtime_checkable
class ReaderConfigurationProtocol(Protocol):
    name: str
    stream: typing.Optional[str]
    unique: typing.Optional[bool]
    redis_configuration: typing.Optional[RedisConfigurationProtocol]

    @classmethod
    def get_parent_collection_name(cls) -> str:
        ...

    def gather_handler_errors(self) -> typing.Optional[typing.Union[typing.Sequence[str], str]]:
        ...

    def get_tracker_ids(self, event_name: str) -> typing.Iterable[str]:
        ...

    def set_application_name(self, application_name: str):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def get_application_name(self, include_instance: bool = None) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def set_instance_identifier(self, instance_identifier: str):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def get_instance_identifier(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def parent(self):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def set_parent(self, parent):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def group(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def consumer_name(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")


@typing.runtime_checkable
class ReaderProtocol(Protocol):
    @property
    def verbose(self) -> bool:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def name(self) -> str:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def can_make_executive_decisions(self):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    @property
    def configuration(self) -> ReaderConfigurationProtocol:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def stop_polling(self):
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    def launch(self) -> asyncio.Task:
        raise NotImplementedError(f"The {self.__class__.__name__} should not be instantiated")

    async def close(self):
        ...

    async def process_response(
        self,
        consumer: ConsumerProtocol,
        message_id: str,
        result: typing.Any
    ) -> typing.Any:
        ...

    async def process_message(
        self,
        consumer: ConsumerProtocol,
        message_id: str,
        payload: typing.Dict[str, typing.Any]
    ) -> typing.Optional[typing.Union[typing.Sequence, MessageProtocol, BaseException]]:
        ...

    async def listen(self):
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
        include: Union[Set[str], Mapping[str, Any]] = None,
        exclude: Union[Set[str], Mapping[str, Any]] = None,
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
        ReaderProtocol,
        MessageProtocol,
        typing.Dict[str, typing.Any]
    ],
    typing.Optional[MessageProtocol]
]
"""
A function meant to operate as an event handler.
Functions should look like:
    >>> def func(connection: Redis, bus: EventBus, message: Message, **kwargs) -> typing.Optional[Message]
"""


STREAM_MESSAGE = Union[int, Tuple[bytes, Dict[bytes, bytes]]]
"""Message contents that comes through a stream. Always (b'message_id', {b'key': b'value'})"""

STREAM_MESSAGES = List[STREAM_MESSAGE]
"""
A collection of messages that comes through a stream

Examples:
    >>> messages: STREAM_MESSAGES = [(b'12355435323-0', {b'key': b'value'})]
"""

STREAMS = List[Tuple[bytes, STREAM_MESSAGES]]
"""
Raw stream data that comes through when calling functions like `connection.xreadgroup`.

Expect a list of 2-tuples, with the first element being the name of the stream and the second element being a STREAM_MESSAGES

Examples:
    >>> data: STREAMS = [
            (b'EVENTS', [(b'2382394823-0', {b'key': b'value'})]),
        ]
"""

PAYLOAD = Union[Dict[str, str], Dict[bytes, bytes]]
"""The type of data that will accompany a stream message as input data"""


def event_handler(aliases: Union[str, List[str]]):
    if isinstance(aliases, str):
        aliases = [aliases]

    def decorate_function(function: HANDLER_FUNCTION):
        enforce_handler(function)
        alias_copy = aliases.copy()
        alias_copy.append(function.__name__)
        setattr(function, "aliases", alias_copy)

        return function

    return decorate_function


def type_matches_special_form(
    encountered_type: Type,
    expected_type: Union,
    check_origin: bool = None
) -> bool:
    """
    Checks to see if an encountered type definition matches the expected type definition. Serves as a deep `isinstance`
    for generic types.

    Examples:
        >>> type_matches_special_form(str, typing.Union[str, int])
        True
        >>> type_matches_special_form(bytes, typing.Union[str, int])
        False
        >>> type_matches_special_form(typing.Union[str, int], typing.Union[str, bytes])
        True

    Args:
        encountered_type:
        expected_type:
        check_origin:

    Returns:
        Whether the encountered type is compatible with the expected type
    """
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


def get_module_from_globals(module_name: str) -> typing.Optional[ModuleType]:
    if not module_name:
        return None

    split_name = module_name.strip().split(".")

    place_to_search = globals()

    for name_part in split_name:
        if isinstance(place_to_search, typing.Mapping):
            place_to_search = place_to_search.get(name_part, dict())
        else:
            place_to_search = getattr(place_to_search, name_part, dict())

    return place_to_search if place_to_search else None


def get_code(
    designation: DesignationProtocol,
    base_class: typing.Type[T] = None
) -> typing.Union[typing.Type[T], typing.Callable, typing.Tuple[str, typing.Any]]:
    """
    Find an object based off a module name and name

    Args:
        designation: The configuration stating what to look for
        base_class: The base class that the found object must comply with

    Returns:
        A type, variable, or
    """
    if designation.module_name in _IMPORTED_LIBRARIES:
        module = _IMPORTED_LIBRARIES[designation.module_name]
    else:
        module = get_module_from_globals(designation.module_name)

        if module is None:
            module = importlib.import_module(designation.module_name)
            _IMPORTED_LIBRARIES[designation.module_name] = module

    if module is None:
        raise Exception(
            f"No modules could be found at {designation.module_name}."
            f"Please check the configuration to check to see if it was correct."
        )

    code = getattr(module, designation.name, None)

    if code is None:
        members = [
            (member_name, member)
            for member_name, member in inspect.getmembers(module)
            if member_name == designation.name
        ]

        if len(members) > 0:
            code = members[0]

    if base_class and not (issubclass(code, base_class) or isinstance(code, base_class)):
        raise ValueError(
            f"The found object ('{str(code)}') from '{str(designation)}' "
            f"does not match the required base class ('{str(base_class)}')"
        )

    if code is not None:
        return code

    raise Exception(
        f"No valid types or functions could be found in {designation.module_name}. Please check the configuration."
    )
