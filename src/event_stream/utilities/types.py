"""
@TODO: Put a module wide description here
"""
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

    def stop_polling(self):
        ...

    def is_allowed_to_close(self) -> bool:
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
    str
]
"""
A function meant to operate as an event handler.
Functions should look like:
    >>> def func(connection: Redis, bus: EventBus, message: Message, **kwargs) -> str
"""


STREAM_MESSAGE = Union[int, Tuple[bytes, Dict[bytes, bytes]]]
STREAM_MESSAGES = List[STREAM_MESSAGE]
STREAMS = List[Tuple[bytes, STREAM_MESSAGES]]

T = TypeVar("T")

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


def enforce_handler(possible_handler) -> HANDLER_FUNCTION:
    """
    Decorator used to enforce the correct function signature for handlers

    Args:
        possible_handler:

    Returns:

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

    function_args = [
        parameter
        for parameter in function_parameters
        if parameter.kind == parameter.VAR_POSITIONAL
    ]

    has_args = len(function_args) != 0

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

    return possible_handler
