"""
Contains common functions
"""
import abc
import os
import typing
import inspect
import importlib
import json
import math

from types import ModuleType

from .constants import INTEGER_PATTERN
from .constants import FLOATING_POINT_PATTERN
from .types import CodeDesignationProtocol
from .types import HANDLER_FUNCTION
from .types import T
from .types import PAYLOAD

_IMPORTED_LIBRARIES: typing.Dict[str, ModuleType] = dict()


def get_current_function_name(parent_name: bool = None) -> str:
    """
    Gets the name of the current function (i.e. the function that calls `get_current_function_name`)

    Call with `parent_name` set to True to get the name of the caller's caller

    Examples:
        >>> def outer():
        >>>     def inner():
        >>>         print(get_current_function_name())
        >>>         print(get_current_function_name(parent_name=True))
        >>>     inner()
        inner
        outer

    Args:
        parent_name: Whether to get the caller of the caller that tried to get a name

    Returns:
        The name of the current function
    """
    stack: typing.List[inspect.FrameInfo] = inspect.stack()

    # Get the info for the second object in the stack - the first frame listed will be for THIS function
    # (`get_current_function_name`)
    if not parent_name:
        frame_index = 1
    else:
        # Get the info for the third object in the stack - the first frame listed will be for THIS function
        # (`get_current_function_name`) and the second will be the function that callled `get_current_function_name`
        frame_index = 2

    caller_info: inspect.FrameInfo = stack[frame_index]
    return caller_info.function


def get_stack_trace() -> typing.List[dict]:
    stack = inspect.stack()
    message_parts = list()

    for frame in stack[1:]:
        message_parts.insert(0, {
            "file": frame.filename,
            "code": frame.code_context[0].strip() if frame.code_context else "",
            "line_number": frame.lineno,
            "function": frame.function
        })

        if frame.function.strip() == "<module>":
            break

    return message_parts

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


def get_by_path(data: typing.Dict[str, typing.Any], *path, default=None):
    found_data = data
    last_index = len(path) - 1

    for path_index, path_part in enumerate(path):
        data_is_sequence = isinstance(found_data, typing.Sequence) and not isinstance(found_data, (str, bytes))
        if found_data is None and path_index < last_index:
            return default
        elif isinstance(path_part, int) and path_part >= 0 and data_is_sequence:
            if path_part > len(found_data) - 1:
                return default

            found_data = found_data[path_part]
        elif isinstance(found_data, typing.Mapping) and path_part in found_data.keys():
            found_data = found_data[path_part]
        else:
            return default

    return found_data



def get_code(
    designation: CodeDesignationProtocol,
    base_class: typing.Type[T] = None
) -> typing.Union[typing.Type[T], typing.Callable]:
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


def get_environment_variable(
    variable_name: str,
    error_message: str = None,
    conversion_function: typing.Callable[[str], T] = None
) -> typing.Union[T, str]:
    while variable_name.startswith("$"):
        variable_name = variable_name[1:]

    try:
        value = os.environ[variable_name]

        if conversion_function:
            value = conversion_function(value)

        return value
    except KeyError as key_error:
        if error_message is None:
            error_message = f"The required environment variable `{variable_name}` was not defined."
        raise ValueError(error_message) from key_error


def json_to_dict_or_list(
    data: str
) -> typing.Optional[typing.Union[typing.Dict[str, typing.Any], typing.List]]:
    if not isinstance(data, str):
        return None

    data = data.strip()

    if not (data.startswith("[") and data.endswith("]") or data.startswith("{") and data.endswith("}")):
        return None

    try:
        return json.loads(data)
    except:
        return None


def decode_stream_message(message_payload: PAYLOAD) -> typing.Dict[str, typing.Any]:
    decoded_message = dict()

    for key, value in message_payload.items():
        if isinstance(value, bytes):
            value = value.decode()

        if isinstance(key, bytes):
            key = key.decode()

        if INTEGER_PATTERN.match(value):
            data = int(value)
        elif FLOATING_POINT_PATTERN.match(value):
            data = float(value)
        elif value.lower() == "true":
            data = True
        elif value.lower() == "false":
            data = False
        elif value.lower() == "nan":
            data = math.nan
        elif value.lower() in ('inf', 'infinity'):
            data = math.inf
        elif value.lower() in ('-inf', '-infinity'):
            data = -math.inf
        elif value in ("None", "Null", "null", "nil"):
            data = None
        else:
            data = json_to_dict_or_list(value)

            if not data:
                data = value

        decoded_message[key] = data

    return decoded_message


async def fulfill_method(
    method: typing.Callable[[typing.Any, ...], typing.Any],
    *args,
    **kwargs
):
    result = method(*args, **kwargs)

    while inspect.isawaitable(result):
        result = await result

    return result


def instanceof(obj: object, object_type: typing.Type) -> bool:
    def is_generic(cls):
        if isinstance(cls, typing._GenericAlias):
            return True
        if isinstance(cls, typing._SpecialForm):
            return cls not in {typing.Any}
        return False

    if not is_generic(object_type):
        return isinstance(obj, object_type)

    if not isinstance(obj, typing.get_origin(object_type)):
        return False

    # TODO: Add handling for functions

    if isinstance(obj, typing.Mapping):
        key_type, value_type = typing.get_args(object_type)

        for key, value in obj.items():
            if not instanceof(key, key_type) or not instanceof(value, value_type):
                return False

        return True
    elif isinstance(obj, typing.Iterable):
        value_type: typing.Type = typing.get_args(object_type)[0]

        for value in obj:
            if not instanceof(value, value_type):
                return False

        return True

    return isinstance(obj, object_type)


def get_concrete_subclasses(cls: typing.Type[T]) -> typing.Iterable[typing.Type[T]]:
    direct_subclasses = cls.__subclasses__()
    concrete_subclasses = list()

    for direct_subclass in direct_subclasses:
        if not inspect.isabstract(direct_subclass):
            concrete_subclasses.append(direct_subclass)
        concrete_subclasses.extend(get_concrete_subclasses(direct_subclass))

    return concrete_subclasses

