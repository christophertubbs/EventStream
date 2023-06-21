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
import random

from types import ModuleType

from .constants import INTEGER_PATTERN
from .constants import FLOATING_POINT_PATTERN
from .constants import TRUE_VALUES
from .constants import IDENTIFIER_SAMPLE_SET
from .constants import IDENTIFIER_LENGTH

from .types import CodeDesignationProtocol
from .types import T
from .types import R
from .types import PAYLOAD
from .types import P

_IMPORTED_LIBRARIES: typing.Dict[str, ModuleType] = dict()


def generate_identifier(length: int = None, separator: str = None, sample: typing.Iterable = None) -> str:
    if length is None:
        length = IDENTIFIER_LENGTH

    if separator is None:
        separator = ""

    if sample is None:
        sample = IDENTIFIER_SAMPLE_SET

    return separator.join([
        str(random.choice(sample))
        for _ in range(length)
    ])


def on_each(
    func: typing.Union[typing.Callable[[T, P], R], typing.Callable[[T], R]],
    values: typing.Iterable[T],
    *args,
    predicate: typing.Callable[[T], bool] = None,
    **kwargs,
) -> typing.Sequence[R]:
    """
    Calls a function on every member in a collection. Differs from functions like `map` in that it ensures that the
    function is called on every member prior to returning whereas the function is only called on demand with
    functions like `map`

    Args:
        func: The function to call
        values: The elements to call te function on
        *args: The *args parameter to insert into every function call
        predicate: Keyword only argument giving a function to limit what objects to call the function on
        **kwargs: The **kwargs parameter to insert into every function call

    Returns:
        The results of each function call
    """
    if values is None:
        return list()

    if isinstance(values, (str, bytes)) or not isinstance(values, typing.Iterable):
        raise ValueError(
            f"Cannot call a function on every object in a collection - received a {values.__class__.__name__}"
            f" instead of a standard iterable object"
        )

    results: typing.List[R] = list()
    has_args = len(args) > 0
    has_kwargs = len(kwargs) > 0

    for value in values:
        if predicate and not predicate(value):
            continue

        if has_args and has_kwargs:
            result = func(value, *args, **kwargs)
        elif has_kwargs:
            result = func(value, **kwargs)
        elif has_args:
            result = func(value, *args)
        else:
            result = func(value)

        results.append(result)

    return results


def is_true(value: typing.Union[str, int, bytes, bool, float], *, minimum_truth: float = None) -> bool:
    """
    Determine if a generally non-boolean value is True or False

    Args:
        value: A value that might represent a true or false value
        minimum_truth: The minimum value to be considered as true when checking floats

    Examples:
         >>> is_true("true")
         True
         >>> is_true("False")
         False
         >>> is_true(True)
         True
         >>> is_true("off")
         False
         >>> is_true("ON")
         True
         >>> is_true(1)
         True
         >>> is_true(0)
         False
         >>> is_true(b'0')
         True
         >>> is_true(0.001)
         False
         >>> is_true(0.97)
         True
         >>> is_true(0.72, minimum_truth=0.7)
         True
         >>> is_true(0.72, minimum_truth=0.72001)
         False

    Returns:
        Whether the value should be considered as `True`
    """
    if value is None:
        return False

    if isinstance(value, bytes):
        value = value.decode()

    if isinstance(value, str) and value == "":
        return False

    if isinstance(value, int):
        return value != 0

    if isinstance(value, float):
        return value > (minimum_truth if isinstance(minimum_truth, typing.SupportsFloat) else 0.3)

    return value in TRUE_VALUES


def get_current_function_name(parent_name: bool = None, frame_index: int = None) -> str:
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
        frame_index: An optional index to retrieve data from rather than evaluating it

    Returns:
        The name of the current function
    """
    stack: typing.List[inspect.FrameInfo] = inspect.stack()[1:]

    if frame_index is None:
        frame_index = 1 if parent_name else 0

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
    conversion_function: typing.Callable[[str], T] = None,
    default = None
) -> typing.Union[T, str]:
    if variable_name is None:
        return variable_name

    original_value = variable_name

    while variable_name.startswith("$"):
        variable_name = variable_name[1:]

    if variable_name not in os.environ:
        return default or original_value

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

