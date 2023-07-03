"""
Contains common functions
"""
import asyncio
import os
import typing
import inspect
import json
import math
import random
import re
from functools import partial

from .constants import INTEGER_PATTERN
from .constants import FLOATING_POINT_PATTERN
from .constants import TRUE_VALUES
from .constants import IDENTIFIER_SAMPLE_SET
from .constants import IDENTIFIER_LENGTH
from .constants import BASE_DIRECTORY

from .types import T
from .types import R
from .types import PAYLOAD
from .types import P
from .types import C_T

from event_stream.system import settings
from event_stream.system import logging


LIBRARY_FILE_PATTERN = re.compile(r"python\d+\.\d+(?!/site-packages)")
"""
Pattern that matches on directory names like 'Versions/3.9/lib/python3.9/unittest/async_case.py' to help identify files 
belonging to Python itself and not third party libraries
"""

PYTHON_DIRECTORY_PATTERN = re.compile(r"[pP]ython\d+\.\d+/?")
"""
Pattern that matches on a part of a string that identifies a python version

Examples:
    >>> path = 'Versions/3.9/lib/python3.9/unittest/async_case.py'
    >>> PYTHON_DIRECTORY_PATTERN.split(path)
    ['Versions/3.9/lib/', 'unittest/async_case.py']
"""


def generate_identifier(
    length: int = None,
    group_count: int = None,
    separator: str = None,
    sample: typing.Sequence = None
) -> str:
    if group_count is None:
        group_count = 1

    if length is None:
        length = IDENTIFIER_LENGTH

    if separator is None:
        separator = ""

    if sample is None:
        sample = IDENTIFIER_SAMPLE_SET

    generated_groups = list()

    for _ in range(group_count):
        generated_groups.append("".join([str(random.choice(sample)) for _ in range(length)]))

    return separator.join(generated_groups)


def generate_group_name(stream_name: str, application_name: str, listener_name: str, *args, **kwargs):
    parts = [stream_name, application_name]

    parts.extend([str(value) for value in args])
    parts.extend([str(value) for value in kwargs.values()])

    parts.append(listener_name)

    return settings.key_separator.join(parts)


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


def is_true(value: typing.Union[str, int, bytes, bool, float, None], *, minimum_truth: float = None) -> bool:
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


def get_stack_trace(
    include_all: bool = None,
    get_parent: bool = None,
    exclude_fields: typing.Container[str] = None,
    cut_off_at: typing.Callable[[dict], bool] = None
) -> typing.List[dict]:
    if cut_off_at is None:
        def cut_off_at(*args, **kwargs):
            return False

    if get_parent is None:
        get_parent = False

    if not isinstance(include_all, bool):
        include_all = is_true(include_all)

    if exclude_fields is None:
        exclude_fields = []

    stack = inspect.stack()
    message_parts = list()

    if get_parent:
        start_index = 2
    else:
        start_index = 1

    cut_off_index = None

    base_directory_to_replace = str(BASE_DIRECTORY) if str(BASE_DIRECTORY).endswith("/") else str(BASE_DIRECTORY) + "/"

    for frame in reversed(stack[start_index:]):
        if not (include_all or frame.filename.startswith(str(BASE_DIRECTORY))):
            continue

        message = {
            "function": frame.function,
            "code": frame.code_context[0].strip() if frame.code_context else "",
            "lineno": frame.lineno
        }

        if frame.filename.startswith(base_directory_to_replace):
            message['file'] = frame.filename.replace(base_directory_to_replace, "")
        else:
            message['file'] = PYTHON_DIRECTORY_PATTERN.split(frame.filename)[-1]

        for key in exclude_fields:
            if key in message:
                message.pop(key)

        if len(message) == 0:
            raise Exception("Too many fields to exclude were given - a stack trace cannot be created")

        message_parts.append(message)

        if cut_off_at(message):
            cut_off_index = len(message_parts) - 1

    if cut_off_index is not None:
        message_parts = message_parts[:cut_off_index]

    return message_parts


def get_stack_depth() -> int:
    """
    Returns:
        The depth of the current stack from the calling code
    """
    # Even though it makes very little difference, we subtract the length by 1 to ensure that this
    # function isn't included
    return len(get_stack_trace())


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


def interpret_value(value):
    if isinstance(value, bytes):
        value = value.decode()

    if isinstance(value, dict):
        data = {
            key.decode() if isinstance(key, bytes) else key: interpret_value(child)
            for key, child in value.items()
        }
    elif isinstance(value, typing.Iterable) and not isinstance(value, (str, bytes)):
        data = [
            interpret_value(child)
            for child in value
        ]
    elif not isinstance(value, str):
        data = value
    elif INTEGER_PATTERN.match(value):
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

    return data


def decode_stream_message(message_payload: PAYLOAD) -> typing.Dict[str, typing.Any]:
    decoded_message = dict()

    for key, value in message_payload.items():
        if isinstance(value, bytes):
            value = value.decode()

        if isinstance(key, bytes):
            key = key.decode()

        decoded_message[key] = interpret_value(value)

    return decoded_message


async def fulfill_method(
    method: typing.Callable[[P], C_T[R]],
    *args,
    **kwargs
) -> R:
    """
    Call a method and await until it no longer returns a coroutine

    Useful for executing functions that may or not be asynchronous

    Example:
        >>> class Example1:
        >>>     def do_whatever(self, a, b, c):
        >>>         return f"{a}:{b}:{c}")
        >>>
        >>> class Example2:
        >>>     async def do_whatever(self, a, b, c):
        >>>         return f"{a}:{b}:{c}")
        >>>
        >>> ex1 = Example1()
        >>> ex2 = Example2()
        >>> print(await fulfill_method(ex1.do_whatever, 3, 2, c="whatever"))
        3:2:whatever
        >>> print(await fulfill_method(ex2.do_whatever, a=3, b=2, c="whatever"))
        3:2:whatever

    Args:
        method: The method to call
        *args: Positional arguments to send to the method
        **kwargs: keyword arguments to send to the method

    Returns:
        The first non-awaitable result of the given method
    """
    if inspect.iscoroutinefunction(method):
        return await method(*args, **kwargs)

    wrapped_function = partial(method, *args, **kwargs)

    try:
        result = await asyncio.get_running_loop().run_in_executor(executor=None, func=wrapped_function)

        while inspect.isawaitable(result):
            result = await result
    except BaseException as exception:
        input_values = list()

        if args:
            input_values.append(", ".join([str(arg) for arg in args]))

        if kwargs:
            input_values.append(", ".join([f"{str(key)}={str(value)}" for key, value in kwargs.items()]))

        method_description = f"{method.__name__}({', '.join(input_values)})"
        logging.error(f"Something went awry while trying to call {method_description}", exception=exception)
        raise

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

    origin = typing.get_origin(object_type)
    is_union = origin == typing.Union
    origin_does_not_match = not is_union and origin is not None and not isinstance(obj, origin)

    if origin_does_not_match:
        return False

    # TODO: Add handling for functions

    if is_union:
        arguments = typing.get_args(object_type)

        for argument in arguments:
            if argument is None and obj is not None:
                continue
            elif argument is None:
                return True

            if instanceof(obj, argument):
                return True

        return False

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

    try:
        return isinstance(obj, object_type)
    except:
        return False


def get_concrete_subclasses(cls: typing.Type[T]) -> typing.Iterable[typing.Type[T]]:
    direct_subclasses = cls.__subclasses__()
    concrete_subclasses = list()

    for direct_subclass in direct_subclasses:
        if not inspect.isabstract(direct_subclass):
            concrete_subclasses.append(direct_subclass)
        concrete_subclasses.extend(get_concrete_subclasses(direct_subclass))

    return concrete_subclasses

