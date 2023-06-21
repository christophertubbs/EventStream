"""
Provides low functionality stock handlers
"""
import typing
from types import ModuleType
import inspect

from .echo import echo_message

from . import master
from event_stream.utilities.types import enforce_handler


def get_master_functions(
    modules_to_check: typing.MutableSequence[ModuleType] = None
) -> typing.Dict[str, typing.Callable]:
    if modules_to_check is None:
        modules_to_check = list()

    if master not in modules_to_check:
        modules_to_check.append(master)

    candidates: typing.Dict[str, typing.Callable] = dict()

    for module in modules_to_check:  # type: ModuleType
        module_name = module.__name__
        module_candidates = {
            name: function
            for name, function in inspect.getmembers(module, predicate=lambda member: inspect.isfunction(member))
            if hasattr(function, "__module__") and function.__module__ == module_name
        }
        candidates.update(module_candidates)

    for name, function in candidates.items():
        try:
            enforce_handler(function)
        except:
            candidates.pop(name)

    return candidates