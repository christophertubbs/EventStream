"""
@TODO: Put a module wide description here
"""
import json
import os.path
import typing
import unittest
import random
import string
import math

import event_stream.utilities.common as common
from event_stream.utilities.constants import IDENTIFIER_LENGTH
from event_stream.utilities.constants import BASE_DIRECTORY
from event_stream.system import settings

RELATIVE_FILE_LOCATION = __file__.replace(str(BASE_DIRECTORY) + "/", "")

TEST_DICTIONARY = {
    "name": "Event Example",
    "handlers": [
        {
            "name": "EchoBus",
            "event": "echo",
            "handler": {
                "module_name": "event_stream.handlers",
                "name": "echo_message"
            }
        },
        {
            "name": "EchoBus with Kwargs",
            "event": "echo",
            "stream": "ALTERNATE_EVENTS",
            "handler": {
                "module_name": "event_stream.handlers",
                "name": "echo_message",
                "kwargs": {
                    "one": 1,
                    "two": 2,
                    "three": {
                        "a": "a",
                        "b": [
                            "c",
                            "d",
                            "e"
                        ]
                    }
                }
            }
        },
        {
            "name": "InstancePrinter",
            "event": "get_instance_response",
            "handler": {
                "module_name": "event_stream.handlers.instance_info",
                "name": "print_instance_info"
            },
            "stream": "MASTER"
        }
    ],
    "busses": [
        {
            "name": "EchoBus",
            "handlers": {
                "echo": [
                    {
                        "module_name": "event_stream.handlers",
                        "name": "echo_message"
                    }
                ]
            }
        },
        {
            "name": "EchoBus with kwargs",
            "handlers": {
                "echo": [
                    {
                        "module_name": "event_stream.handlers",
                        "name": "echo_message",
                        "kwargs": {
                            "one": 1,
                            "two": 2,
                            "three": {
                                "a": "a",
                                "b": ["c", "d", "e"]
                            }
                        }
                    }
                ]
            }
        }
    ],
    "stream": "EVENTS"
}


class TestCommon(unittest.TestCase):
    def test_generate_identifier(self):
        default_identifier = common.generate_identifier()
        self.assertEqual(len(default_identifier), IDENTIFIER_LENGTH)

        only_a = common.generate_identifier(sample=["a"])
        self.assertEqual(only_a, "a" * IDENTIFIER_LENGTH)

        groups_no_separator = common.generate_identifier(group_count=2)
        self.assertEqual(len(groups_no_separator), 2 * IDENTIFIER_LENGTH)

        random_length = random.randint(3, 10)
        random_count = random.randint(3, 10)
        separator = "".join([random.choice(string.punctuation) for _ in range(4)])
        sample_set = list({random.choice(string.printable) for _ in range(8)})

        random_groups = common.generate_identifier(
            length=random_length,
            group_count=random_count,
            separator=separator,
            sample=sample_set
        )

        self.assertEqual(random_groups.count(separator), random_count - 1)

        groups = random_groups.split(separator)

        self.assertEqual(len(groups), random_count)

        for group in groups:
            self.assertEqual(len(group), random_length)

            for character in group:
                self.assertIn(character, sample_set)

    def test_on_each(self):
        input_values = [1, 2, 3, "5", "6", True, False, {"value1": 1, "value2": 2}]
        input_args = [1, 2, 3, 4]
        input_kwargs = {"value1": 1, "value2": 2, "value3": 3}

        plain_function_list = list()
        function_with_args_list = list()
        function_with_kwargs_list = list()
        function_with_both_list = list()

        def predicate(value) -> bool:
            try:
                new_value = int(value)
            except:
                new_value = sum([
                    ord(character)
                    for character in str(value)
                ])

            return new_value % 2 == 1

        def plain_function(value):
            try:
                new_value = int(value)
            except:
                new_value = sum([
                    ord(character)
                    for character in str(value)
                ])

            plain_function_list.append(new_value)
            return new_value

        def function_with_args(value, *args):
            try:
                new_value = int(value)
            except:
                new_value = sum([
                    ord(character)
                    for character in str(value)
                ])

            function_with_args_list.extend(args)
            return new_value

        def function_with_kwargs(value, **kwargs):
            try:
                new_value = int(value)
            except:
                new_value = sum([
                    ord(character)
                    for character in str(value)
                ])

            function_with_kwargs_list.append(kwargs)
            return new_value

        def function_with_both(value, *args, **kwargs):
            try:
                new_value = int(value)
            except:
                new_value = sum([
                    ord(character)
                    for character in str(value)
                ])

            function_with_both_list.append((args, kwargs))
            return new_value

        plain_function_results = common.on_each(plain_function, input_values)
        function_with_args_results = common.on_each(function_with_args, input_values, *input_args)
        function_with_kwargs_results = common.on_each(function_with_kwargs, input_values, **input_kwargs)
        function_with_both_results = common.on_each(function_with_both, input_values, *input_args, **input_kwargs)

        self.assertEqual(len(plain_function_results), len(input_values))
        self.assertEqual(len(function_with_args_results), len(input_values))
        self.assertEqual(len(function_with_kwargs_results), len(input_values))
        self.assertEqual(len(function_with_both_results), len(input_values))

        self.assertEqual(plain_function_results, plain_function_list)
        self.assertEqual(function_with_args_results, plain_function_results)
        self.assertEqual(function_with_kwargs_results, plain_function_list)
        self.assertEqual(function_with_kwargs_results, function_with_args_results)

        self.assertEqual(input_args * len(input_values), function_with_args_list)
        self.assertEqual([input_kwargs] * len(input_values), function_with_kwargs_list)
        self.assertEqual([(tuple(input_args), input_kwargs)] * len(input_values), function_with_both_list)

        plain_function_list.clear()
        function_with_args_list.clear()
        function_with_kwargs_list.clear()
        function_with_both_list.clear()

        plain_function_results = common.on_each(plain_function, input_values, predicate=predicate)
        function_with_args_results = common.on_each(function_with_args, input_values, *input_args, predicate=predicate)
        function_with_kwargs_results = common.on_each(function_with_kwargs, input_values, predicate=predicate, **input_kwargs)
        function_with_both_results = common.on_each(function_with_both, input_values, *input_args, predicate=predicate, **input_kwargs)

        self.assertEqual(plain_function_results, [1, 3, 5, 1])
        self.assertEqual(plain_function_results, plain_function_list)
        self.assertEqual(function_with_kwargs_results, plain_function_results)
        self.assertEqual(function_with_args_results, plain_function_results)
        self.assertEqual(function_with_both_results, plain_function_results)
        self.assertEqual(input_args * len(plain_function_results), function_with_args_list)
        self.assertEqual([(tuple(input_args), input_kwargs)] * len(plain_function_results), function_with_both_list)
        self.assertEqual([input_kwargs] * len(plain_function_results), function_with_kwargs_list)

    def test_is_true(self):
        self.assertTrue(common.is_true("true"))
        self.assertFalse(common.is_true("false"))
        self.assertTrue(common.is_true(True))
        self.assertFalse(common.is_true(False))
        self.assertTrue(common.is_true("ON"))
        self.assertFalse(common.is_true("off"))
        self.assertTrue(common.is_true(1))
        self.assertFalse(common.is_true(0))
        self.assertTrue(common.is_true(b'1'))
        self.assertFalse(common.is_true(b'0'))
        self.assertTrue(common.is_true(0.97))
        self.assertFalse(common.is_true(0.001))
        self.assertTrue(common.is_true(0.72, minimum_truth=0.7))
        self.assertFalse(common.is_true(0.72, minimum_truth=0.72001))

    def test_get_current_function_name(self):
        function_name = common.get_current_function_name()
        parent_function_name = common.get_current_function_name(parent_name=True)
        this_function_name = common.get_current_function_name(frame_index=0)
        following_function_name = common.get_current_function_name(frame_index=1)
        last_function_name = common.get_current_function_name(frame_index=-1)

        self.assertEqual("test_get_current_function_name", function_name)
        self.assertEqual(function_name, this_function_name)
        self.assertEqual(parent_function_name, following_function_name)
        self.assertEqual(last_function_name, "<module>")

    def test_get_stack_trace(self):
        current_stack_trace = common.get_stack_trace()
        last_frame = current_stack_trace[-1]

        self.assertTrue(isinstance(last_frame, dict))
        self.assertEqual(last_frame['function'], common.get_current_function_name())
        self.assertEqual(last_frame['file'], RELATIVE_FILE_LOCATION)
        self.assertTrue(last_frame['code'].endswith("common.get_stack_trace()"))

    def test_get_by_path(self):
        self.assertEqual(common.get_by_path(TEST_DICTIONARY, "name"), "Event Example")
        self.assertEqual(common.get_by_path(TEST_DICTIONARY, "handlers", 0), TEST_DICTIONARY['handlers'][0])
        self.assertEqual(common.get_by_path(TEST_DICTIONARY, "handlers", 17, default=False), False)
        self.assertEqual(common.get_by_path(TEST_DICTIONARY, "handlers", 17, "name", "other", "something", default=False), False)
        self.assertEqual(
            common.get_by_path(TEST_DICTIONARY, "handlers", 1, "handler", "kwargs", "three", "a", default=7),
            "a"
        )

    def test_get_environment_variable(self):
        random_id = "".join(random.choices(string.hexdigits, k=12))
        assigned_random_id = f"$$${random_id}"

        self.assertEqual(common.get_environment_variable(random_id), random_id)
        self.assertEqual(common.get_environment_variable(random_id, default="banana"), "banana")
        self.assertEqual(common.get_environment_variable(assigned_random_id), assigned_random_id)
        self.assertEqual(common.get_environment_variable(assigned_random_id, default="quack"), "quack")

        environment_variable_name_remapping: typing.Dict[str, str] = {
            key: "$" * random.randint(1, 8) + key
            for key in os.environ.keys()
        }

        for key, remapped_key in environment_variable_name_remapping.items():
            retrieved_value = common.get_environment_variable(remapped_key)
            retrieved_conversion = common.get_environment_variable(remapped_key, conversion_function=lambda value: 7)

            self.assertEqual(os.environ[key], retrieved_value)
            self.assertEqual(retrieved_conversion, 7)

    def test_json_to_dict_or_list(self):
        self.assertIsNone(common.json_to_dict_or_list(32))
        self.assertIsNone(common.json_to_dict_or_list("[1, 2, 3, 4}"))
        self.assertIsNone(common.json_to_dict_or_list("  [1 2 3 4] "))
        self.assertEqual(common.json_to_dict_or_list("[1, 2, 3]"), [1, 2, 3])
        self.assertEqual(common.json_to_dict_or_list('{"one": 1, "two": 2}'), {"one": 1, "two": 2})

    def test_decode_stream_message(self):
        original_data = {
            b'root': {
                "value1": b'1.232',
                "value2": "true",
                b'value3': "False",
                b'value4': "NaN",
                "value5": {
                    b'value6': "-inf",
                    b'value7': "INFINiTy",
                },
                "value8": [1, b'2', 3, 'inf', 'null'],
                "value9": [
                    b'1',
                    'true',
                    'FaLsE',
                    {
                        "value1": b'1.232',
                        "value2": "true",
                        b'value3': "False",
                        b'value4': "NaN",
                        "value5": {
                            b'value6': "-inf",
                            b'value7': "INFINiTy",
                        }
                    }
                ]
            }
        }

        expected_data = {
            'root': {
                "value1": 1.232,
                "value2": True,
                'value3': False,
                'value4': math.nan,
                "value5": {
                    'value6': -math.inf,
                    'value7': math.inf,
                },
                "value8": [1, 2, 3, math.inf, None],
                "value9": [
                    1,
                    True,
                    False,
                    {
                        "value1": 1.232,
                        "value2": True,
                        'value3': False,
                        'value4': math.nan,
                        "value5": {
                            'value6': -math.inf,
                            'value7': math.inf,
                        }
                    }
                ]
            }
        }

        decoded_data = common.decode_stream_message(original_data)
        self.assertEqual(expected_data, decoded_data)

    def test_instance_of(self):
        self.assertTrue(common.instanceof(8, int))
        self.assertFalse(common.instanceof("bool", bool))

        self.assertTrue(common.instanceof(8, typing.Union[int, typing.Mapping]))
        self.assertFalse(common.instanceof(8, typing.Union[str, bool]))

        self.assertTrue(common.instanceof([1, 2, 3, 4, 5], typing.Iterable[int]))
        self.assertFalse(common.instanceof([1, 'a', 4, False], typing.Sequence[int]))

        self.assertTrue(common.instanceof({"value1": 1, "value2": 2}, typing.Dict[str, int]))
        self.assertFalse(common.instanceof({"value1": 1, "value2": 2}, typing.Dict[str, bool]))

    def test_generate_group_name(self):
        stream_name = "UNITTEST"
        application_name = "UnitTest"
        listener_name = "Test"

        no_args_or_kwargs_name = common.generate_group_name(stream_name, application_name, listener_name)
        expected_name = f"{stream_name}{settings.key_separator}" \
                        f"{application_name}{settings.key_separator}" \
                        f"{listener_name}"
        self.assertEqual(expected_name, no_args_or_kwargs_name)

        args = ["GroupConsumer", "OrderedDict"]
        args_name = common.generate_group_name(stream_name, application_name, listener_name, *args)
        expected_name = f"{stream_name}{settings.key_separator}" \
                        f"{application_name}{settings.key_separator}" \
                        f"{settings.key_separator.join(args)}{settings.key_separator}" \
                        f"{listener_name}"
        self.assertEqual(expected_name, args_name)

        kwargs = {"class_name": "HandlerGroup"}
        kwargs_name = common.generate_group_name(stream_name, application_name, listener_name, **kwargs)
        expected_name = f"{stream_name}{settings.key_separator}" \
                        f"{application_name}{settings.key_separator}" \
                        f"{settings.key_separator.join(kwargs.values())}{settings.key_separator}" \
                        f"{listener_name}"
        self.assertEqual(expected_name, kwargs_name)

        args_and_kwargs_name = common.generate_group_name(
            stream_name,
            application_name,
            listener_name,
            *args,
            **kwargs
        )
        expected_name = f"{stream_name}{settings.key_separator}" \
                        f"{application_name}{settings.key_separator}" \
                        f"{settings.key_separator.join(args)}{settings.key_separator}" \
                        f"{settings.key_separator.join(kwargs.values())}{settings.key_separator}" \
                        f"{listener_name}"
        self.assertEqual(expected_name, args_and_kwargs_name)

    def test_get_concrete_subclasses(self):
        class Root:
            def __init__(self, *args, **kwargs):
                self.value1 = 1
                self.args = args
                self.kwargs = kwargs

        class ChildOne(Root):
            def __init__(self, val):
                super().__init__(1, 2, 3, 4, kwarg1="dsfs", kwarg2="3234")
                self.val = val

        class ChildTwo(ChildOne):
            def __init__(self, val, other):
                super().__init__(val=val)
                self.other = other

        subclasses = list(common.get_concrete_subclasses(Root))
        self.assertEqual(len(subclasses), 2)
        self.assertIn(ChildTwo, subclasses)
        self.assertIn(ChildOne, subclasses)


async def recursive_async_function(value, *args, add_amount: int = 0.7, **kwargs):
    if value <= 0:
        return 78

    updated_value = value + add_amount
    updated_value -= 1

    return await recursive_async_function(updated_value)


class TestCommonAsyncFunctions(unittest.IsolatedAsyncioTestCase):
    async def test_fulfill_method(self):
        result = await common.fulfill_method(recursive_async_function, 97, add_amount=0.2)
        self.assertEqual(result, 78)