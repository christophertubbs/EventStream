"""
@TODO: Put a module wide description here
"""
import json
import unittest

import messages
from messages import Message
from messages import ValueEvent
from messages import ExampleEvent
from messages.master import TrimMessage
from messages import JSONIntListMessage
from messages import TypedJSONMessage

class TestMessages(unittest.TestCase):
    def test_generic_message(self):
        value_message = {
            "event": "value test",
            "example_body_value": 1
        }

        example_message = {
            "event": "example test",
            "example_data": '{"example": 3}'
        }

        generic_message = {
            "event": "generic",
            "hoopla": "HOOPLA",
            "data": {
                "example_data": '{"example": 3}',
                "example_body_value": 1
            }
        }

        trim_message = {
            "event": "trim"
        }

        parsed_value_message: messages.Message = messages.parse(value_message)
        parsed_example_message: messages.Message = messages.parse(example_message)
        parsed_generic_message: messages.Message = messages.parse(generic_message)
        parsed_trim_message: messages.Message = messages.parse(trim_message)

        self.assertEqual(type(parsed_generic_message), Message)
        self.assertEqual(type(parsed_value_message), ValueEvent)
        self.assertEqual(type(parsed_example_message), ExampleEvent)
        self.assertEqual(type(parsed_trim_message), TrimMessage)

    def test_payload_message(self):
        payload_data = {
            "event": "payload testing",
            "data": "[1, 2, 3]"
        }

        value_message = {
            "event": "value test",
            "example_body_value": 1
        }

        typed_payload_data = {
            "event": "payload testing",
            "data": json.dumps(value_message)
        }

        parsed_payload_message = messages.parse(payload_data)
        parsed_typed_payload_message = messages.parse(typed_payload_data)

        self.assertEqual(type(parsed_payload_message), JSONIntListMessage)
        self.assertEqual(type(parsed_typed_payload_message), TypedJSONMessage)
