#!/usr/bin/env python3
"""
Removes message records for handler work distribution
"""
import typing
import re

from datetime import datetime

from argparse import ArgumentParser

from dateutil.parser import parse as parse_date
from redis import Redis


MESSAGE_HANDLER_PATTERN = re.compile(r":[0-9]+-[0-9]+$")


class Arguments(object):
    def __init__(self, *args):
        # Replace '__option' with any of the expected arguments
        self.__application_name: typing.Optional[str] = None
        self.__oldest_allowed: typing.Optional[datetime] = None
        self.__parse_command_line(*args)

    # Add a property for each argument
    @property
    def application_name(self) -> str:
        return self.__application_name

    @property
    def oldest_allowed(self) -> datetime:
        return self.__oldest_allowed

    def __parse_command_line(self, *args):
        parser = ArgumentParser("Removes stale message handler records")

        # Add Arguments
        parser.add_argument(
            "application_name",
            type=str,
            help="This is an example of an option"
        )

        parser.add_argument(
            "--oldest-allowed",
            metavar="date",
            type=str,
            dest="oldest_allowed",
            default=datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
            help="The oldest allowable record"
        )

        # Parse the list of args if one is passed instead of args passed to the script
        if args:
            parameters = parser.parse_args(args)
        else:
            parameters = parser.parse_args()

        # Assign parsed parameters to member variables
        self.__application_name = parameters.application_name
        self.__oldest_allowed = parse_date(parameters.oldest_allowed)


def get_message_date(message_id: typing.Union[bytes, str]) -> datetime:
    if isinstance(message_id, bytes):
        message_id = message_id.decode()

    timestamp = message_id.split("-")[0]

    return datetime.fromtimestamp(int(timestamp) / 1000)


def main():
    """
    Define your main function here
    """
    arguments = Arguments()

    connection = Redis()

    key_pattern = f"*:{arguments.application_name}:*"

    for possible_key in connection.keys(key_pattern):
        possible_key = possible_key.decode()

        if not MESSAGE_HANDLER_PATTERN.search(possible_key):
            continue

        message_id = possible_key.split(":")[-1]
        message_date = get_message_date(message_id)

        if message_date < arguments.oldest_allowed:
            print(f"Removing {possible_key}")
            connection.delete(possible_key)
        else:
            print(f"Not deleting {possible_key} - it isn't old enough to remove")


if __name__ == "__main__":
    main()
