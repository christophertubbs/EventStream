#!/usr/bin/env python3
"""
@TODO: Describe the application here
"""
import typing
import re

from argparse import ArgumentParser

from redis import Redis

LOCK_LENGTH = 4
LOCK_TYPE = b'string'

MESSAGE_PATTERN = re.compile(r"\d+-\d+$")


class Arguments(object):
    def __init__(self, *args):
        self.__application_name: typing.Optional[str] = None

        self.__parse_command_line(*args)

    # Add a property for each argument
    @property
    def application_name(self) -> str:
        return self.__application_name

    def __parse_command_line(self, *args):
        parser = ArgumentParser("Releases all locks for an application")

        # Add Arguments
        parser.add_argument(
            "application",
            type=str,
            help="This name of the application to unlock"
        )

        # Parse the list of args if one is passed instead of args passed to the script
        if args:
            parameters = parser.parse_args(args)
        else:
            parameters = parser.parse_args()

        # Assign parsed parameters to member variables
        self.__application_name = parameters.application


def main():
    """
    Define your main function here
    """
    arguments = Arguments()

    got_approval = False
    max_approval_attempts = 5
    approval_attempts = 0
    should_clear = False

    prompt_message = f"Are you sure you want to clear all locks for the {arguments.application_name} application? " \
                    f"This might disrupt currently running processes or delete keys that aren't locks. Y/N"

    while got_approval is False and approval_attempts < max_approval_attempts:
        prompt_answer = input(f"Are you sure you want to clear all locks in {prompt_message}")

        if prompt_answer.lower().startswith("y"):
            got_approval = True
            should_clear = True
        elif prompt_answer.lower().startswith("n"):
            got_approval = True
            should_clear = False
        else:
            approval_attempts += 1
            print(f"'{prompt_answer}' is not a valid answer. {max_approval_attempts - approval_attempts} prompts left.")
            print()

    if not got_approval:
        print("Wrong answers were given too many times. Quitting...")
        exit(1)
    elif not should_clear:
        print("Not clearing locks...")
        exit(1)
    else:
        print("Clearing locks...")

    key_pattern = f"*:{arguments.application_name}:*:LOCK"

    connection = Redis()

    keys = connection.keys(key_pattern)

    for key in keys:
        key = key.decode()

        if MESSAGE_PATTERN.search(key):
            continue
        elif not key.endswith("LOCK"):
            continue

        key_type = connection.type(key)

        if key_type != LOCK_TYPE:
            continue

        connection.delete(key)
        print(f"Deleted the {key} entry")


if __name__ == "__main__":
    main()
