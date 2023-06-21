#!/usr/bin/env python3
"""
@TODO: Describe the application here
"""
import os
import sys
import json
import pathlib
import typing

from argparse import ArgumentParser

from configuration import EventBusConfigurations

DEFAULT_SCHEMA_PATH = os.environ.get("EVENT_STREAM_SCHEMA_PATH", "schema.json")


class Arguments(object):
    def __init__(self, *args):
        # Replace '__option' with any of the expected arguments
        self.__path: typing.Optional[pathlib.Path] = None
        self.__pipe: bool = False

        self.__parse_command_line(*args)

    # Add a property for each argument
    @property
    def path(self) -> pathlib.Path:
        return self.__path

    @property
    def pipe(self) -> bool:
        return self.__pipe

    def __parse_command_line(self, *args):
        parser = ArgumentParser("Write the configuration schema to disk")

        # Add Arguments
        parser.add_argument(
            "-p",
            metavar="path",
            dest="path",
            type=str,
            default=DEFAULT_SCHEMA_PATH,
            help="The path to where the schema should be written to disk"
        )

        parser.add_argument(
            "--pipe",
            dest="pipe",
            action="store_true",
            default=False,
            help="Output to stdout rather than to a file"
        )

        # Parse the list of args if one is passed instead of args passed to the script
        if args:
            parameters = parser.parse_args(args)
        else:
            parameters = parser.parse_args()

        # Assign parsed parameters to member variables
        path = pathlib.Path(parameters.path)
        if path.is_dir():
            path = path / "schema.json"

        self.__path = path.resolve()
        self.__pipe = parameters.pipe


def main():
    """
    Define your main function here
    """
    arguments = Arguments()

    schema = EventBusConfigurations.schema()

    arguments.path.parent.mkdir(parents=True, exist_ok=True)

    if arguments.pipe:
        json.dump(obj=schema, fp=sys.stdout, indent=4)
    else:
        with arguments.path.open(mode='w') as schema_file:
            json.dump(obj=schema, fp=schema_file, indent=4)

            print("An updated schema was written to:")
            print(arguments.path)


if __name__ == "__main__":
    main()
