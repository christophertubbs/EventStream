#!/usr/bin/env python3
"""
Clear unused stream groups for an application
"""
import typing

from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta

from dateutil.parser import parse as parse_date

from redis import Redis


from event_stream.system import settings


STREAM_TYPE = b'stream'


class Arguments(object):
    def __init__(self, *args):
        # Replace '__option' with any of the expected arguments
        self.__oldest_allowed: typing.Optional[datetime] = None
        self.__inbox_name: typing.Optional[str] = None
        self.__ignore_pending_messages: bool = False

        self.__parse_command_line(*args)

    @property
    def oldest_allowed(self) -> datetime:
        return self.__oldest_allowed

    @property
    def inbox_name(self) -> str:
        return self.__inbox_name

    @property
    def ignore_pending_messages(self) -> bool:
        return self.__ignore_pending_messages

    def __parse_command_line(self, *args):
        parser = ArgumentParser("Remove messages for an application")

        # Add Arguments
        parser.add_argument(
            "--oldest-allowed",
            metavar="date",
            dest="oldest_allowed",
            type=str,
            default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            help="The oldest allowed message for the application in the stream"
        )

        parser.add_argument(
            "--inbox-name",
            metavar="name",
            dest="inbox_name",
            type=str,
            default=settings.consumer_inbox_name,
            help="The name of the inbox in each stream group"
        )

        parser.add_argument(
            "--ignore-pending",
            dest="ignore_pending",
            action="store_true",
            default=False,
            help="Delete consumers even if they have pending messages"
        )

        # Parse the list of args if one is passed instead of args passed to the script
        if args:
            parameters = parser.parse_args(args)
        else:
            parameters = parser.parse_args()

        # Assign parsed parameters to member variables
        self.__inbox_name = parameters.inbox_name
        self.__oldest_allowed = parse_date(parameters.oldest_allowed)
        self.__ignore_pending_messages = parameters.ignore_pending


def main():
    """
    Define your main function here
    """
    arguments = Arguments()

    connection = Redis()

    all_keys = connection.keys("*")

    streams: typing.List[bytes] = list()

    for key in all_keys:
        if connection.type(key) == STREAM_TYPE:
            streams.append(key)

    for stream in streams:
        stream_groups: typing.List[typing.Dict[str, typing.Optional[typing.Union[bytes, int]]]] = connection.xinfo_groups(stream)

        for group in stream_groups:
            if not arguments.ignore_pending_messages and group.get("pending", 0) > 0:
                continue
            elif group.get("consumers", 0) > 1:
                continue

            consumers: typing.List[typing.Dict[str, typing.Union[bytes, int]]] = connection.xinfo_consumers(stream, group.get("name"))

            if len(consumers) > 0:
                consumer = consumers[0]

                idle_time = timedelta(seconds=consumer.get("idle") / 1000)

                if consumer.get("name") != arguments.inbox_name.encode() or datetime.now() - idle_time > arguments.oldest_allowed:
                    continue

                print(f"Deleting the '{consumer.get('name')}' consumer in the group named '{group.get('name')}' in the '{stream}' stream")
                connection.xgroup_delconsumer(stream, group.get("name"), consumer.get("name"))

            print(f"Deleting the group named '{group.get('name')}' in the '{stream}' stream")
            connection.xgroup_destroy(stream, group.get("name"))


if __name__ == "__main__":
    main()
