#!/usr/bin/env python3
"""
@TODO: Describe the application here
"""
import typing
import asyncio

from argparse import ArgumentParser

from redis.asyncio import Redis

from event_stream.utilities.types import STREAM_MESSAGES
from event_stream.utilities.common import decode_stream_message
from event_stream.utilities.communication import organize_messages


class Arguments(object):
    def __init__(self, *args):
        # Replace '__option' with any of the expected arguments
        self.__name: typing.Optional[str] = None
        self.__stream: str = "MASTER"
        self.__application_name: typing.Optional[str] = None
        self.__application_instance: typing.Optional[str] = None

        self.__parse_command_line(*args)

    # Add a property for each argument
    @property
    def name(self) -> str:
        return self.__name

    @property
    def stream(self) -> str:
        return self.__stream

    @property
    def application_name(self) -> typing.Optional[str]:
        return self.__application_name

    @property
    def application_instance(self) -> typing.Optional[str]:
        return self.__application_instance

    def __parse_command_line(self, *args):
        parser = ArgumentParser("Closes all instances of the given application or all if no name is given")

        # Add Arguments
        parser.add_argument(
            "--name",
            metavar="name",
            dest="name",
            type=str,
            help="The name of the application to close"
        )

        parser.add_argument(
            "--stream",
            metavar="stream",
            dest="stream",
            type=str,
            default="MASTER",
            help="The name of the master stream for the application to close"
        )

        parser.add_argument(
            "--application-name",
            metavar="application-name",
            dest="application_name",
            type=str,
            help="The name of the application to close"
        )

        parser.add_argument(
            "--application-instance",
            metavar="application-instance",
            dest="application_instance",
            type=str,
            help="The id for the instance to close"
        )

        # Parse the list of args if one is passed instead of args passed to the script
        if args:
            parameters = parser.parse_args(args)
        else:
            parameters = parser.parse_args()

        # Assign parsed parameters to member variables
        self.__name = parameters.name
        self.__stream = parameters.stream
        self.__application_name = parameters.application_name
        self.__application_instance = parameters.application_instance


def should_close(
    message: typing.Dict[str, typing.Any],
    get_instance_event_name: str,
    application_to_close: typing.Optional[str],
    instance_to_close: typing.Optional[str]
) -> bool:
    if message.get("event") != get_instance_event_name:
        return False

    if instance_to_close is not None and message.get("application_instance") != instance_to_close:
        return False

    if application_to_close is not None and message.get("application_name") != application_to_close:
        return False

    return True


async def main():
    """
    Define your main function here
    """
    arguments = Arguments()

    connection = Redis()
    await connection.xadd("MASTER", {"event": "get_instance"})

    print("waiting to make sure that 'get_instance_response' has been sent")
    await asyncio.sleep(1.0)

    previous_entries: STREAM_MESSAGES = await connection.xrevrange("MASTER", count=15)

    messages = organize_messages(previous_entries)
    messages = {
        message_id: decode_stream_message(contents)
        for message_id, contents in messages.items()
    }

    to_close: typing.List[typing.Tuple[str, str]] = list()

    for contents in messages.values():
        if should_close(contents, "get_instance_response", arguments.application_name, arguments.application_instance):
            to_close.append((contents.get("application_name"), contents.get("application_instance")))

    for application_name, application_instance in to_close:
        contents = {
            "event": "close_streams",
            "application_name": application_name,
            "application_instance": application_instance
        }
        await connection.xadd("MASTER", contents)
        print(f"Sent the message to close {application_name}:{application_instance}")




if __name__ == "__main__":
    asyncio.run(main())
