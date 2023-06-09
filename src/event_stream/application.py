#!/usr/bin/env python3
"""
Launches configured event busses
"""
import os
import json
import pathlib
import typing
import asyncio

from argparse import ArgumentParser

from configuration import EventBusConfigurations
from event_stream.bus import EventBus
from event_stream.bus import MasterBus

MASTER_BUS_CONFIGURATION_PATH = pathlib.Path(
    os.environ.get("MASTER_BUS_CONFIGURATION_PATH", "master_bus_configuration.json")
)


class Arguments(object):
    def __init__(self, *args):
        # Replace '__option' with any of the expected arguments
        self.__path: typing.Optional[pathlib.Path] = None
        self.__verbose: bool = False

        self.__parse_command_line(*args)

    # Add a property for each argument
    @property
    def path(self) -> pathlib.Path:
        return self.__path

    @property
    def verbose(self) -> bool:
        return self.__verbose

    def __parse_command_line(self, *args):
        parser = ArgumentParser("Put the description of your application here")

        # Add Arguments
        parser.add_argument(
            "path",
            type=str,
            help="The path to the configuration data for this event stream"
        )

        parser.add_argument(
            "-v",
            dest="verbose",
            action="store_true",
            help="Print verbose information"
        )

        # Parse the list of args if one is passed instead of args passed to the script
        if args:
            parameters = parser.parse_args(args)
        else:
            parameters = parser.parse_args()

        # Assign parsed parameters to member variables
        self.__path = pathlib.Path(parameters.path).resolve()
        self.__verbose = parameters.verbose

        if not (self.__path.exists() and self.__path.is_file()):
            raise ValueError(f"A configuration file could not be found at {str(self.__path)}")


async def main():
    """
    Define your main function here
    """
    arguments = Arguments()

    configuration: EventBusConfigurations = EventBusConfigurations.parse_file(arguments.path)

    busses = list()

    for bus_configuration in configuration:
        bus = EventBus(bus_configuration=bus_configuration, verbose=arguments.verbose)
        busses.append(bus)

    busses.append(MasterBus(all_configurations=configuration, verbose=arguments.verbose))

    bus_listeners: typing.List[asyncio.Task] = [
        bus.launch()
        for bus in busses
    ]

    try:
        complete_listening_tasks, pending_listening_tasks = await asyncio.wait(
            bus_listeners,
            return_when=asyncio.FIRST_COMPLETED
        )
    except BaseException as e:
        pass


if __name__ == "__main__":
    asyncio.run(main())
