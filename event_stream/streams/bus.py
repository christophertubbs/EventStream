"""
Defines a bus object that polls for messages and distributes them to appropriate registered event handlers
"""
import typing

from event_stream.configuration.bus import EventBusConfiguration
from event_stream.streams.reader import EventStreamReader
from event_stream.utilities.communication import GroupConsumer

from event_stream.system import logging

from event_stream.utilities.common import fulfill_method


class EventBus(EventStreamReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.warning(f"An {self.__class__.__name__} was just created - consider using a handler instead")

    @property
    def configuration(self) -> EventBusConfiguration:
        return self._configuration

    async def process_message(
        self,
        consumer: GroupConsumer,
        message_id: str,
        payload: typing.Dict[str, typing.Any]
    ) -> typing.Sequence[typing.Hashable]:
        """
        Process an incoming message by finding its event name

        Args:
            consumer: The consumer controlling the communication with the redis instance
            message_id: The ID of the received message
            payload: The data that came with the message (this data should be able to be deserialized into a message)

        Returns:
            A collection of all results from matching functions for the identified event in the payload
        """
        event_name = payload.get("event")
        results: typing.List[typing.Hashable] = list()

        if event_name:
            event_handled = False
            event_defined = event_name in self.configuration.handlers

            for handler in self.configuration.get_handlers(event_name):
                event_handled = True
                result = None
                result_created = False

                try:
                    result = await fulfill_method(handler, consumer.connection, self, **payload)
                    result_created = True
                    if isinstance(result, typing.Hashable):
                        results.append(result)
                except BaseException as exception:
                    logging.error(str(exception), exception=exception)

                if result_created:
                    try:
                        await self.process_response(
                            consumer=consumer,
                            message_id=message_id,
                            result=result
                        )
                    except BaseException as exception:
                        logging.error(str(exception), exc_info=exception)

            if event_defined and not event_handled:
                logging.warning(
                    f"There were no handlers for the '{event_name}' event."
                )

        else:
            logging.warning(
                f"No event name was passed in message '{message_id}' "
                f"in the '{self.configuration.stream}' stream"
            )

        return results

    def __str__(self):
        return str(self.configuration)

    def __repr__(self):
        return str(self.configuration)