"""
@TODO: Put a module wide description here
"""
import asyncio
import typing
import abc

from redis.asyncio import Redis

import event_stream.system.logging as logging
from event_stream.configuration.communication import ListenerConfiguration
from event_stream.messages import Message
from event_stream.utilities.common import decode_stream_message
from event_stream.utilities.common import is_true
from event_stream.utilities.common import on_each
from event_stream.utilities.communication import GroupConsumer
from event_stream.utilities.communication import get_redis_connection_from_configuration

LISTENER_CONFIGURATION = typing.TypeVar("LISTENER_CONFIGURATION", bound=ListenerConfiguration, covariant=True)


class EventStreamReader(abc.ABC, typing.Generic[LISTENER_CONFIGURATION]):
    """
    Base class used for reading from Redis Streams
    """
    def __init__(self, configuration: LISTENER_CONFIGURATION, verbose: bool = False):
        """
        Constructor

        Args:
            configuration: The configuration responsible for defining how this reader should behave
            verbose: Whether the reader should output extra messages for insight
        """
        self.__verbose = is_true(verbose)
        """Whether the reader should output extra messages for insight"""

        self._configuration: LISTENER_CONFIGURATION = configuration
        """The configuration responsible for defining how this reader should behave"""

        self.__keep_polling = True
        """Indicator that the loop should keep running if it already is"""

        self.__current_operation: typing.Optional[asyncio.Task] = None
        """An asynchronous reading task created via the launch command"""

        self.__most_recent_message: typing.Optional[str] = None
        """The ID of the most recently processed message from a redis stream"""

    @property
    def verbose(self) -> bool:
        """
        Whether the reader should output extra messages for insight
        """
        return self.__verbose

    @property
    def name(self) -> str:
        """
        The name for this reader
        """
        return self._configuration.name

    @property
    def can_make_executive_decisions(self):
        """
        Whether handlers within this reader may make important system wide decisions
        """
        return False

    @property
    def configuration(self) -> LISTENER_CONFIGURATION:
        """
        The configuration responsible for defining how this reader should behave
        """
        return self._configuration

    def stop_polling(self):
        """
        Stop the reader from continuing to poll the redis stream
        """
        self.__keep_polling = False

    def launch(self) -> asyncio.Task:
        """
        Launch a new reader task

        Returns:
            An asynchronous task that will allow reading to occur in the background
        """
        task = asyncio.create_task(self.listen(), name=self.configuration.name)
        self.__current_operation = task
        return task

    async def close(self):
        """
        Stop polling and end operations within a currently running operation
        """
        still_running = self.__current_operation is not None
        still_running = still_running and not self.__current_operation.cancelling()
        still_running = still_running and not self.__current_operation.done()
        still_running = still_running and not self.__current_operation.cancelled()

        self.__keep_polling = False

        if still_running:
            try:
                self.__current_operation.cancel()
            except:
                pass

    async def process_response(
        self,
        consumer: GroupConsumer,
        message_id: str,
        result: typing.Any
    ):
        """
        Process responses to processed requests

        Args:
            consumer: The consumer providing the redis connection and communication details
            message_id: The ID of the message that served as the request
            result: Data created in reaction to a request
        """
        # Send the result if it was a message
        if isinstance(result, Message):
            if result.response_to is None:
                result.response_to = message_id
            await result.send(consumer.connection, consumer.stream_name)

    @abc.abstractmethod
    async def process_message(
        self,
        consumer: GroupConsumer,
        message_id: str,
        payload: typing.Dict[str, typing.Any]
    ) -> typing.Optional[typing.Union[typing.Sequence, Message, BaseException]]:
        """
        Interpret an incoming message

        Args:
            consumer: The consumer providing the redis connection and communication details
            message_id: The ID of the incoming message
            payload: The data that arrived with the message

        Returns:
            The results of all called handlers
        """
        ...

    async def listen(self):
        """
        Poll the redis stream and bring back relevant messages
        """
        connection: Redis = await get_redis_connection_from_configuration(
            self._configuration.redis_configuration
        )

        self.__keep_polling = True

        async with connection:
            consumer = GroupConsumer(
                connection=connection,
                stream_name=self._configuration.stream,
                group_name=self._configuration.group
            )

            async with consumer:
                if self.verbose:
                    logging.info(f"Now listening to {consumer.group_name}...")

                while self.__keep_polling:
                    messages = await consumer.read()

                    if messages is None:
                        logging.error(f"Something went wrong when reading from the stream - waiting and trying again")
                        await asyncio.sleep(1)
                        continue

                    message_processes: typing.Dict[str, typing.Coroutine] = {
                        message_id: self.process_message(consumer, message_id, decode_stream_message(payload))
                        for message_id, payload in messages.items()
                    }

                    response_processes: typing.List[typing.Coroutine] = list()

                    for message_id, process in message_processes.items():
                        try:
                            responses = await process
                            if responses is None or isinstance(responses, typing.Sequence) and len(responses) == 0:
                                continue

                            if isinstance(responses, BaseException):
                                logging.error(
                                    f"A process for message '{message_id}' in Event Stream "
                                    f"'{self.configuration.get_application_name()}:"
                                    f"{self.configuration.get_instance_identifier()}' failed",
                                    responses
                                )
                            elif isinstance(responses, Message):
                                response_processes.append(
                                    self.process_response(consumer=consumer, message_id=message_id, result=responses)
                                )
                            else:
                                response_processes.extend([
                                    self.process_response(consumer=consumer, message_id=message_id, result=response)
                                    for response in responses
                                ])
                        except BaseException as exception:
                            logging.error(
                                f"Processing for message '{message_id}' in Event Stream "
                                f"'{self.configuration.get_application_name()}:"
                                f"{self.configuration.get_instance_identifier()}' failed",
                                exception
                            )

                    response_results = await asyncio.gather(*response_processes)

                    on_each(
                        func=lambda error: logging.error(
                            f"An errored occurred when processing a message response in "
                            f"'{self.configuration.get_application_name()}:"
                            f"{self.configuration.get_instance_identifier()}'"
                        ),
                        values=response_results,
                        predicate=lambda result: isinstance(result, BaseException)
                    )

                if self.__verbose:
                    logging.info(
                        f"The '{self._configuration.name}' reader in "
                        f"'{self.configuration.get_application_name()} is no longer listening for messages"
                    )


class CloseableEventStreamReader(ListenerConfiguration, abc.ABC):
    @property
    def is_allowed_to_close(self):
        return True