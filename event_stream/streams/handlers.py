"""
@TODO: Put a module wide description here
"""
import inspect
import typing
from types import ModuleType

from event_stream.handlers import get_master_functions

from .reader import EventStreamReader

from event_stream.configuration.group import HandlerGroup
from event_stream.utilities.common import fulfill_method
from event_stream.utilities.communication import GroupConsumer

from event_stream.system import logging


class HandlerReader(EventStreamReader):
    @property
    def configuration(self) -> HandlerGroup:
        return self._configuration

    async def process_message(
        self,
        consumer: GroupConsumer,
        message_id: str,
        payload: typing.Dict[str, typing.Any]
    ):
        event_name = payload.get("event")
        processed = False
        result = None

        if event_name == self.configuration.event:
            try:
                result = await fulfill_method(self.configuration.handler, consumer.connection, self, **payload)
                processed = True
            except BaseException as exception:
                logging.error(str(exception), exception=exception)
        else:
            processed = True

        if processed:
            await consumer.mark_message_processed(message_id)
        else:
            logging.warning(f"Message '{message_id}' could not be processed - returning it to the queue for processing")
            await consumer.give_up_message(message_id)

        return result


class MasterHandlerReader(HandlerReader):
    def can_make_executive_decisions(self) -> bool:
        return True


def create_master_handlers(
    application_name: str,
    application_instance: str,
    stream_name: str,
    verbose: bool = None,
    extra_modules: typing.Sequence[ModuleType] = None
) -> typing.Sequence[MasterHandlerReader]:
    if verbose is None:
        verbose = False
    handlers: typing.List[MasterHandlerReader] = list()

    master_functions: typing.Mapping[str, typing.Callable] = get_master_functions(extra_modules)

    for function_name, function in master_functions.items():
        handler_name = " ".join(function_name.strip("_").split("_")).title()
        handler_event = function_name.strip("_").lower()
        master_handler_definition = {
            "stream": stream_name,
            "name": handler_name,
            "event": handler_event,
            "unique": True,
            "handler": {
                "module_name": inspect.getmodule(function).__name__,
                "name": function_name
            }
        }
        configuration: HandlerGroup = HandlerGroup.parse_obj(master_handler_definition)
        configuration.set_application_name(application_name)
        configuration.set_instance_identifier(application_instance)
        configuration.handler.set_function(function)
        handler = MasterHandlerReader(configuration=configuration, verbose=verbose)
        handlers.append(handler)

    return handlers