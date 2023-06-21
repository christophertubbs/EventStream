"""
@TODO: Put a module wide description here
"""
import inspect
import typing

from collections import Counter

from event_stream.utilities.common import get_concrete_subclasses
from .base import ACCEPTABLE_INPUT_TYPES

from .base import Message
from .base import ParseableModel
from .basic import ForwardingMessage

from .master import CloseMessage

MESSAGE_TYPE = typing.TypeVar("MESSAGE_TYPE", bound=typing.Type[Message], covariant=True)
MESSAGE = typing.TypeVar("MESSAGE", bound=Message, covariant=True)


def update_message_ranker(ranker: typing.Dict[MESSAGE_TYPE, float]):
    """
    Updates the entries in the ranker to ensure that dependent classes always have a higher
    weight than the classes they depend on

    Given the following classes:

    Example:
        >>> class Body1(Message):
        >>>     val1: int
        >>>     val2: str
        >>> class Body2(Message):
        >>>     val3: int
        >>>     val4: bool
        >>> class Body3(Body1):
        >>>     val5 = float
        >>> class Body4(Body1, Body2):
        >>>     ...
        >>> class Body5(Body2, Body3)
        >>>     ...
        >>> class Body6(Message):
        >>>     val6: str
        >>>     val7: int
        >>>     val8: bool
        >>>     val9: int

    And a `ranker` of:

    Example:
        >>> rnker = Counter()
        >>> rnker[Body2] = Body2.get_weight()
        >>> rnker[Body1] = Body1.get_weight()
        >>> rnker[Body3] = Body3.get_weight()
        >>> rnker[Body4] = Body4.get_weight()
        >>> rnker[Body5] = Body5.get_weight()
        >>> rnker
        Counter({
            <class 'Body5'>: 8
            <class 'Body4'>: 7,
            <class 'Body6'>: 5,
            <class 'Body3'>: 5,
            <class 'Body1'>: 3,
            <class 'Body2'>: 3
        })

    The result is:

    Example:
        >>> update_message_ranker(rnker)
        >>> rnker
        Counter({
            <class 'Body4'>: 13,
            <class 'Body5'>: 11,
            <class 'Body3'>: 8,
            <class 'Body6'>: 5,
            <class 'Body1'>: 3,
            <class 'Body2'>: 3
        })

    Args:
        ranker: A mapping between message body types and their weights
    """

    values_to_weigh = {
        message_type: [message_type]
        for message_type in ranker.keys()
    }

    for cls in ranker.keys():
        for message_type in values_to_weigh:
            if issubclass(cls, message_type) and message_type not in values_to_weigh[cls]:
                values_to_weigh[cls].append(message_type)
            elif issubclass(message_type, cls) and cls not in values_to_weigh[message_type]:
                values_to_weigh[message_type].append(cls)

    for cls, parents in values_to_weigh.items():
        ranker[cls] = sum(map(lambda parent: parent.get_weight(), parents))


def get_message_body_data_types() -> typing.Tuple[MESSAGE_TYPE, ...]:
    """
    Creates a tuple of all concrete subclasses of `MessageBody`. The resulting tuple needs to be in order of the
    greatest covariance rank to the least covariance rank. Putting a less specific class prior the to more specific
    class will result in the more specific class to not be deserialized and instead favor its parent.

    Returns:
        A tuple to insert into a Union demonstrating acceptable types of MessageBodies to parse
    """
    subclasses = get_concrete_subclasses(Message)

    # Create the counter that will ensure that encountered types are ordered by their weight
    ranker = {
        subclass: 0
        for subclass in subclasses
    }

    # Update the rankings to ensure that the classes with the greatest amount of covariance come first
    update_message_ranker(ranker)

    # Gather the classes into a list sorted by their rank
    ranked_classes = [
        cls
        for cls, count in sorted(ranker.items(), key=lambda entry: entry[1], reverse=True)
    ]

    # If a class isn't found, default to the base class
    ranked_classes.append(Message)

    # Add the values to a tuple in order to make the returned values immutable
    return tuple(ranked_classes)


def get_message_wrapper_type() -> typing.Type[ParseableModel]:
    class MessageWrapper(ParseableModel):
        """
        A generic message type that may produce any type of found MessageBody
        """
        data: typing.Union[get_message_body_data_types()]

    return MessageWrapper


def parse(
    data: typing.Union[ACCEPTABLE_INPUT_TYPES],
    *,
    content_type: str = None,
    allow_pickle: bool = None
) -> MESSAGE:
    wrapper_type = get_message_wrapper_type()
    data = {
        "event": "wrapping",
        "data": data
    }
    message = wrapper_type.parse(data=data, content_type=content_type, allow_pickle=allow_pickle)
    return message.data
