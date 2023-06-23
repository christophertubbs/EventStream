"""
Defines base classes for messages
"""
from __future__ import annotations

import logging
import abc
import json
import inspect
import os
import sys
import typing
import socket
import typing_extensions

from functools import partial as prepare_function
from pathlib import Path
from datetime import datetime

from typing import (
    Union,
    Any,
    Mapping,
    Final,
    Tuple,
    Type,
    ClassVar,
    Optional,
    Dict
)

from pydantic import BaseModel
from pydantic import Field
from pydantic import FilePath
from pydantic import Json
from pydantic import PrivateAttr
from pydantic import validator
from pydantic import ValidationError

from redis.asyncio import Redis

from event_stream.utilities.common import get_by_path
from event_stream.utilities.common import get_current_function_name
from event_stream.utilities.types import INCLUDE_EXCLUDE_TYPES
from event_stream.system.system import settings

ACCEPTABLE_INPUT_TYPES: Final[Tuple[Type, ...]] = (
    dict,
    FilePath,
    str,
    bytes,
)

MODEL_TYPE = typing.TypeVar("MODEL_TYPE", bound=BaseModel, covariant=True)

WEIGHT_FUNCTION = typing.Callable[[typing.Type["WeightedModel"]], int]

EVENT_LITERAL_ADJUSTER = 100
MAX_STACK_SIZE = 5


def extra_calculation(calculation: WEIGHT_FUNCTION) -> WEIGHT_FUNCTION:
    """
    A decorator that attaches the 'is_weight_calculation'=True attribute on a function
    Args:
        calculation:

    Returns:

    """
    if not isinstance(calculation, typing.Callable):
        raise ValueError(
            f"An object must be callable to be considered as an extra calculations"
        )
    setattr(calculation, "is_weight_calculation", True)
    return calculation


class ParseableModel(abc.ABC, BaseModel):
    """
    A BaseModel with a `parse` function that can call one of three BaseModel parse functions based on input types
    """
    @classmethod
    def parse(
        cls,
        data: Union[ACCEPTABLE_INPUT_TYPES],
        *,
        content_type: str = None,
        allow_pickle: bool = None
    ) -> typing_extensions.Self:
        """
        Deserialize the given data as a pydantic model

        Args:
            data: The data to parse
            content_type: The data to parse
            allow_pickle: Whether pickle functionality may be used

        Returns:
            A new instance of this class
        """
        if allow_pickle is None:
            allow_pickle = False

        if not isinstance(data, ACCEPTABLE_INPUT_TYPES):
            raise TypeError(
                f"'{type(data)}' is not a supported input format for EventStream Messages. "
                f"Acceptable formats are: "
                f"{', '.join([str(acceptable_type) for acceptable_type in ACCEPTABLE_INPUT_TYPES])}"
            )

        if isinstance(data, FilePath):
            model = cls.parse_file(data, content_type=content_type, allow_pickle=allow_pickle)
        elif isinstance(data, dict):
            model = cls.parse_obj(data)
        elif isinstance(data, (str, bytes)):
            model = cls.parse_raw(data, content_type=content_type, allow_pickle=allow_pickle)
        else:
            raise NotImplementedError(f"The parsing logic for '{type(data)}' data has not yet been implemented")

        return model


class WeightedModel(ParseableModel, abc.ABC):
    """
    A parseable model whose specificity in a mro tree may be measured. A weighted model with the fields
    {"one", "two", "three"} has a heigher weight than a model with the fields {"one", "two"}
    and is therefore more specific

    When choosing models to deserialize as, the more specific models should be tried first to ensure that
    the values for all fields end up where they need to go


    The weight is a function of the number of required fields,
    the depth of the mro chain, and the results of class specific functions
    """
    _weight: ClassVar[Optional[int]] = None
    """The measure of the specificity of the class"""

    _extra_weight_calculations: ClassVar[typing.List[typing.Union[str, WEIGHT_FUNCTION]]] = list()
    """Class specific functions that help describe how specific the function is under special circumstances"""

    @classmethod
    def __could_be_internal_weight_function(cls, obj) -> bool:
        if getattr(obj, "__self__", None) is not cls or not inspect.ismethod(obj):
            return False

        signature = inspect.signature(obj)

        # The return type is only compatible if it isn't defined or if it can be considered as an integer,
        #   otherwise this function needs to return False
        return_type = signature.return_annotation
        return_type_is_incompatible = return_type is not signature.empty
        return_type_is_incompatible = return_type_is_incompatible and not isinstance(return_type, typing.SupportsInt)

        if return_type_is_incompatible:
            return False

        # Internal weight functions will be called without any parameters,
        #   so try to grab a list of all parameters that are required
        required_parameters = [
            value
            for value in signature.parameters.values()
            if value.default is not None
        ]

        # This function is not valid for calculating weights if it has at least one required parameter
        return len(required_parameters) > 0

    @classmethod
    def __could_be_external_weight_function(cls, obj) -> bool:
        """
        Determines whether the given object qualifies as an external function that takes at least one function
        that may be a weighted function for this class

        Args:
            obj: The object to check

        Returns:
            whether the given object qualifies as an external function that takes at least one function
            that may be a weighted function for this class
        """
        # A usable external function is one that is a function and has a name that matches its qualified name
        #   This will exclude class member functions because their qualified names don't match their regular names
        is_function = inspect.isfunction(obj)
        object_name = getattr(obj, "__name__", -1)
        qualified_name = getattr(obj, "__qualname__", 1)

        if not is_function or object_name != qualified_name:
            return False

        signature = inspect.signature(obj)

        # Weight functions will be called with a single positional parameter,
        #   so try to grab a list of all parameters that don't require a keyword
        parameters = [
            value
            for value in signature.parameters.values()
            if value.kind in (value.POSITIONAL_OR_KEYWORD, value.POSITIONAL_ONLY)
        ]

        # If there are no defined parameters where we can inject the
        #   cls as the first variable it is not a valid weight function
        if len(parameters) == 0:
            return False

        # Weight functions are called with just a single parameter - the class.
        #   If the function requires other parameters, it will fail when called
        number_of_required_parameters = len([True for parameter in parameters if parameter.default is not None])

        if number_of_required_parameters > 1:
            return False

        # Lastly, check the annotation on that first parameter. If there isn't an annotation,
        #   it could be anything so we might be in the clear.
        #   If this class is a subclass of what is defined, we'll be good to go
        first_parameter = parameters[0]

        if first_parameter.annotation == first_parameter.empty:
            return True

        # We'll need to check if the annotation is something like `typing.Union[something_else, WeightedModel]`.
        # If that's the case, a simple issubclass will break and we'll need to break out the possible
        # types and see if one fits
        annotation_origin = typing.get_origin(first_parameter.annotation)

        # Only missing origins or unions are valid
        if annotation_origin is not None and annotation_origin is not typing.Union:
            return False
        elif annotation_origin is not None:
            # Since there's an origin that's a Union, we need to check every possible type and see if this class
            # fulfills that need. An annotation of `typing.Union[str, WeightedModel]` is valid since `WeightedModel`
            # is a parent class of whatever this class is
            possible_types = typing.get_args(first_parameter.annotation)

            for possible_type in possible_types:
                if issubclass(cls, possible_type):
                    return True

            # This class was not an option for the union, so we fail out
            return False

        # We can now perform a simple `issubclass` check since we know it's not a Union.
        #   If that parameter is annotated as anything, it MUST be something along this class' MRO chain
        if not issubclass(cls, first_parameter.annotation):
            return False

        # Check to make sure the return value is compatible

        # The return type is only compatible if it isn't defined or if it can be considered as an integer,
        #   otherwise this function needs to return False
        return_type = signature.return_annotation
        return_type_is_incompatible = return_type is not signature.empty
        return_type_is_incompatible = return_type_is_incompatible and not isinstance(return_type, typing.SupportsInt)

        return return_type_is_incompatible

    @classmethod
    def get_weight(cls) -> int:
        """
        Get the weight of this model expressing how specific it is.
        The higher the number, the greater its specificity

        Returns:
            The weight of this model describing its specificity
        """
        if cls._weight is None:
            required_fields = {
                key: field
                for key, field in cls.__fields__.items()
                if field.required
            }

            weight = len(inspect.getmro(cls))

            for key, field in required_fields.items():
                if isinstance(field.type_, WeightedModel):
                    weight += field.type_.get_weight()
                else:
                    weight += 1

            weight += cls.__perform_extra_weight_calculations()

            cls._weight = round(weight)

        return cls._weight

    @classmethod
    def _get_extra_weight_calculations(cls) -> typing.Sequence[typing.Callable[[], typing.SupportsInt]]:
        """
        Get all weight calculations that need to be performed to determine this class' weight

        Returns:
            A series of functions that will modify the calculated weight of the class
        """
        found_calculations = list()

        # First look through the hardcoded calculations.
        # Throw errors if incompatible calculations were added, otherwise add them to the list of found calculations
        for entry in cls._extra_weight_calculations:
            # Users can add strings. If so, try to grab a matching attribute from this class
            if isinstance(entry, str):
                method = getattr(cls, entry, None)

                if method is None:
                    raise ValueError(
                        f"The weight of {cls.__name__} cannot be determined - "
                        f"the required weight function named '{entry}' is not a member of {cls.__name__}"
                    )
                elif not inspect.isfunction(method):
                    raise ValueError(
                        f"The weight of {cls.__name__} cannot be determined - "
                        f"the attribute named '{entry}' of {cls.__name__} is not a callable class method"
                    )
            else:
                # Capture the entry as 'method' so it may be used the same was the method from the str block as above
                method = entry

            # We can only add functions if they can be internal or external weight functions. Fail otherwise
            if cls.__could_be_external_weight_function(method):
                # We want to call all the weight functions like `method()`, but external weight functions
                # require this cls as the first parameter. Prepare the function as a partial in order to call it in
                # the same fashion as the internal functions
                found_calculations.append(prepare_function(method, cls))
            elif cls.__could_be_internal_weight_function(method):
                found_calculations.append(method)
            else:
                raise ValueError(
                    f"The weight of {cls.__name__} cannot be determined - "
                    f"The attribute '{str(entry)}' object is not a valid weight function"
                )

        # Now check for all decorated functions within this class to find other weight calculations
        members = inspect.getmembers(
            cls,
            predicate=lambda member: hasattr(member, "is_weight_calculation") and inspect.isfunction(member)
        )

        for name, function in members:
            # If the found weight calculation complies with weight function constraints, we can go ahead and add it
            if cls.__could_be_internal_weight_function(function):
                found_calculations.append(function)
            else:
                # Otherwise the function was incorrectly marked and we should throw an error
                raise ValueError(
                    f"The weight of {cls.__name__} cannot be determined - "
                    f"The marked weight calculation '{name}' is not a valid weight function."
                )

        return found_calculations

    @classmethod
    def __perform_extra_weight_calculations(cls) -> float:
        """
        Perform all identified weight functions for this class

        Returns:
            A weight modifier that may serve as and adjustment to the initial weight calculation
        """
        return sum([
            function()
            for function in cls._get_extra_weight_calculations()
        ])


class StackInfo(BaseModel):
    """
    Describes stack information for where the current line and its path lies in the codebase
    """
    @classmethod
    def create(cls, frame: inspect.FrameInfo):
        """
        Create the information object based on the python object that provides all of the data

        Args:
            frame: The data about currently executed code

        Returns:
            A new instance
        """
        # There's a potential risk if the entire path is given in a stack trace, so we want to limit how much is seen
        if isinstance(settings.base_directory, Path):
            base_directory = str(settings.base_directory.resolve())
        else:
            base_directory = str(Path(settings.base_directory).resolve())

        filepath = str(Path(frame.filename).resolve())

        # If the path of the current file is underneath that of the base directory, we're safe to display
        # everything AFTER it since it won't show any system details, only those of the app
        if filepath.startswith(base_directory):
            filename = filepath.replace(base_directory, "")
            filename = filename[1:] if filename.startswith("/") else filename
        else:
            # There's a reasonable chance that system details like the host os or file structures and
            # locations will be given away if the entire filepath is returned, so we just want to return the
            # last few parts of the path
            filepath_parts = filepath.split(os.pathsep)

            if len(filepath_parts) > 5:
                filename = os.path.join(*filepath_parts[-3:])
            else:
                # If the path is fairly short, only return the filename to be safe
                filename = filepath_parts[-1]

        return cls(
            line_number=frame.lineno,
            function=frame.function,
            code=frame.code_context[0].strip(),
            file=filename
        )

    @classmethod
    def create_full_stack(cls) -> typing.List[StackInfo]:
        """
        Returns:
            A series of stack information showing a stack trace of reasonable length
        """
        full_stack = list()

        # Walk through the entire stack trace minus the two entries - those will just show the creation of this stack
        # trace and nothing of real interest
        for index, frame in enumerate(inspect.stack()[2:]):
            # If the name of the function is `<module>`, it means this function was called as part of an import
            # and stack traces can be quite long, so limit the length by exiting the loop if we've gone too deep
            if frame.function.strip() == "<module>" or index >= MAX_STACK_SIZE:
                break

            # Insert the entry rather than appending it in order to maintain a list of entries in chronological order.
            # The values will appear in the stack in reverse chronological order
            full_stack.insert(
                0,
                cls.create(frame)
            )

        return full_stack

    line_number: int = Field(description="The line in the code that was hit")
    function: str = Field(description="The function that is currently being called")
    code: str = Field(description="The line of code that is being called")
    file: str = Field(description="The name of the file containing the code being called")

    def __str__(self):
        return f"""{self.file}
    {self.function}
        {self.line_number}. {self.code}"""


class HeaderInfo(BaseModel):
    """
    Header-like information to pass along like a standard Http message
    """
    @classmethod
    def create(cls, include_stack: bool = None):
        """
        Create a network header based on the current network state

        Args:
            include_stack: Include information describing what is being called in the codebase

        Returns:
            Header information that may be attached to messages
        """
        # Defer to whether or not debug mode is enabled to the need to include stack information wasn't given
        if include_stack is None:
            include_stack = settings.debug

        args = {
            "caller_application": os.path.basename(sys.argv[0]),
            "caller_function": get_current_function_name(parent_name=True),
            "caller": socket.getfqdn(socket.gethostname()),
            "date": datetime.now().astimezone().strftime(settings.datetime_format),
            "host": socket.gethostbyname(socket.gethostname())
        }

        if include_stack:
            args['trace'] = StackInfo.create_full_stack()

        return cls(**args)

    caller_application: str = Field(description="The application from where the request originated")
    caller_function: str = Field(description="What created the message")
    caller: str = Field(description="The user asking engaging in messaging")
    date: datetime = Field(description="When the request was made")
    host: str = Field(description="Where the request originated")

    trace: typing.Optional[typing.Union[Json[typing.List[StackInfo]], typing.List[StackInfo]]] = Field(
        description="The codepath that ended up creating the message"
    )


class Message(WeightedModel, Mapping):
    """
    The atomic structure describing communication crossing the event stream
    """
    event: str = Field(description="The event that created this message")
    message_id: typing.Optional[str] = Field(
        default=None,
        description="This ID describing the root of where this message came from"
    )
    header: typing.Optional[typing.Union[Json[HeaderInfo], str, HeaderInfo]] = Field(
        default=None,
        description="Optional infomation describing where the message came from"
    )
    application_name: typing.Optional[str] = Field(
        default=None,
        description="The name of the application from where the message originated"
    )
    application_instance: typing.Optional[str] = Field(
        default=None,
        description="The identifier for the instance of the application"
    )
    response_to: typing.Optional[str] = Field(
        default=None,
        description="The id of the message that this is responding to"
    )
    workflow_id: typing.Optional[str] = Field(description="The ID of a workflow if this message is a part of one")
    __extra_data: dict = PrivateAttr(default_factory=dict)
    """A container for extra data that was transmitted but not explicitly defined on the model"""

    @classmethod
    @extra_calculation
    def _adjust_weight_for_literal_event(cls) -> typing.SupportsInt:
        """
        Increase the weight of this class if its event is a literal
        """
        signature = inspect.signature(cls)
        class_parameters = signature.parameters
        event_parameter_annotation = class_parameters['event'].annotation

        # The event is generally just a string. If this message requires a specific event,
        # pump up the value in order to reflect that this message is highly specific
        if event_parameter_annotation != str and event_parameter_annotation != signature.empty:
            return EVENT_LITERAL_ADJUSTER
        else:
            return 0

    def get_event(self) -> str:
        """
        Get the name of the event

        Defined here to help support protocol connectivity
        """
        return self.event

    @classmethod
    def respond_to_message(
        cls,
        request: Message,
        application_name: str,
        application_instance: str,
        data: dict = None,
        **kwargs
    ):
        """
        Create a message as a response to another message

        Used when a type of message needs to be created that does not match that of the requesting message

        Args:
            request: The message to respond to
            application_name: The name of the application responding
            application_instance: The instance of the application responding
            data: Raw, possibly nested, data to include in the message
            **kwargs:

        Returns:
            A new message to respond to
        """
        request_data = request.dict()

        if data:
            request_data.update(data)

        request_data.update(kwargs)

        try:
            response = cls.parse_obj(request_data)

            response.response_to = request.get("message_id", None)

            if response.event == request.event:
                response.event += "_response"

            response.application_name = application_name
            response.application_instance = application_instance

            return response
        except BaseException as exception:
            raise ValueError(
                f"Cannot create a(n) {cls.__name__} message as a response to a(n) {request.__class__.__name__} "
                f"message. More information is needed."
            ) from exception

    def create_response(self, application_name: str, application_instance: str) -> Message:
        """
        Create a simple response to the given message as the same type of message

        Args:
            application_name: The name of the application doing the responding
            application_instance: The instance of the application doing the responding

        Returns:
            A copy of this message ready to respond
        """
        copy = self.__class__.parse_obj(self.dict())
        copy.event += "_response"
        copy['response_to'] = copy.get("message_id", None)
        copy.application_instance = application_instance
        copy.application_name = application_name
        return copy

    def get_data(self) -> dict:
        """
        Get the raw data for this message that does not fit in predefined fields
        """
        return self.__extra_data.copy()

    def get(self, *keys, default=None):
        """
        Get a value from this message by name

        Args:
            *keys: The path to the value within this message. Multiple keys will be used to try to navigate through a contained dictionary
            default: A value to return if the field doesn't exist

        Returns:
            The desired value or the default if the desired value isn't present
        """
        # If there's only one key given and it's a member of the model (whether it's a field or private attribute),
        # return that value or its default
        if len(keys) == 1 and hasattr(self, keys[0]):
            return getattr(self, keys[0], default)

        # Try to find the value within its internal set of extra data
        return get_by_path(self.__extra_data, *keys, default)

    async def send(
        self,
        connection: Redis,
        stream_name: str,
        include_header: bool = None,
        application_name: str = None,
        application_instance: str = None,
        **kwargs
    ):
        """
        Send this message through the given redis connection

        Args:
            connection: The connection to send the message through
            stream_name: The stream to publish this data to
            include_header: Whether to include header information along with the message
            application_name: The name of the application. Required if not already on the message
            application_instance: The instance of the application. Required if not already on the message
            **kwargs:
        """
        # An application identifier is required. Raise an error if one or both can't be found.
        application_identifier_errors: typing.List[str] = list()
        if self.application_name in (None, "") and application_name in ("", None):
            application_identifier_errors.append(
                f"{self.__class__.__name__} Message cannot be sent through the {stream_name} stream - "
                f"it is missing an application name"
            )

        if self.application_instance in (None, "") and application_instance in ("", None):
            application_identifier_errors.append(
                f"{self.__class__.__name__} Message cannot be sent through the {stream_name} stream - "
                f"it is missing an application instance"
            )

        if len(application_identifier_errors) > 0:
            error = ". ".join(application_identifier_errors) + "."
            raise ValueError(error)

        if include_header is None:
            include_header = True

        if include_header and self.header is None:
            self.header = HeaderInfo.create(include_stack=kwargs.get("include_stack")).json()

        # Copy the extra data into a set of key value pairs that will be sent along with the message.
        # This will flatten the data structure.
        key_value_pairs = self.__extra_data.copy()

        # Climb through all fields and attach its data in a format that can be sent through the stream
        for field_name in self.__fields__.keys():  # type: str
            field_value = getattr(self, field_name)

            # 'None' can't go through the stream since it gives incorrect values later, so just don't send it here
            if field_value is None:
                continue
            elif hasattr(field_value, "json") and inspect.isfunction(field_value.json):
                # Convert the data to json if it can be - that's the only way to ensure that it can be
                # parsed correctly later
                key_value_pairs[field_name] = field_value.json()
            elif not isinstance(field_value, (str, bytes, int, float)):
                # If it isn't a natural Redis data type, try to convert it into a form that will be accepted
                try:
                    key_value_pairs[field_name] = json.dumps(field_value)
                except:
                    # Otherwise try to convert it into bytes (Redis' native type) so that it may be sent across the wire
                    key_value_pairs[field_name] = bytes(field_value)
            elif isinstance(field_value, str):
                # Strings need to be converted to bytes before being sent over
                key_value_pairs[field_name] = field_value.encode()
            elif isinstance(field_value, bytes):
                # Bytes are already in the right format before being sent
                key_value_pairs[field_name] = field_value
            else:
                key_value_pairs[field_name] = field_value

        await connection.xadd(stream_name, fields=key_value_pairs, maxlen=settings.approximate_max_stream_length)

    def dict(
        self,
        *,
        include: INCLUDE_EXCLUDE_TYPES = None,
        exclude: INCLUDE_EXCLUDE_TYPES = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        """
        Convert this class instance into a dictionary and ensure that the extra data values are included

        Args:
            include: Fields to make sure are included in the dictionary
            exclude: Fields to exclude from the dictionary
            by_alias: Whether keys should be their field aliases rather than their member names
            exclude_unset: whether fields which were not explicitly set when creating the model should be
                excluded from the returned dictionary
            exclude_defaults: whether fields which are equal to their default values (whether set or otherwise)
                should be excluded from the returned dictionary
            exclude_none: whether fields which are equal to None should be excluded from the returned dictionary

        Returns:
            A dictionary representation of this class instance
        """
        dictionary_representation = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none
        )

        # Add everything not already in the dictionary from the extra data to the dictionary that will be returned
        for key, value in self.get_data():
            if key not in dictionary_representation:
                dictionary_representation[key] = value

        return dictionary_representation

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Add all keyword arguments that aren't fields as extra data
        for key, value in kwargs.items():
            if key in self.__fields__:
                continue

            self.__extra_data[key] = value

    def __getitem__(self, key: Union[int, str]):
        """
        Subscript function for getting data. Will deliver data from either fields or from the extra data

        If an int is given, it will be used as an index in a list of all values starting with the extra data and
        ending with the values from the fields

        Args:
            key: The name or index of the data to get

        Returns:
            The value if there was a key for it
        """
        if isinstance(key, int):
            return list(self.values())[key]

        if key in self.__fields__:
            return getattr(self, key)
        # Try to get the value from the extra data if it wasn't in the fields
        return self.__extra_data[key]

    def __setitem__(self, key: str, value: Any):
        """
        Set the value of a member. Only existing values may be altered

        Args:
            key: The name of the member to change
            value: The new value
        """
        # Set the value normally if it's a field
        if key in self.__fields__:
            setattr(self, key, value)
        elif key not in self.__extra_data:
            # Only existing values may be altered, so if it's not a field and wasn't in extra data when the
            # object was created, fail
            raise KeyError(f"{self.__class__.__name__} does not have a field named '{key}'")
        else:
            self.__extra_data[key] = value

    def __len__(self) -> int:
        """
        Returns:
            The length of the extra data
        """
        return len(self.__extra_data) + len(self.__fields__)

    def keys(self) -> typing.Sequence[str]:
        """
        Returns:
            The keys for all extra data and fields
        """
        data = [
            key
            for key in self.__extra_data.keys()
            if key not in self.__fields__
        ]

        data.extend(self.__fields__.keys())

        return data

    def values(self) -> typing.Sequence[typing.Any]:
        """
        Returns:
            All values from the extra data and fields
        """
        data = [
            value
            for key, value in self.__extra_data.items()
            if key not in self.__fields__
        ]

        for field_name in self.__fields__:
            data.append(getattr(self, field_name))

        return data

    def items(self) -> typing.Sequence[typing.Tuple[str, typing.Any]]:
        """
        Returns:
            A sequence of 2-tuples stored within the instance, with the first value being a key and the second
            being a value
        """
        data: typing.List[typing.Tuple[str, typing.Any]] = [
            (key, value)
            for key, value in self.__extra_data.items()
            if key not in self.__fields__
        ]

        for field_name in self.__fields__:
            data.append((field_name, getattr(self, field_name)))

        return data

    def __iter__(self):
        return iter(self.items())


class GenericMessage(Message):
    """
    A very basic message type that just takes an unstructured dictionary as its 'data' payload
    """
    data: typing.Union[dict, Json[dict]] = Field(description="Unstructured hierarchical data")