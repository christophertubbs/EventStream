# Event Stream

This is a python package designed to facilitate event based communication through Redis Streams.

## How to Run

Running an **Event Stream** application is as simple as calling:

```bash
$ python ./event_stream/application.py example.json
```

## I don't want a whole application - I just want an event handler

Good news! The application itself just loads the handlers and adds their tasks to an event loop. 
Mimic the config and launching logic and you can do pretty much whatever you want. The most difficult part is 
just keeping track and managing that task as everything is going on.

For example:

```python
from uuid import uuid1

import event_stream.handlers
from event_stream.configuration.parts import CodeDesignation
from event_stream.configuration.group import HandlerGroup

from event_stream.streams.handlers import HandlerReader

handler_config = {
    "name": "Example",
    "event": "execution_complete",
    "stream": "EVENTS",
    "handler": CodeDesignation.from_function(event_stream.handlers.echo_message)
}

handler_configuration = HandlerGroup.parse_obj(handler_config)
handler_configuration.set_application_name("Whatever Service")
handler_configuration.set_instance_identifier(str(uuid1()))

handler = HandlerReader(configuration=handler_configuration, verbose=True)
task = handler.launch()
```