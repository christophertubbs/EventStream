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
from configuration.parts import CodeDesignation
from configuration.group import HandlerGroup

handler_config = {
    "name": "Example",
    "event": "execution_complete",
    "stream": "EVENTS",
    "handler": CodeDesignation.from_function(event_stream.handlers.echo_message)
}

handler = HandlerGroup.parse_obj(handler_config)
handler.set_application_name("Whatever Service")
handler.set_instance_identifier(uuid1())
task = handler.launch()
```