# serve-common

This is a library for writing web services.
It's a collection of modules written during the development of other projects.


## Modules

The following are introductory overviews of the modules.
You are still recommended to read the docstrings and source files.


### `serve_common.cached_property`

Provides cached properties and related utilities.

`cached` is like the built-in [`property`][property],
except the value is computed once on first read and cached thereafter.
A cached property can declare dependencies on other properties,
which will automatically invalidate its cache
when any of its dependencies is assigned a new value.

```python
@enable_cache_dependency
class C:
    def __init__(self):
        self._var = 42

    @cached
    @property
    def some_var(self):
        return self._var

    @some_var.setter
    def some_var(self, val):
        self._var = val

    @dependency(some_var)
    @cached
    def double_var(self):
        return 2 * self.some_var
```

See docstrings and source for details.

[property]: https://docs.python.org/3/library/functions.html#property


### `serve_common.callback`

Possibly misnamed,
this module allows functions to be registered for and triggered by events.

Functions can be `register`ed for events or patterns of events,
and will be called when events of the registered names are triggered.

An event can be triggered by calling `notify`
with the desired event name and data,
which will then be passed to the registered functions.

This module needs to be `initialize`d before usage,
which, among other things, starts a new thread to collect events
and invoke the registered functions (i.e., an event loop).
It can optionally be initialized to communicate
over the IPC module (see [`serve_common.ipc`](#serve_commonipc)),
which allows sending events to and receiving them from other processes.
Note that this is limited to events, i.e., `notify`;
`register` calls are not reflected across processes.


### `serve_common.config`

A module for managing application configuration.

Configuration is stored in `config.toml`
in the [TOML](https://toml.io/) format.
The module reads this file and initializes a singleton object `config`,
which is used for all configuration access.

The object `config` supports methods similar to `dict`,
and can be subscripted with string configuration "paths".

```python
from serve_common.config import config
a = config["x.y.z"]
b = config.get("may.or.may.not.exist", default="fallback")
config["f.g.h"] = a
```

A configuration path is simply a string of dot-separated configuration keys.
See the `lookup` function for the implementation (it's only 7 lines).


### `serve_common.default`

From module docstring:

> This module provides the default, opinionated implementations
> of various common functionalities for convenience.
> Here we sacrifice versatility for ease of use.

The author used [gunicorn][gunicorn] and [falcon][falcon],
and some of the boilerplate code specific to those frameworks are put here.

[gunicorn]: https://gunicorn.org/
[falcon]: https://falcon.readthedocs.io/en/stable/


### `serve_common.default.falcon`

Defines `setup_falcon`, which sets up a default falcon application
with middlewares that adds a request id
(see [`serve_common.request_id`][requestid])
and times each request (see [`serve_common.log_util`][requesttime]),
then registers it to generate Open API specifications
(using [SpecTree][spectree]).
By default, also adds routes to the default management endpoints
(see [`serve_common.default.management`][manage]).

[requestid]: #serve_commonrequest_id
[requesttime]: #serve_commonlog_util
[spectree]: https://github.com/0b01001001/spectree
[manage]: #serve_commondefaultmanagement


### `serve_common.default.logging`

A default logging setup using [loguru](https://github.com/Delgan/loguru).
Call the `add_output_*` functions at application startup
to setup the logger outputs.


### `serve_common.default.management`

Default management endpoints.

- `/` and `/manage/spec`:
  responds to `GET`s with an Open API specification JSON document
  for all registered routes in the current application.
- `/manage/version`: on `GET`, returns the version string.
  The version string can be changed by assigning to the `version` attribute
  of this module.
- `/manage/logging/level`
  - on `GET`, returns the current logging level;
  - on `POST`, changes the application logging level.

  See source for the format of the `GET` response and `POST` request bodies.


### `serve_common.export`

Decorators that manipulate `__all__` in the caller's global namespace.

From the [Python Language Reference][PLR],
section 7.11 on the `import` statement:

> The *public names* defined by a module are determined
> by checking the module's namespace for a variable named `__all__`;
> if defined, it must be a sequence of strings which are names defined
> or imported by that module.
> The names given in `__all__` are all considered public
> and are required to exist.

[PLR]: https://docs.python.org/3.11/reference/simple_stmts.html

When an object is `export`ed,
its name will be added to `__all__` in the module it was defined in.
This implicitly sets up the `__all__` variable
and makes the module suitable for wild card imports.

(If you do not understand the above,
you do not need to care about this module
and you can ignore the `@export` decorations.)


### `serve_common.ipc`

Message based IPC using Unix domain sockets.

This module operates using a dedicated IPC server process.
Interested parties (typically child processes)
connect to the server process and join "groups" identified by string ID's.
Each message sent to a group is forwarded to all other members of the group.

The server process maintains a listening socket
and manages the groups and their members.
When a connection is accepted,
the server expects the first message to identify
which group the peer would like to join.
The server does not inspect any subsequent messages,
and forwards them verbatim to other members in the group.

The server process is started by calling the `start` function
defined in `serve_common.ipc.ipc`.
During initialization,
the server process sets up the listening socket
and writes its address to standard output,
which is read by the calling parent process as part of `start`.
The parent process stores this address and sets up its copy of this module
to communicate with the newly started server,
then also writes the address to an environment variable.
Child processes will initialize their IPC modules using the `setup` function,
which by default checks the inherited environment variable
and uses it to establish communication with the the same server.

Because this initialization is based on an environment variable,
`start` should be invoked before any child processes are started,
and therefore should happend early in the application startup.
For gunicorn applications,
you may want to consider a two stage startup
that first runs a plain Python application to start the IPC server
(and anything else if appropriate),
then at the end starts gunicorn using one of the `exec` family of functions.

Once the module is setup using either `start` or `setup`,
it can be used to send messages through the IPC server
by joining a group using `join_group`.
The group ID's are typically constants in your application.
A group does not need to be created beforehand:
the first member of a group creates that group.

`join_group` returns regular [`socket`][socket]s.
The returned sockets are connected and already put in the specified group
(the first greeting message was sent automatically).
The socket will be of the `AF_UNIX` family, of type `SOCK_SEQPACKET`.
Although you may use these sockets directly to send raw byte buffers,
it may be more convenient to use the `send` and `recv`
from `serve_common.ipc.sockutil`,
which allows arbitrary Python objects to be sent and received directly
(through automatic pickling and unpickling).

[socket]: https://docs.python.org/3/library/socket.html

The IPC server can be shutdown at anytime from any process
by calling the `shutdown` function from `serve_common.ipc.ipc`,
though typically it is only invoked on application shutdown.
The server also shuts down automatically when it detects
that it has be adopted by the init process
(i.e., its parent process has died).
Note that it explicitly ignores the `SIGINT` signal.
This is because a `^C` in bash
sends the interrupt to all processes in the *process group*,
which causes the IPC server to initiate shutdown
along side other processes that may depend on it,
creating a race condition.

The functions defined in sub-modules of this module mentioned above
are imported in the module's `__init__.py`.
For most usages you should only need to import the `serve_common.ipc` module.


### `serve_common.log_util`

The three objects here are related to logging
but have little to do with each other.
Perhaps you can help find a better place to put them.

`LogLevelFilter` is a loguru filter to allow changing log level on a sink.
It filters out everything below the log level it's set to.
This level can be modified,
and is automatically updated when the `logging.level` config value changes.
See [loguru documentation][loguru-logger].

`RequestTimer` is a falcon middleware used to time each request.
See [falcon documentation regarding middleware][falcon-midware].

`time_and_log` is a decorator and context manager
to time and log a block or function.

```python
@time_and_log("running f")
def f(x):
    return 2 * x
# running f: 1506 ns

with time_and_log("sleeping"):
    import time
    time.sleep(3)
# sleeping: 3000132280 ns
```

[loguru-logger]: https://loguru.readthedocs.io/en/stable/api/logger.html
[falcon-midware]: https://falcon.readthedocs.io/en/stable/api/middleware.html


### `serve_common.reloading`

Implementations of file reloading.
The current sole implementation, `PollingReloader`,
checks the modification time of the target file every second
and invokes the specified reload function
when it finds it newer than the previous check.

See `serve_common.config` for a usage example,
where this module is used to reload the config file.

Maybe you can write something more efficient using inotify or something.
(Note that the inotify package on PyPI is GPL,
which is not compatible with CC0.)


### `serve_common.request_id`

Defines a thread local variable and a falcon middleware
for tracking each request with an UUID.

This is primarily used for logging.
It allows a request id to be printed automatically
even when getting a reference to the request itself
would otherwise be difficult.
