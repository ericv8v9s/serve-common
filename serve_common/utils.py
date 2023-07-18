from typing import Union, Callable


class as_context:
    """
    Wraps the provided enter and exit hooks as a context manager.
    """

    def __init__(self, enter=None, exit=None):
        self.enter = enter if enter is not None else lambda: None
        self.exit = exit if exit is not None else lambda: None

    def __enter__(self):
        return self.enter()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.exit()


def repr_call(callable, *args, **kws):
    """
    Constructs a string to represent the call ``callable(*args, **kws)``.
    The string is formatted like the actual call syntax,
    including the function name if available and argument values.
    """
    func = getattr(callable, "__qualname__", repr(callable))
    args = ", ".join(map(repr, args))
    kws = ", ".join(map(lambda kv: f"{kv[0]}={repr(kv[1])}", kws.items()))

    if args == "":
        args = kws
    elif kws != "":
        args = args + ", " + kws
    return f"{func}({args})"


def require(
        test: bool,
        exc: Union[Callable[[], BaseException], BaseException, str] = None):
    if not test:
        if callable(exc):
            exc = exc()
        elif isinstance(exc, str):
            exc = RuntimeError(exc)
        elif exc is None:
            exc = RuntimeError()
        raise exc
