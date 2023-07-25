"""
Decorators that manipulate ``__all__`` in the caller's global namespace.
"""

import inspect


__all__ = [
    "export_as",
    "export"
]


def _export_name(ident: str, stack_depth=2):
    frame = inspect.currentframe()
    try:
        for _ in range(stack_depth):
            frame = frame.f_back

        try:
            __all__ = frame.f_globals["__all__"]
        except KeyError:
            __all__ = []
            frame.f_globals["__all__"] = __all__

        __all__.append(ident)

    finally:
        del frame


def export_as(ident: str):
    _export_name(ident)
    return lambda x: x


def export(obj):
    if isinstance(obj, str):
        _export_name(obj)
    else:
        _export_name(obj.__name__)
    return obj

