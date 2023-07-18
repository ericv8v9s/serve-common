from typing import TypeVar
import inspect


__all__ = [
    "export"
]


def _export_name(ident: str, stack_depth=2):
    frame = inspect.currentframe()
    try:
        for _ in range(stack_depth):
            frame = frame.f_back
            if frame is None:  # stack wasn't deep enough for export
                return  # yes, the finally block does execute

        try:
            __all__ = frame.f_globals["__all__"]
        except KeyError:
            module = frame.f_globals["__name__"]
            raise RuntimeError(f"{module}.__all__ not declared")

        if ident not in __all__:
            module = frame.f_globals["__name__"]
            raise RuntimeError(f"exported name not in __all__: {module}.{ident}")

    finally:
        del frame


T = TypeVar('T')
def export(obj: T) -> T:
    """
    Marks this object as exported:
    checks ``__all__`` for an entry with the name of this object,
    and raises an error if such an entry is missing.

    If the object is a str, it is checked for as is;
    otherwise, its name from ``__name__`` is used.

    The object is returned untouched.
    """

    if isinstance(obj, str):
        _export_name(obj)
    else:
        _export_name(obj.__name__)
    return obj
