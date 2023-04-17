from typing import Union, Callable

import functools
import inspect
from collections import defaultdict


__all__ = [
    "cached",
    "enable_cache_dependency",
    "dependency"
]


def cached(property_or_method):
    """Apply on top of @property or getter method to cache the property."""

    if isinstance(property_or_method, property):
        return _CachedProperty(property_or_method)
    elif callable(property_or_method):
        fget = property_or_method
        return _CachedProperty(property(fget))
    else:
        raise TypeError()


class _CachedProperty:
    def __init__(self, prop: property):
        self.backing_property = prop
        # the set of properties that update this
        self.dependencies: set[str] = set()
        # the set of properties updated by this property
        self.dependents: set[_CachedProperty] = set()
        self.cache_field = None
        self.cache_status = None

    def __set_name__(self, cls, name):
        self.cache_field = f"_cached_property_{name}_cache"
        self.cache_status = f"{self.cache_field}_ready"

    def has_cache(self, instance):
        try:
            return getattr(instance, self.cache_status)
        except AttributeError:
            return False

    def invalidate_cache(self, instance):
        setattr(instance, self.cache_status, False)
        for dependent in self.dependents:
            dependent.invalidate_cache(instance)

    def __get__(self, instance, cls=None):
        if instance is None:
            return self
        if not self.has_cache(instance):
            value = self.backing_property.__get__(instance, cls)
            setattr(instance, self.cache_field, value)
            setattr(instance, self.cache_status, True)
        return getattr(instance, self.cache_field)

    def __set__(self, instance, value):
        if instance is None:
            return self
        self.backing_property.__set__(instance, value)
        self.invalidate_cache(instance)
        setattr(instance, self.cache_field, value)
        setattr(instance, self.cache_status, True)

    def __delete__(self, instance):
        if instance is None:
            return self
        self.backing_property.__delete__(instance)
        self.invalidate_cache(instance)

    def getter(self, fget):
        self.backing_property = self.backing_property.getter(fget)
        return self
    def setter(self, fset):
        self.backing_property = self.backing_property.setter(fset)
        return self
    def deleter(self, fdel):
        self.backing_property = self.backing_property.deleter(fdel)
        return self


def enable_cache_dependency(cls):
    """
    Sets up @cached properties in a class to allow automatic cache clearing
    based on declared dependency relations.
    """

    # Reverse dependency references such that
    #   A (has dependency) B => B (causes update) A
    # gets changed to
    #   B (has dependent) A => B (causes update) A
    # The following statements are equivalent:
    #   A (has dependency) B <=> B (has dependent) A <=> B (causes update) A
    # But the reversed relation (has dependent) aligns the dependency reference
    # with the update causation relation (causes update),
    # which allows easy cache invalidation of A when B is modified.

    for name, prop in inspect.getmembers(
            cls, lambda m: isinstance(m, _CachedProperty)):

        for dependency_name in prop.dependencies:
            dependency = getattr(cls, dependency_name)

            if type(dependency) == property:
                dependency = _CachedProperty(dependency)
                setattr(cls, dependency_name, dependency)
                dependency.__set_name__(cls, dependency_name)

            if isinstance(dependency, _CachedProperty):
                dependency.dependents.add(prop)
            else:
                raise TypeError()

    return cls


def dependency(*properties: Union[property, _CachedProperty, str]):
    dep_names = set()
    for p in properties:
        if isinstance(p, _CachedProperty):
            p = p.backing_property
        if isinstance(p, property):
            # property takes its name from the getter function's name
            p = p.fget.__name__
        dep_names.add(p)

    def deco(prop):
        if not isinstance(prop, _CachedProperty):
            prop = cached(prop)
        prop.dependencies |= dep_names
        return prop
    return deco
