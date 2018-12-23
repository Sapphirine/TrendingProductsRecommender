import collections
import functools


class Memoized(object):
    """Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    """

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args, **kwargs):
        for arg in args:
            if not isinstance(arg, collections.Hashable):
                # uncacheable. a list, for instance.
                # better to not cache than blow up.
                return self.func(*args)
        for _,arg in kwargs.items():
            if not isinstance(arg, collections.Hashable):
                # uncacheable. a list, for instance.
                # better to not cache than blow up.
                return self.func(*args)
        kwargs_hashable = tuple(sorted(kwargs.items()))
        if (args,kwargs_hashable) in self.cache and ('cache_clear' not in kwargs or not kwargs['cache_clear']):
            return self.cache[(args,kwargs_hashable)]
        else:
            value = self.func(*args,**kwargs)
            self.cache[(args,kwargs_hashable)] = value
            return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)
