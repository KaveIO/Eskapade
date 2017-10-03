# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2017/09/14                                                            *
# * Description:                                                                   *
# *     A collection of generic meta classes for some (design) patterns:           *
# *         - Singleton: Meta class for the Singleton pattern                      *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************


class Singleton(type):

    """Metaclass for singletons.

    Any instantiation of a Singleton class yields the exact same object, e.g.:

    >>> class Klass(metaclass=Singleton):
    >>>     pass
    >>>
    >>> a = Klass()
    >>> b = Klass()
    >>> a is b
    True

    See https://michaelgoerz.net/notes/singleton-objects-in-python.html.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """Return the singleton."""
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]

    @classmethod
    def __instancecheck__(mcs, instance) -> bool:
        if instance.__class__ is mcs:
            return True
        else:
            return isinstance(instance.__class__, mcs)
