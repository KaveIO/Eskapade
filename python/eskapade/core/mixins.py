# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Classes: ArgumentsMixin, LoggingMixin                                          *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *          Base classes of Link                                                  *
# *          ArgumentsMixin: allows attributes to be accessed as dict items.       *
# *          LoggingMixin: logger functionality for the class.                     *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
import timeit


class ArgumentsMixin(object):
    """Mixin base class for argument parsing

    Class allows attributes to be accessed as dict items.  Plus several
    argument processing helper functions.
    """

    def __contains__(self, key, *args, **kwargs):
        """Dict style __contains__ function

        Note: works for attributes, but does not work for properties.
        """

        if key in vars(self):
            return hasattr(self, key)
        super_meth = getattr(super(ArgumentsMixin, self), '__contains__', None)
        if not super_meth:
            return False
        return super_meth(key, *args, **kwargs)

    def __getitem__(self, key, *args, **kwargs):
        """Dict style __getitem__ function

        Note: works for attributes, but does not work for properties.
        """

        if key in vars(self):
            return getattr(self, key)
        super_meth = getattr(super(ArgumentsMixin, self), '__getitem__', None)
        if not super_meth:
            raise KeyError('%s does not contain item "%s"' % (str(self), str(key)))
        return super_meth(key, *args, **kwargs)

    def __setitem__(self, key, item, *args, **kwargs):
        """Dict style __setitem__ function"""

        if isinstance(key, str):
            return setattr(self, key, item)
        super_meth = getattr(super(ArgumentsMixin, self), '__setitem__', None)
        if not super_meth:
            raise TypeError('item name must be a string, not "%s"' % type(key).__name__)
        return super_meth(key, item, *args, **kwargs)

    def _process_kwargs(self, kwargs, **name_val):
        """Add names from kwargs as attributes"""

        if not name_val:
            name_val = kwargs.copy()
        for name, def_val in name_val.items():
            setattr(self, name, kwargs.pop(name, def_val))

    def check_extra_kwargs(self, kwargs):
        """Check for residual kwargs"""

        for key in list(kwargs.keys()):
            if hasattr(self, key):
                kwargs.pop(key)
        if kwargs:
            raise RuntimeError('extraneous keyword arguments for %s: %s' % (str(self), str(kwargs)))

    def check_required_args(self, *arg_names):
        """Check if set of arguments exists as attributes"""

        for arg_name in arg_names:
            if not hasattr(self, arg_name):
                raise AttributeError('argument "%s" not set for %s' % (arg_name, str(self)))

    def check_arg_vals(self, *arg_names):
        """Check if set of arguments exists as attributes and values"""

        self.check_required_args(*arg_names)
        for arg_name in arg_names:
            if not getattr(self, arg_name):
                raise ValueError('argument "%s" of %s has no value' % (arg_name, str(self)))

    def check_arg_types(self, recurse=False, allow_none=False, **name_type):
        """Check if set of arguments has correct types"""

        def check_attr(self, attr, a_name, a_type):
            if allow_none and attr is None:
                return
            if not isinstance(attr, a_type):
                if recurse and hasattr(attr, '__iter__'):
                    for a in attr:
                        check_attr(self, a, a_name, a_type)
                else:
                    raise TypeError('type of (element of) argument "%s" of %s is "%s" ("%s" required)'
                                    % (a_name, str(self), type(attr).__name__, a_type.__name__))

        self.check_required_args(*list(name_type.keys()))
        for arg_name, arg_type in name_type.items():
            check_attr(self, getattr(self, arg_name), arg_name, arg_type)

    def check_arg_iters(self, *arg_names):
        """Check if set of arguments has iterators"""

        self.check_required_args(*arg_names)
        for arg_name in arg_names:
            if not hasattr(getattr(self, arg_name), '__iter__'):
                raise TypeError('argument "%s" of %s is not iterable' % (arg_name, str(self)))


class LoggingMixin(object):
    """Mixin base class for logging"""

    @classmethod
    def log(cls):
        """Get logger of the module of this class"""

        return logging.getLogger(cls.__module__)

    @classmethod
    def set_log_level(cls, level):
        """Set logging level for the module of this class"""

        try:
            cls.log().setLevel(level)
            cls.log().debug('logging level of "%s" set to "%s"', cls.log().name, level)
        except ValueError:
            cls.log().error('logging level of "%s" unchanged: got invalid value "%s"', cls.log().name, level)


class TimerMixin(object):
    """Mixin base class for timing"""

    def __init__(self):
        """Initialize timer"""

        self._start_time = 0.
        self._stop_time = 0.
        self._total_time = 0.

    def start_timer(self):
        """Start run timer

        Start the timer. The timer is used to compute the run time.  The
        returned timer start value has an undefined reference and should,
        therefore, only be compared to other timer values.

        :returns: start time in seconds
        :rtype: float
        """

        self._start_time = timeit.default_timer()
        return self._start_time

    def stop_timer(self, start_time=None):
        """Stop the run timer

        Stop the timer.  The timer is used to compute the run time.  The
        elapsed time since the timer start is returned.

        :param float start_time: function start_time input
        :returns: time difference with start in seconds
        :rtype: float
        """

        self._stop_time = timeit.default_timer()

        diff_time = self._stop_time - (start_time if start_time is not None else self._start_time)
        self._total_time += diff_time

        return diff_time

    def total_time(self):
        """Return the total run time

        :returns: total time in seconds
        :rtype: float
        """

        return self._total_time
