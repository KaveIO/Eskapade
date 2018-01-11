"""Project: Eskapade - A python-based package for data analysis.

Created: 2016/11/08

Classes: ArgumentsMixin, TimerMixin

Description:
    Mixin classes:
        - ArgumentsMixin: processes/checks arguments and sets them as attributes
        - TimerMixin:     keeps track of execution time
        - ConfigMixin:    reads and handles settings from configuration files

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import configparser
import timeit


class ArgumentsMixin:
    """Mixin base class for argument parsing.

    Class allows attributes to be accessed as dict items.  Plus several
    argument processing helper functions.
    """

    def __contains__(self, key, *args, **kwargs):
        """Check if key is present.

        Dict style __contains__ function.
        Note: works for attributes, but does not work for properties.
        """
        if key in vars(self):
            return hasattr(self, key)
        super_meth = getattr(super(ArgumentsMixin, self), '__contains__', None)
        if not super_meth:
            return False
        return super_meth(key, *args, **kwargs)

    def __getitem__(self, key, *args, **kwargs):
        """Get item.

        Dict style __getitem__ function.
        Note: works for attributes, but does not work for properties.
        """
        if key in vars(self):
            return getattr(self, key)
        super_meth = getattr(super(ArgumentsMixin, self), '__getitem__', None)
        if not super_meth:
            raise KeyError('{!s} does not contain item "{!s}"'.format(self, key))
        return super_meth(key, *args, **kwargs)

    def __setitem__(self, key, item, *args, **kwargs):
        """Set item.

        Dict style __setitem__ function.
        """
        if isinstance(key, str):
            return setattr(self, key, item)
        super_meth = getattr(super(ArgumentsMixin, self), '__setitem__', None)
        if not super_meth:
            raise TypeError('item name must be a string, not "{}"'.format(type(key).__name__))
        return super_meth(key, item, *args, **kwargs)

    def _process_kwargs(self, kwargs, **name_val):
        """Add names from kwargs as attributes."""
        if not name_val:
            name_val = kwargs.copy()
        for name, def_val in name_val.items():
            setattr(self, name, kwargs.pop(name, def_val))

    def check_extra_kwargs(self, kwargs):
        """Check for residual kwargs."""
        for key in list(kwargs.keys()):
            if hasattr(self, key):
                kwargs.pop(key)
        if kwargs:
            raise RuntimeError('extraneous keyword arguments for {!s}: {!s}'.format(self, kwargs))

    def check_required_args(self, *arg_names):
        """Check if set of arguments exists as attributes."""
        for arg_name in arg_names:
            if not hasattr(self, arg_name):
                raise AttributeError('argument "{}" not set for {!s}'.format(arg_name, self))

    def check_arg_vals(self, *arg_names, allow_none=False):
        """Check if set of arguments exists as attributes and values."""
        self.check_required_args(*arg_names)
        for arg_name in arg_names:
            attr = getattr(self, arg_name)
            if not (allow_none and attr is None) and not bool(attr):
                raise ValueError('argument "{}" of {!s} has no value'.format(arg_name, self))

    def check_arg_opts(self, allow_none=False, **name_vals):
        """Check if argument values are in set of options."""
        self.check_required_args(*tuple(name_vals.keys()))
        for arg_name, arg_opts in name_vals.items():
            attr = getattr(self, arg_name)
            if not (allow_none and attr is None) and attr not in arg_opts:
                raise ValueError('invalid value for argument "{}" of {!s}: "{!s}" (options are {!s})'
                                 .format(arg_name, self, attr, tuple(arg_opts)))

    def check_arg_types(self, recurse=False, allow_none=False, **name_type):
        """Check if set of arguments has correct types."""
        def check_attr(self, attr, a_name, a_type):
            """Check if argument has correct type."""
            if allow_none and attr is None:
                return
            if not isinstance(attr, a_type):
                if recurse and hasattr(attr, '__iter__'):
                    for a in attr:
                        check_attr(self, a, a_name, a_type)
                else:
                    raise TypeError('type of (element of) argument "{}" of {!s} is "{}" ("{}" required)'
                                    .format(a_name, self, type(attr).__name__, a_type.__name__))

        self.check_required_args(*tuple(name_type.keys()))
        for arg_name, arg_type in name_type.items():
            check_attr(self, getattr(self, arg_name), arg_name, arg_type)

    def check_arg_iters(self, *arg_names, allow_none=False):
        """Check if set of arguments has iterators."""
        self.check_required_args(*arg_names)
        for arg_name in arg_names:
            attr = getattr(self, arg_name)
            if allow_none and attr is None:
                continue
            if not hasattr(getattr(self, arg_name), '__iter__'):
                raise TypeError('argument "{}" of {!s} is not iterable'.format(arg_name, self))

    def check_arg_callable(self, *arg_names, allow_none=False):
        """Check if set of arguments has iterators."""
        self.check_required_args(*arg_names)
        for arg_name in arg_names:
            attr = getattr(self, arg_name)
            if allow_none and attr is None:
                continue
            if not callable(getattr(self, arg_name)):
                raise TypeError('argument "{}" of {!s} is not callable'.format(arg_name, self))


class TimerMixin:
    """Mixin base class for timing."""

    def __init__(self):
        """Initialize timer."""
        self._start_time = 0.
        self._stop_time = 0.
        self._total_time = 0.

    def start_timer(self):
        """Start run timer.

        Start the timer. The timer is used to compute the run time.  The
        returned timer start value has an undefined reference and should,
        therefore, only be compared to other timer values.

        :returns: start time in seconds
        :rtype: float
        """
        self._start_time = timeit.default_timer()
        return self._start_time

    def stop_timer(self, start_time=None):
        """Stop the run timer.

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
        """Return the total run time.

        :returns: total time in seconds
        :rtype: float
        """
        return self._total_time


class ConfigMixin:
    """Mixin base class for configuration settings."""

    def __init__(self, config_path=None):
        """Initialize config settings.

        :param str config_path: path of configuration file
        """
        self._config_path = str(config_path) if config_path else ''
        self._config = None

    @property
    def config_path(self):
        """Path of configuration file."""
        return self._config_path

    @config_path.setter
    def config_path(self, path):
        """Set path of configuration file.

        :param str path: path to be set
        :raises ValueError: if path is not a string or has no value
        """
        path = str(path) if path else ''
        if not path:
            raise ValueError('no value specified for config-file path')
        self._config_path = path

    def get_config(self, config_path=None):
        """Get settings from configuration file.

        Read and return the configuration settings from a configuration file.
        If the path of this file is not specified as an argument, the value of
        the "config_path" property is used.  If the file has already been read,
        return previous settings.

        :param str config_path: path of configuration file
        :returns: configuration settings read from file
        :rtype: configparser.ConfigParser
        :raises RuntimeError: if config_path is not set
        """
        if not self._config:
            # set config-path attribute
            if config_path:
                self.config_path = config_path
            elif not self.config_path:
                raise RuntimeError('no configuration-file path set')

            # read configuration file
            self._config = configparser.ConfigParser()
            self._config.read(self.config_path)

        return self._config

    def reset_config(self):
        """Remove previously read settings."""
        self._config = None
