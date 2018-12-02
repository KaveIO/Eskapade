"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/08/23

Description:

   Logging module for Eskapade.

   This module is thin layer on top of the Python logging module
   that provides a simple interface to the user and PEP-3101
   (Advanced String Formatting) with named placeholders.

   Sources of inspiration for this module are:

       - The Twisted Logger interface.
       - The Python logging cookbook and how to.

Authors:
   KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import inspect
import logging
import sys
import time
from enum import IntEnum, unique
from typing import Union, Any

import pendulum


# Protected. Should not be exported.
class _Message(object):
    __slots__ = 'fmt', 'kwargs'

    def __init__(self, fmt: str, kwargs) -> None:
        # WARNING is seven characters.
        self.fmt = fmt
        self.kwargs = kwargs

    def __str__(self) -> str:
        return self.fmt.format(**self.kwargs)


@unique
class LogLevel(IntEnum):
    """Logging level integer enumeration class.

    The enumerations are:

    * NOTSET  ( 0)
    * DEBUG   (10)
    * INFO    (20)
    * WARNING (30)
    * ERROR   (40)
    * FATAL   (50)

    They have the same meaning and value as the logging levels in the
    Python logging module.
    """

    NOTSET = logging.NOTSET
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    FATAL = logging.FATAL

    def __str__(self) -> str:
        return self.name


# Publishers.
class LogPublisher(logging.getLoggerClass()):
    """Logging publisher that listens for log events."""

    def __init__(self, name: str = '', level: Union[int, LogLevel] = LogLevel.NOTSET):
        """Initialize the publisher."""
        super().__init__(name, level)

    def log(self, _level: LogLevel, _msg: str, *args, **kwargs):
        """Log a message with log info."""
        if self.isEnabledFor(_level):
            exc_info = kwargs.pop('exc_info', False)
            extra = kwargs.pop('extra', None)
            stack_info = kwargs.pop('stack_info', False)

            human_time = pendulum.utcfromtimestamp(kwargs.get('log_time', time.time()))
            kwargs['log_time'] = str(human_time)
            # We need LogLevel(level) to convert logging level from logging to LogLevel
            # and get its str representation.
            kwargs['log_level'] = str(kwargs.get('log_level', LogLevel(_level)))
            kwargs['log_namespace'] = kwargs.get('log_namespace', 'publisher')

            _msg = '{log_time!s} [{log_namespace!s}#{log_level!s}] ' + _msg
            self._log(_level,
                      _Message(_msg, kwargs),
                      args,
                      exc_info=exc_info,
                      extra=extra,
                      stack_info=stack_info)

    def add_handler(self, handler: logging.StreamHandler) -> None:
        """Add a log record handler.

        :param handler: The log record handler.
        """
        self.addHandler(handler)

    def remove_handler(self, handler: logging.StreamHandler):
        """Remove a log record handler.

        :param handler: The log record handler.
        """
        self.removeHandler(handler)

    def event(self, event: dict) -> None:
        """Log a logging event.

        This method is called by event emitters, e.g. :class:`Logger`.

        :param event: The event to log.
        :type event: dict
        """
        self.log(event['log_level'], event.pop('log_msg'), **event)

    @property
    def log_level(self):
        """Get logging level.

        :return: The logging level.
        :rtype: LogLevel
        """
        return LogLevel(self.level)

    @log_level.setter
    def log_level(self, level: LogLevel):
        """Set the logging level.

        :param level: The logging level to set/use.
        :type level: LogLevel
        """
        if level in LogLevel:
            self.setLevel(level)
            return

        raise ValueError('Unknown logging level {level!s}'.format(level=level))


# Set LogPublisher to be the logger class.
logging.setLoggerClass(LogPublisher)

# The global application publisher. There should be only one.
global_log_publisher = logging.getLogger('eskapade')


# Handlers
class ConsoleHandler(logging.StreamHandler):
    """A stream handler that sends log messages with levels up to and including WARNING to stdout."""

    def __init__(self):
        """Initialize the ConsoleHandler object."""
        super().__init__(sys.stdout)
        self.max_log_level = LogLevel.WARNING

    def filter(self, record):
        """Determine if a record should be sent to stdout."""
        return record.levelno <= self.max_log_level


class ConsoleErrHandler(logging.StreamHandler):
    """A stream handler that sends log messages with level ERROR and above to stderr."""

    def __init__(self):
        """Initialize the ConsoleHandler object."""
        super().__init__(sys.stderr)
        self.setLevel(LogLevel.ERROR)


class Logger(object):
    """A logger that emits log messages to an observer.

    The logger can be instantiated as a module or class attribute, e.g.

    >>> logger = Logger()
    >>> logger.info("I'm a module logger attribute.")
    >>>
    >>> class Point(object):
    >>>     logger = Logger()
    >>>
    >>>     def __init__(self, x = 0.0, y = 0.0):
    >>>         Point.logger.debug('Initializing {point} with x = {x}  y = {y}', point=Point, x=x, y=y)
    >>>         self._x = x
    >>>         self._y = y
    >>>
    >>>     @property
    >>>     def x(self):
    >>>         self.logger.debug('Getting property x = {point._x}', point=self)
    >>>         return self._x
    >>>
    >>>     @x.setter
    >>>     def x(self, x):
    >>>         self.logger.debug('Setting property y = {point._x}', point=self)
    >>>         self._x = x
    >>>
    >>>     @property
    >>>     def y(self):
    >>>        self.logger.debug('Getting property y = {point._y}', point=self)
    >>>        return self._y
    >>>
    >>>     @y.setter
    >>>     def y(self, y):
    >>>         self.logger.debug('Setting property y = {point._y}', point=self)
    >>>         self._y = y
    >>>
    >>> a_point = Point(1, 2)
    >>>
    >>> logger.info('p_x = {point.x} p_y = {point.y}', point=a_point)
    >>> logger.log_level = LogLevel.DEBUG
    >>> logger.info('p_x = {point.x} p_y = {point.y}', point=a_point)

    The logger uses PEP-3101 (Advanced String Formatting) with named placeholders,
    see <https://www.python.org/dev/peps/pep-3101/>
    and <https://pyformat.info/> for more details and examples.

    Furthermore, logging events are only formatted and evaluated for logging
    levels that are enabled. So, there's no need to check the logging level
    before logging. It's also efficient.
    """

    @staticmethod
    def __namespace_from_calling_context() -> str:
        """Private static method to derive namespace/calling context.

        :return: The calling frame context.
        :rtype: str
        """
        # We use the inspect module to get and traverse the stack.
        # Index 0 - Me.
        # Index 1 - The guy calling me. In this case __init__.
        # Index 2 - The guy calling/instantiating Logger.
        calling_frame_context = inspect.stack()[2].frame.f_globals["__name__"]

        return calling_frame_context

    def __init__(self, name: str = None, source: str = None, observer: Union[LogPublisher, logging.Logger] = None):
        """Initialize logger instance."""
        name = name or self.__namespace_from_calling_context()

        self.name = name
        self.source = source
        self.observer = observer or global_log_publisher

    def __get__(self, instance: Any, instance_type=None):
        """Create or get a logger.

        When used as a descriptor, i.e.::

            # File: a_module.py
            class Klass(object):
                 logger = Logger()

                 def hello(self):
                     self.logger.info('Hello')

        The logger's name will be set to the name of the class it's declared on. In the above example
        the name of the logger is :class:`a_module.Klass`. Also, :attr:`Klass.logger.source` would be
        :class:`a_module.Klass` and :attr:`Klass().logger.source` would be an instance of
        :class:`a_module.Klass`.

        Note that :func:`logging.getLogger` will either create or get a logger with said name.
        """
        source = instance or instance_type
        paths = [instance_type.__module__, instance_type.__name__]
        if not instance_type.__module__.startswith(global_log_publisher.name + '.'):
            paths.insert(0, global_log_publisher.name)
        name = '.'.join(paths)
        return self.__class__(name=name,
                              source=source,
                              observer=logging.getLogger(name))

    def __set__(self, instance: Any, value):
        raise AttributeError('Cannot redefine logger for {instance!s}!'.format(instance=instance))

    def __repr__(self):
        return '<{klass!s} ' \
               'name={namespace!r} ' \
               'source={source!r} ' \
               'observer={observer!r} ' \
               'level={observer_level!r} ' \
               'id=\'{id!r}\'>'.format(klass=self.__class__.__name__,
                                       namespace=self.name,
                                       source=self.source,
                                       observer=self.observer,
                                       observer_level=self.observer.log_level,
                                       id=id(self), )

    def __log(self, log_level: LogLevel, fmt: str = '', **kwargs):
        # In Python 3.6 the order of kwargs is preserved, see PEP 468.
        # So, we are not going to bother to fix it here.
        event = kwargs
        if not kwargs:
            fmt = fmt.replace('{', '{{').replace('}', '}}')
        fmt = fmt or ' '.join(k + ' = {' + k + '}' for k in event.keys())
        event.update(log_time=time.time(),
                     log_level=log_level,
                     log_namespace=self.name,
                     log_source=self.source,
                     log_msg=fmt)
        self.observer.event(event)

    def debug(self, fmt: str = '', **kwargs) -> None:
        """Emit a log event at the DEBUG level.

        DEBUG log events are only formatted and evaluated if DEBUG is enabled.

        :param fmt: The DEBUG event string to be formatted.
        :type fmt: str
        :param kwargs: The placeholder keywords and their values.
        """
        self.__log(LogLevel.DEBUG, fmt, **kwargs)

    def info(self, fmt: str = '', **kwargs) -> None:
        """Emit a log event at the INFO level.

        INFO log events are only formatted and evaluated if INFO is enabled.

        :param fmt: The INFO event string to be formatted.
        :type fmt: str
        :param kwargs: The placeholder keywords and their values.
        """
        self.__log(LogLevel.INFO, fmt, **kwargs)

    def warning(self, fmt: str = '', **kwargs) -> None:
        """Emit a log event at the WARNING level.

        WARNING log events are only formatted and evaluated if WARNING is enabled.

        :param fmt: The WARNING event string to be formatted.
        :type fmt: str
        :param kwargs: The placeholder keywords and their values.
        """
        self.__log(LogLevel.WARNING, fmt, **kwargs)

    def error(self, fmt: str = '', **kwargs) -> None:
        """Emit a log event at the ERROR level.

        ERROR log events are only formatted and evaluated if ERROR is enabled.

        :param fmt: The ERROR event string to be formatted.
        :type fmt: str
        :param kwargs: The placeholder keywords and their values.
        """
        self.__log(LogLevel.ERROR, fmt, **kwargs)

    def fatal(self, fmt: str = '', **kwargs) -> None:
        """Emit a log event at the FATAL level.

        FATAL log events are only formatted and evaluated if FATAL is enabled.

        :param fmt: The ERROR event string to be formatted.
        :type fmt: str
        :param kwargs: The placeholder keywords and their values.
        """
        self.__log(LogLevel.FATAL, fmt, **kwargs)

    @property
    def log_level(self) -> LogLevel:
        """Get the logging level of the logger.

        :return: The logging level.
        :rtype: LogLevel
        """
        return self.observer.log_level

    @log_level.setter
    def log_level(self, level: LogLevel):
        """Set the logging level of the logger.

        :param level:
        :type level: LogLevel
        """
        self.observer.log_level = level


_LOG_LEVEL_MAP = {'NOTSET': LogLevel.NOTSET,
                  'DEBUG': LogLevel.DEBUG,
                  'INFO': LogLevel.INFO,
                  'WARNING': LogLevel.WARNING,
                  'ERROR': LogLevel.ERROR,
                  'FATAL': LogLevel.FATAL
}

def log_level_mapper(log_level: Union[str, LogLevel]) -> LogLevel:
    """Logging level conversion
    """
    if isinstance(log_level, LogLevel):
        return log_level
    elif log_level not in _LOG_LEVEL_MAP:
        raise AssertionError('log level {0:s} unknown.'.format(log_level))
    return _LOG_LEVEL_MAP[log_level]
