# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2017/08/23                                                            *
# * Description:                                                                   *
# *      Simple logger descriptor inspired by Twisted from Twisted Matrix Labs.    *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import inspect
import logging
import sys
import time
from enum import IntEnum, unique

import pendulum


class _Message:
    def __init__(self, fmt: str, kwargs):
        self.fmt = '{log_time} - {log_level:<10} - {log_namespace:<20} : ' + fmt
        self.kwargs = kwargs

    def __str__(self):
        return self.fmt.format(**self.kwargs)


@unique
class LogLevel(IntEnum):
    NOTSET = logging.NOTSET
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    FATAL = logging.FATAL

    def __str__(self):
        return self.name


class LogPublisher(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})

        if self.logger.level == LogLevel.NOTSET:
            self.logger.setLevel(LogLevel.INFO)

    def log(self, level: LogLevel, msg: str, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)

            exc_info = kwargs.pop('exc_info', False)
            extra = kwargs.pop('extra', None)
            stack_info = kwargs.pop('stack_info', False)

            human_time = pendulum.utcfromtimestamp(kwargs.get('log_time', time.time()))
            kwargs['log_time'] = str(human_time)
            # We need LogLevel(level) to convert logging level to LogLevel.
            kwargs['log_level'] = str(kwargs.get('log_level', LogLevel(level)))
            kwargs['log_namespace'] = kwargs.get('log_namespace', 'publisher')

            self.logger.log(level,
                            _Message(msg, kwargs),
                            *args,
                            exc_info=exc_info,
                            extra=extra,
                            stack_info=stack_info)

    def add_handler(self, handler):
        self.logger.addHandler(handler)

    def remove_handler(self, handler):
        self.logger.removeHandler(handler)

    def event(self, event):
        self.log(event['log_level'], event.pop('log_msg'), **event)

    @property
    def name(self):
        return self.logger.name

    @property
    def log_level(self):
        return self.logger.level

    @log_level.setter
    def log_level(self, level):
        self.logger.setLevel(level)


# global publisher.
global_log_publisher = LogPublisher(logging.getLogger('eskapade'))


# Handlers
class ConsoleHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)


class ConsoleErrHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__(sys.stderr)
        self.setLevel(LogLevel.ERROR)


class Logger:
    """A logger emits log messages. You should instantiate it as a class or module attribute.
    """

    @staticmethod
    def _namespace_from_calling_context() -> str:
        """Derive namespace from the module containing the caller's caller.

        :return: The calling frame context.
        :rtype: str
        """
        # 0 - caller is me.
        # 1 - caller is init.
        # 2 - caller is outside.
        calling_frame_context = inspect.stack()[2].frame.f_globals["__name__"]

        return calling_frame_context

    def __init__(self, name=None, source=None, observer=None):
        name = name or self._namespace_from_calling_context()

        self.name = name
        self.source = source
        self.observer = observer or global_log_publisher

    def __get__(self, object_self, a_type=None):
        source = object_self or a_type
        name = '.'.join([self.observer.name, a_type.__module__, a_type.__name__])
        logger = logging.getLogger(name)

        return self.__class__(
            name=name,
            source=source,
            observer=LogPublisher(logger)
        )

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
                                       id=id(self),
                                       )

    def __log(self, level, fmt: str = '', **kwargs):
        event = kwargs
        event.update(log_time=time.time(),
                     log_level=level,
                     log_namespace=self.name,
                     log_source=self.source,
                     log_msg=fmt)
        self.observer.event(event)

    def debug(self, fmt: str = '', **kwargs):
        self.__log(LogLevel.DEBUG, fmt, **kwargs)

    def info(self, fmt: str = '', **kwargs):
        self.__log(LogLevel.INFO, fmt, **kwargs)

    def warning(self, fmt: str = '', **kwargs):
        self.__log(LogLevel.WARNING, fmt, **kwargs)

    def error(self, fmt: str = '', **kwargs):
        self.__log(LogLevel.ERROR, fmt, **kwargs)

    def fatal(self, fmt: str = '', **kwargs):
        self.__log(LogLevel.FATAL, fmt, **kwargs)


def set_logging_level(logger: Logger, level: LogLevel = LogLevel.INFO) -> None:
    """ Set the logging level for a logger.

    :param logger: The logger.
    :param level: The new logging level for the logger.
    """

    logger.observer.log_level = level
