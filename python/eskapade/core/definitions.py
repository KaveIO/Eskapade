# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2017/02/27                                                            *
# * Description:                                                                   *
# *      Definitions used in Eskapade runs:                                        *
# *        * logging levels                                                        *
# *        * return-status codes                                                   *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************


from enum import Enum
import logging

# dummy logging level to turn off logging.
logging.OFF = 60

LOG_LEVELS = {
    'NOTSET': logging.NOTSET,
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'FATAL': logging.FATAL,
    'CRITICAL': logging.CRITICAL,
    'OFF': logging.OFF,
    logging.DEBUG: 'DEBUG',
    logging.INFO: 'INFO',
    logging.WARNING: 'WARNING',
    logging.ERROR: 'ERROR',
    logging.FATAL: 'FATAL',
    logging.CRITICAL: 'CRITICAL',
    logging.NOTSET: 'NOTSET',
    logging.OFF: 'OFF'
}


class StatusCode(Enum):
    """Return-status codes for Eskapade run

    A StatusCode object is returned after each initialize, execute, and
    finalize function call of links, chains, and the process manager.
    """

    # all okay
    Success = 1
    # not okay, but can continue
    Recoverable = 2
    # skip this chain
    SkipChain = 3
    # failure means quit
    Failure = 4
    # repeat this chain
    RepeatChain = 5
    # undefined = default status
    Undefined = 6

    def isSuccess(self):
        """Check if status is "Success"

        :rtype: bool
        """
        return StatusCode.Success == self

    def isRecoverable(self):
        """Check if status is "Recoverable"

        :rtype: bool
        """
        return StatusCode.Recoverable == self

    def isSkipChain(self):
        """Check if status is "SkipChain"

        :rtype: bool
        """
        return StatusCode.SkipChain == self

    def isRepeatChain(self):
        """Check if status is "RepeatChain"

        :rtype: bool
        """
        return StatusCode.RepeatChain == self

    def isFailure(self):
        """Check if status is "Failure"

        :rtype: bool
        """
        return StatusCode.Failure == self

    def isUndefined(self):
        """Check if status is "Undefined"

        :rtype: bool
        """
        return StatusCode.Undefined == self
