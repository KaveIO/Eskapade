"""Project: Eskapade - A python-based package for data analysis.

Classes: TimePeriod, FreqTimePeriod

Created: 2017/03/14

Description:
    Time period and time period with frequency.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

modification, are permitted according to the terms listed in the file
Redistribution and use in source and binary forms, with or without
LICENSE.
"""

import pandas as pd

from escore.core.mixin import ArgumentsMixin
from eskapade.logger import Logger


class TimePeriod(ArgumentsMixin):
    """Time period."""

    logger = Logger()

    def __init__(self, **kwargs):
        """Initialize TimePeriod instance."""
        pass

    def period_index(self, dt):
        """Get number of periods until date/time "dt".

        :param dt: specified date/time
        """
        self.logger.fatal('period_index method not implemented for {cls}; please implement derived class.',
                          cls=self.__class__.__name__)
        raise NotImplementedError('period_index function is not implemented.')

    @classmethod
    def parse_time_period(cls, period):
        """Try to parse specified time period.

        :param period: specified period
        """
        # catch single value
        if not isinstance(period, dict):
            period = dict(value=period)

        # try to parse specified period
        try:
            return pd.Timedelta(**period).delta
        except Exception as ex:
            cls.logger.fatal('Unable to parse period: {period!s}.', period=period)
            raise ex

    @classmethod
    def parse_date_time(cls, dt):
        """Try to parse specified date/time.

        :param dt: specified date/time
        """
        try:
            return pd.Timestamp(dt).value
        except Exception as ex:
            cls.logger.fatal('Unable to parse date/time: {dt!s}', dt=dt)
            raise ex


class UniformTsTimePeriod(TimePeriod):
    """Time period with offset."""

    def __init__(self, **kwargs):
        """Initialize TimePeriod instance."""
        super(UniformTsTimePeriod, self).__init__(**kwargs)

        # get parameters from arguments
        self._process_kwargs(kwargs, period='1d', offset=0)

        # check for extraneous keyword arguments
        self.check_extra_kwargs(kwargs)

        # check value of period
        if self.period <= 0:
            self.logger.fatal('Invalid time period specified: {period:d} ns.', period=self.period)
            raise AssertionError('Time period must be greater than zero.')

    def period_index(self, dt):
        """Get number of periods until date/time "dt" since "offset", given specified "period".

        :param dt: specified date/time
        """
        return (pd.Timestamp(dt).value - self.offset) // self.period

    @property
    def period(self):
        """Get period parameter."""
        return self._period

    @period.setter
    def period(self, period):
        """Set period parameter.

        :param period: specified period parameter
        """
        self._period = self.parse_time_period(period)

    @property
    def offset(self):
        """Get offset parameter."""
        return self._offset

    @offset.setter
    def offset(self, offset):
        """Set offset parameter.

        :param offset: specified offset parameter
        """
        self._offset = self.parse_date_time(offset)


class FreqTimePeriod(TimePeriod):
    """Time period with frequency."""

    def __init__(self, **kwargs):
        """Initialize TimePeriod instance."""
        super(FreqTimePeriod, self).__init__(**kwargs)

        # get parameters from arguments
        self._process_kwargs(kwargs, freq='D')

        # check for extraneous keyword arguments
        self.check_extra_kwargs(kwargs)

    def period_index(self, dt):
        """Return number of periods until date/time "dt" since 1970-01-01.

        :param dt: specified date/time parameter
        """
        return pd.Period(freq=self.freq, value=pd.Timestamp(dt)).ordinal

    def dt_string(self, period_index):
        """Convert period index into date/time string (start of period).

        :param int period_index: specified period index value.
        """
        return str(pd.Period(freq=self.freq, ordinal=period_index).start_time)

    @property
    def freq(self):
        """Return frequency."""
        return self._freq

    @freq.setter
    def freq(self, freq):
        """Try to construct a period object with specified "frequency".

        :param freq: specified frequency
        """
        try:
            per = pd.Period(freq=freq, value='1970-01-01')
        except Exception as ex:
            self.logger.fatal('Invalid frequency specified: {freq!s}.', freq=freq)
            raise ex

        # set "frequency"
        self._freq = per.freq
