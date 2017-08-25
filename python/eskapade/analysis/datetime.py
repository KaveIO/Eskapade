import pandas as pd

from eskapade.mixins import ArgumentsMixin, LoggingMixin


################
# time periods #
################


class TimePeriod(ArgumentsMixin, LoggingMixin):

    def __init__(self, **kwargs):
        """ initialize TimePeriod instance
        """
        pass

    def period_index(self, dt):
        """ get number of periods until date/time "dt"

        :param dt: specified date/time
        """
        self.log().critical('period_index method not implemented for %s; please implement derived class',
                            self.__class__.__name__)
        raise NotImplementedError('period_index function is not implemented')

    @classmethod
    def parse_time_period(cls, period):
        """ try to parse specified time period

        :param period: specified period
        """
        # catch single value
        if not isinstance(period, dict):
            period = dict(value=period)

        # try to parse specified period
        try:
            return pd.Timedelta(**period).delta
        except Exception as ex:
            cls.log().critical('unable to parse period: %s', str(period))
            raise ex

    @classmethod
    def parse_date_time(cls, dt):
        """ try to parse specified date/time

        :param dt: specified date/time
        """
        try:
            return pd.Timestamp(dt).value
        except Exception as ex:
            cls.log().critical('unable to parse date/time: %s', str(dt))
            raise ex


class UniformTsTimePeriod(TimePeriod):

    def __init__(self, **kwargs):
        """ initialize TimePeriod instance
        """
        super(UniformTsTimePeriod, self).__init__(**kwargs)

        # get parameters from arguments
        self._process_kwargs(kwargs, period='1d', offset=0)

        # check for extraneous keyword arguments
        self.check_extra_kwargs(kwargs)

        # check value of period
        if not self.period > 0:
            self.log().critical('invalid time period specified: %d ns', self.period)
            raise AssertionError('time period must be greater than zero')

    def period_index(self, dt):
        """ get number of periods until date/time "dt" since "offset", given specified "period"

        :param dt: specified date/time
        """
        return (pd.Timestamp(dt).value - self.offset) // self.period

    @property
    def period(self):
        """ get period parameter
        """
        return self._period

    @period.setter
    def period(self, period):
        """ set period parameter

        :param period: specified period parameter
        """
        self._period = self.parse_time_period(period)

    @property
    def offset(self):
        """ get offset parameter
        """
        return self._offset

    @offset.setter
    def offset(self, offset):
        """ set offset parameter

        :param offset: specified offset parameter
        """
        self._offset = self.parse_date_time(offset)


class FreqTimePeriod(TimePeriod):

    def __init__(self, **kwargs):
        """ initialize TimePeriod instance
        """
        super(FreqTimePeriod, self).__init__(**kwargs)

        # get parameters from arguments
        self._process_kwargs(kwargs, freq='D')

        # check for extraneous keyword arguments
        self.check_extra_kwargs(kwargs)

    def period_index(self, dt):
        """ return number of periods until date/time "dt" since 1970-01-01

        :param dt: specified date/time parameter
        """
        return pd.tseries.period.Period(
            freq=self.freq, value=pd.Timestamp(dt)).ordinal

    def dt_string(self, period_index):
        """ convert period index into date/time string (start of period)

        :param int period_index: specified period index value.
        """
        return str(pd.tseries.period.Period(
            freq=self.freq, ordinal=period_index).start_time)

    @property
    def freq(self):
        """ Return frequency
        """
        return self._freq

    @freq.setter
    def freq(self, freq):
        """ Try to construct a period object with specified "frequency"

        :param freq: specified frequency
        """
        try:
            per = pd.tseries.period.Period(freq=freq, value='1970-01-01')
        except Exception as ex:
            self.log().critical('invalid "frequency" specified: %s', str(freq))
            raise ex

        # set "frequency"
        self._freq = per.freq
