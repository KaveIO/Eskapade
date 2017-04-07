# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : HistogrammarFiller                                                    *
# * Created: 2017/03/21                                                            *
# * Description:                                                                   *
# *      Algorithm to fill histogrammar sparse-bin histograms.
# *      It is possible to do cleaning of these histograms by
# *      rejecting certain keys or removing inconsistent data types.
# *      Timestamp columns are converted to nanoseconds before
# *      the binning is applied.
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
import numpy as np
import pandas as pd
import histogrammar as hg

# numeric datatypes get converted to an index, which is then used for value counting
NUMERIC_SUBSTR = [np.dtype('int'), np.dtype('float'), np.dtype('double')]

# string datatype get treated as categories
STRING_SUBSTR = [np.dtype('str'), np.dtype('object'), np.dtype('bool')]

# timestamps are converted to nanoseconds (int)
TIME_SUBSTR = [np.dtype('datetime64[ns]'), np.datetime64]
NUM_NS_DAY = 24 * 3600 * int(1e9)


class HistogrammarFiller(Link):
    """Fill histogrammar sparse-bin histograms

    It is possible to do cleaning of these histograms by rejecting certain
    keys or removing inconsistent data types.  Timestamp columns are
    converted to nanoseconds before the binning is applied. Final histograms
    are stored in the datastore.
    """

    def __init__(self, **kwargs):
        """Initialize HistogrammarFiller instance

        Store and do basic check on the attributes of link
        HistogrammarFiller.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store histograms in data store
        :param list columns: colums to pick up from input data
        :param dict bin_specs: dictionaries used for rebinning numeric or timestamp columns.

        Example bin_specs dictionary is:

        >>> bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
                         'y': {'bin_edges': [0,2,3,4,5,7,8]}}

        :param dict datatype: dict of datatypes of the columns to study from dataframe.
                              If not provided, try to determine datatypes directy from dataframe.
        :param dict quantity: dictionary of lambda functions of how to pars certain columns.

        Example quantity dictionary is:

        >>> quantity = {'y': lambda x: x}

        :param drop_keys dict: dictionary used for dropping specific keys from bins dictionaries of histograms.
               Example drop_keys dictionary is:

        >>> drop_keys = {'x': [1,4,8,19],
                         'y': ['apple', 'pear', 'tomato']
                         'x:y': [(1,'apple'),(19,'tomato')]}
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'HistogrammarFiller'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key=None,
                             store_key=None,
                             columns=[],
                             bin_specs={},
                             datatype={},
                             quantity={},
                             drop_keys={})

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

        self._unit_bin_specs = {'bin_width': 1.0, 'bin_offset': 0.0}
        self._unit_timestamp_specs = {'bin_width': pd.Timedelta(days=30).value,
                                      'bin_offset': pd.Timestamp('2010-01-04').value}
        # these get filled during execution
        self._hists = {}

    def initialize(self):
        """Initialize HistogrammarFiller"""

        # check basic attribute settings
        assert isinstance(self.read_key, str) and len(self.read_key), \
            'read_key has not been set correctly'
        assert isinstance(self.store_key, str) and len(self.store_key), \
            'store_key has not been set to string'

        # check that columns are set correctly.
        for i, c in enumerate(self.columns):
            if isinstance(c, str):
                self.columns[i] = [c]
            assert isinstance(self.columns[i], list), \
                'columns "%s" need to be a string or list of strings' % self.columns[i]

        # check for supported data types
        for k in self.datatype.keys():
            try:
                self.datatype[k] = np.dtype(self.datatype[k]).type
                if (self.datatype[k] is np.string_) or (self.datatype[k] is np.object_):
                    self.datatype[k] = str
            except:
                raise RuntimeError('unknown assigned datatype to variable "%s"' % k)

        return StatusCode.Success

    def execute(self):
        """Execute HistogrammarFiller

        Does 4 things:

        * check presence and data type of requested columns
        * numerical and timestamp variables are converted to indices (for grouping by)
        * do the actual value counting based on categories and created indices
        * add to datastore
        """

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        assert self.read_key in list(ds.keys()), 'key "%s" not in data store' % self.read_key
        df = ds[self.read_key]
        if not isinstance(df, pd.DataFrame):
            raise TypeError('retrieved object not of type pandas DataFrame')
        assert len(df.index) > 0, 'dataframe "%s" is empty' % self.read_key

        # 1. check presence and data type of requested columns
        # sort columns into numerical, timestamp and category based
        strcols = []
        numcols = []
        dtcols = []
        for c in self.columns:
            for col in c:
                assert col in df.columns, 'column "%s" not in dataframe "%s"' % (col, self.read_key)
                dt = df[col].dtype
                if col not in self.datatype:
                    self.datatype[col] = dt.type
                    if (self.datatype[col] is np.string_) or (self.datatype[col] is np.object_):
                        self.datatype[col] = str
                assert dt in STRING_SUBSTR or dt in NUMERIC_SUBSTR or dt in TIME_SUBSTR, \
                    'cannot process column "%s" of data type "%s"' % (col, dt)
                is_number = isinstance(dt.type(), np.number)
                is_timestamp = isinstance(dt.type(), np.datetime64)
                colset = numcols if is_number else dtcols if is_timestamp else strcols
                if col not in colset:
                    colset.append(col)
                self.log().debug('Datatype of column "%s" is "%s"', col, self.datatype[col])

        # 2. timestamp variables are converted to ns here
        idf = df[strcols + numcols].copy(deep=False)
        for col in dtcols:
            self.log().debug('Converting column "%s" of type "%s" to nanosec', col, self.datatype[col])

            def to_ns(x):
                if pd.isnull(x):
                    return 0
                return pd.to_datetime(x).value
            idf[col] = df[col].apply(to_ns)

        # 3. do the actual histogram filling
        for c in self.columns:
            name = ':'.join(c)
            self.log().debug('Filling histogram for column(s) "%s"', c)
            if name not in self._hists:
                # create an (empty) histogram of right type
                self._hists[name] = self._construct_empty_hist(c)
            hist = self._hists[name]
            # do the actual filling
            clm = c[0] if len(c) == 1 else c
            hist.fill.numpy(idf[clm])
            # remove specific keys from histogram, if so requested
            hist = self.drop_requested_keys(name, hist)
            self._hists[name] = hist
        # cleanup temp df
        del idf

        # 4. storage
        ds[self.store_key] = self._hists

        return StatusCode.Success

    def _construct_empty_hist(self, columns):
        """Create an (empty) histogram of right type

        Create a multi-dim histogram by iterating through the columns in
        reverse order and passing a single-dim hist as input to the next
        column.

        :param columns: histogram columns
        :returns: created histogram
        :rtype: histogrammar.Count
        """

        hist = hg.Count()

        # create a multi-dim histogram by iterating through the columns in reverse order
        # and passing a single-dim hist as input to the next column
        for col in reversed(columns):
            # histogram type depends on the data type
            dt = np.dtype(self.datatype[col])

            # processing function, e.g. only accept boolians during filling
            f = self.quantity[col] if col in self.quantity else QUANTITY[dt.type]
            if len(columns) == 1:
                # df[col] is a pd.series
                q = lambda x, fnc=f: fnc(x)
            else:
                # df[columns] is a pd.Dataframe
                # fix column to col
                q = lambda x, fnc=f, clm=col: fnc(x[clm])

            is_number = isinstance(dt.type(), np.number)
            is_timestamp = isinstance(dt.type(), np.datetime64)

            if is_number or is_timestamp:
                # numbers and timestamps are put in a sparse binned histogram
                bs = self.bin_specs.get(col, self._unit_bin_specs if is_number
                                        else self._unit_timestamp_specs)
                hist = hg.SparselyBin(binWidth=bs['bin_width'], origin=bs['bin_offset'], quantity=q, value=hist)
            else:
                # string and boolians are treated as categories
                hist = hg.Categorize(quantity=q, value=hist)

        # FIXME stick data types and number of dimension to histogram
        dta = [self.datatype[col] for col in columns]
        hist.datatype = dta[0] if len(columns) == 1 else dta
        hist.n_dim = len(columns)
        @property
        def n_bins(self):
            if hasattr(self, num):
                return self.num
            elif hasattr(size, size):
                return self.size
            else:
                raise Exception('Cannot retrieve number of bins from hgr hist.')
        hist.n_bins = n_bins

        return hist

    def drop_requested_keys(self, name, hist):
        """Drop requested keys from histogram

        :param string name: key of drop_keys dict to get array of keys to be dropped
        :param hist: histogram to drop keys from
        :returns: histogram without dropped keys
        """

        # drop requested keys
        if name in self.drop_keys:
            keys_to_drop = self.drop_keys[name]
            if not isinstance(keys_to_drop, list):
                raise TypeError('drop_keys value needs to be a list of values')
            for key in keys_to_drop:
                if key in hist.bins:
                    self.log().debug('Removing key "%s" with value "%s", as requested', key, hist.bins[key])
                    del hist.bins[key]
        return hist


def to_str(val):
    """Convert input to (array of) string(s)

    :param val: value to be converted
    :returns: converted value
    :rtype: str or np.ndarray
    """

    if isinstance(val, str):
        return val
    elif hasattr(val, '__iter__'):
        return np.asarray(list(map(lambda s: s if isinstance(s, str) else str(s) if hasattr(s, '__str__') else '',
                                   val)))
    elif hasattr(val, '__str__'):
        return str(val)

    return ''


def only_str(val):
    """Pass input value or array only if it is a string

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: str or np.ndarray
    """

    if isinstance(val, str):
        return val
    elif hasattr(val, '__iter__'):
        return np.asarray(list(filter(lambda s: isinstance(s, str), val)))

    return None


def only_bool(val):
    """ Pass input value or array only if it is a bool

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.bool or np.ndarray
    """

    if isinstance(val, np.bool_) or isinstance(val, bool):
        return np.bool(val)
    elif hasattr(val, '__iter__') and not isinstance(val, str):
        return np.asarray(list(filter(lambda s: isinstance(s, np.bool_) or isinstance(s, bool), val)))

    return None


def only_int(val):
    """ Pass input val value or array only if it is an integer

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64 or np.ndarray
    """
    if isinstance(val, np.int64) or isinstance(val, int):
        return np.int64(val)
    elif hasattr(val, '__iter__') and not isinstance(val, str):
        return np.asarray(list(filter(lambda s: isinstance(s, np.int64) or isinstance(s, int), val)))

    return None


def only_float(val):
    """ Pass input val value or array only if it is a float

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.float64 or np.ndarray
    """

    if isinstance(val, np.float64) or isinstance(val, float):
        return np.float64(val)
    elif hasattr(val, '__iter__') and not isinstance(val, str):
        return np.asarray(list(filter(lambda s: isinstance(s, np.float64) or isinstance(s, float), val)))

    return np.nan


QUANTITY = {str: only_str, np.str_: only_str,
            int: only_int, np.int64: only_int,
            bool: only_bool, np.bool_: only_bool,
            float: only_float, np.float64: only_float,
            np.datetime64: only_int}
