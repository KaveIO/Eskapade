"""Project: Eskapade - A python-based package for data analysis.

Class: HistogramFillerBase

Created: 2017/03/21

Description:
    Algorithm to fill histogrammar sparse-bin histograms.
    It is possible to do cleaning of these histograms by
    rejecting certain keys or removing inconsistent data types.
    Timestamp columns are converted to nanoseconds before
    the binning is applied.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd

from eskapade import process_manager, DataStore, Link, StatusCode

# numeric datatypes get converted to an index, which is then used for value counting
NUMERIC_SUBSTR = [np.dtype('int'), np.dtype('float'), np.dtype('double')]

# string datatype get treated as categories
STRING_SUBSTR = [np.dtype('str'), np.dtype('object'), np.dtype('bool')]

# timestamps are converted to nanoseconds (int)
TIME_SUBSTR = [np.dtype('datetime64[ns]'), np.datetime64, np.dtype('<M8')]
NUM_NS_DAY = 24 * 3600 * int(1e9)


class HistogramFillerBase(Link):
    """Base class link to fill histograms.

    It is possible to do after-filling cleaning of these histograms by rejecting certain
    keys or removing inconsistent data types. Timestamp columns are
    converted to nanoseconds before the binning is applied. Final histograms
    are stored in the datastore.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store and do basic check on the attributes of link HistogramFillerBase.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store histograms in data store
        :param list columns: colums to pick up from input data. (default is all columns)
        :param dict bin_specs: dictionaries used for rebinning numeric or timestamp columns

        Example bin_specs dictionary is:

        >>> bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
                         'y': {'bin_edges': [0, 2, 3, 4, 5, 7, 8]}}

        :param dict var_dtype: dict of datatypes of the columns to study from dataframe.
                               If not provided, try to determine datatypes directy from dataframe.
        :param bool store_at_finalize: Store histograms in datastore at finalize(), not at
                                       execute(). Useful when looping over datasets. Default is False.
        :param drop_keys dict: dictionary used for dropping specific keys from bins dictionaries of histograms

        Example drop_keys dictionary is:

        >>> drop_keys = {'x': [1,4,8,19],
                         'y': ['apple', 'pear', 'tomato'],
                         'x:y': [(1, 'apple'), (19, 'tomato')]}
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'HistogramFillerBase'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key=None,
                             store_key=None,
                             columns=[],
                             bin_specs={},
                             var_dtype={},
                             drop_keys={},
                             store_at_finalize=False)

        self._unit_bin_specs = {'bin_width': 1.0, 'bin_offset': 0.0}
        self._unit_timestamp_specs = {'bin_width': pd.Timedelta(days=30).value,
                                      'bin_offset': pd.Timestamp('2010-01-04').value}

        # these get filled during execution
        self._hists = {}

        # initialize attributes
        self.all_columns = []
        self.str_cols = []
        self.num_cols = []
        self.dt_cols = []

    def initialize(self):
        """Initialize the link."""
        # check basic attribute settings
        assert isinstance(self.read_key, str) and len(self.read_key), 'read_key has not been set correctly'
        if self.store_key is not None:
            assert isinstance(self.store_key, str) and len(self.store_key), 'store_key has not been set to string'

        # default histogram creation is at execute(). Storage at finalize is useful for
        # looping over datasets.
        if self.store_at_finalize:
            self.logger.debug('Storing (and possible post-processing) at finalize, not execute.')

        # check that columns are set correctly.
        for i, c in enumerate(self.columns):
            if isinstance(c, str):
                self.columns[i] = [c]
            elif not isinstance(self.columns[i], list):
                raise TypeError('Columns "{}" needs to be a string or list of strings'.format(self.columns[i]))

        # check for supported data types
        for k in self.var_dtype:
            try:
                self.var_dtype[k] = np.dtype(self.var_dtype[k]).type
                if self.var_dtype[k] is np.string_ or self.var_dtype[k] is np.object_:
                    self.var_dtype[k] = str
            except BaseException:
                raise RuntimeError('unknown assigned datatype to variable "{}"'.format(k))

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Execute() four things:

        * check presence and data type of requested columns
        * timestamp variables are converted to nanosec (integers)
        * do the actual value counting based on categories and created indices
        * then convert to histograms and add to datastore
        """
        ds = process_manager.service(DataStore)

        # basic checks on contensts of the data frame
        if self.read_key not in ds:
            raise KeyError('key "{}" not in data store'.format(self.read_key))
        df = ds[self.read_key]
        self.assert_dataframe(df)

        # determine all possible columns, used for comparison below
        self.all_columns = self.get_all_columns(df)

        # copy all columns from the dataframe?
        if not self.columns:
            self.columns = self.all_columns
            for i, c in enumerate(self.all_columns):
                self.columns[i] = [c]

        # 1. check presence and data type of requested columns
        # sort columns into numerical, timestamp and category based
        self.categorize_columns(df)

        # 2. timestamp variables are converted to ns here
        idf = self.process_columns(df)

        # 3. do the actual histogram/counter filling
        for c in self.columns:
            name = ':'.join(c)
            self.logger.debug('Processing column(s) "{col}".', col=name)
            self.fill_histogram(idf, c)

        # cleanup temp df
        del idf

        # 4. storage
        if not self.store_at_finalize:
            self.process_and_store()

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        Store Histograms here, if requested.
        """
        # convert to histograms and add to datastore
        if self.store_at_finalize:
            self.process_and_store()

        return StatusCode.Success

    def process_and_store(self):
        """Store (and possibly process) histogram objects."""
        ds = process_manager.service(DataStore)

        if self.store_key is not None:
            ds[self.store_key] = self._hists

    def assert_dataframe(self, df):
        """Check that input data is a filled pandas data frame.

        :param df: input (pandas) data frame
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError('retrieved object not of type pandas DataFrame')
        assert len(df.index) > 0, 'input dataframe is empty'

    def get_all_columns(self, data):
        """Retrieve all columns / keys from input data.

        :param data: input data sample (pandas dataframe or dict)
        :returns: list of columns
        :rtype: list
        """
        if isinstance(data, pd.DataFrame):
            all_columns = sorted(data.columns.tolist())
        else:
            raise RuntimeError('Cannot determine columns in input data found for {!s}'.format(self))

        return all_columns

    def get_data_type(self, df, col):
        """Get data type of dataframe column.

        :param df: input data frame
        :param str col: column
        """
        if col not in df.columns:
            raise KeyError('column "{0:s}" not in input dataframe'.format(col))
        return df[col].dtype

    def categorize_columns(self, df):
        """Categorize columns of dataframe by data type.

        :param df: input (pandas) data frame
        """
        # check presence and data type of requested columns
        # sort columns into numerical, timestamp and category based
        for c in self.columns:
            for col in c:
                if col not in df.columns:
                    raise KeyError('Column "{0:s}" not in dataframe "{1:s}".'.format(col, self.read_key))
                dt = self.get_data_type(df, col)
                if col not in self.var_dtype:
                    self.var_dtype[col] = dt.type
                    if (self.var_dtype[col] is np.string_) or (self.var_dtype[col] is np.object_):
                        self.var_dtype[col] = str
                if not any(dt in types for types in (STRING_SUBSTR, NUMERIC_SUBSTR, TIME_SUBSTR)):
                    raise TypeError('Cannot process column "{0:s}" of data type "{1!s}".'.format(col, dt))
                is_number = isinstance(dt.type(), np.number)
                is_timestamp = isinstance(dt.type(), np.datetime64)
                colset = self.num_cols if is_number else self.dt_cols if is_timestamp else self.str_cols
                if col not in colset:
                    colset.append(col)
                self.logger.debug('Data type of column "{col}" is "{type}".', col=col, type=self.var_dtype[col])

    def process_columns(self, df):
        """Process columns before histogram filling.

        Specifically, convert timestamp columns to integers

        :param df: input (pandas) data frame
        :returns: output (pandas) data frame with converted timestamp columns
        :rtype: pandas DataFrame
        """
        # timestamp variables are converted to ns here
        # make temp df for value counting (used below)
        idf = df[self.num_cols + self.str_cols].copy(deep=False)
        for col in self.dt_cols:
            self.logger.debug('Converting column "{col}" of type "{type}" to nanosec.',
                              col=col, type=self.var_dtype[col])
            idf[col] = df[col].apply(to_ns)
        return idf

    def fill_histogram(self, idf, c):
        """Fill input histogram with column(s) of input dataframe.

        :param idf: input data frame used for filling histogram
        :param list c: histogram column(s)
        """
        return

    def drop_requested_keys(self, name, counts):
        """Drop requested keys from counts dictionary.

        :param string name: key of drop_keys dict to get array of keys to be dropped
        :param dict counts: counts dictionary to drop specific keys from
        :returns: count dict without dropped keys
        """
        # drop requested keys
        if name in self.drop_keys:
            keys_to_drop = self.drop_keys[name]
            if not isinstance(keys_to_drop, list):
                raise TypeError('drop_keys value needs to be a list of values')
            for key in keys_to_drop:
                if key in counts:
                    self.logger.debug('Removing key "{key}" with value: "{value}", as requested.',
                                      key=key, value=counts[key])
                    del counts[key]
        return counts


def to_ns(x):
    """Convert input timestamps to nanoseconds (integers).

    :param x: value to be converted
    :returns: converted value
    :rtype: int
    """
    if pd.isnull(x):
        return 0
    try:
        return pd.to_datetime(x).value
    except Exception:
        if hasattr(x, '__str__'):
            return pd.to_datetime(str(x)).value
    return 0


def to_str(val):
    """Convert input to (array of) string(s).

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
    """Pass input value or array only if it is a string.

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
    """Pass input value or array only if it is a bool.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.bool or np.ndarray
    """
    if isinstance(val, (np.bool_, bool)):
        return np.bool(val)
    elif hasattr(val, '__iter__') and not isinstance(val, str):
        return np.asarray(list(filter(lambda s: isinstance(s, (np.bool_, bool)), val)))

    return None


def only_int(val):
    """Pass input val value or array only if it is an integer.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.int64 or np.ndarray
    """
    if isinstance(val, (np.int64, int)):
        return np.int64(val)
    elif hasattr(val, '__iter__') and not isinstance(val, str):
        return np.asarray(list(filter(lambda s: isinstance(s, (np.int64, int)), val)))

    return None


def only_float(val):
    """Pass input val value or array only if it is a float.

    :param val: value to be evaluated
    :returns: evaluated value
    :rtype: np.float64 or np.ndarray
    """
    if isinstance(val, (np.float64, float)):
        return np.float64(val)
    elif hasattr(val, '__iter__') and not isinstance(val, str):
        return np.asarray(list(filter(lambda s: isinstance(s, (np.float64, float)), val)))

    return np.nan


QUANTITY = {str: only_str, np.str_: only_str,
            int: only_int, np.int64: only_int,
            bool: only_bool, np.bool_: only_bool,
            float: only_float, np.float64: only_float,
            np.datetime64: only_int}


def value_to_bin_index(val, **kwargs):
    """Convert value to bin index.

    Convert a numeric or timestamp column to an integer bin index.

    :param bin_width: bin_width value needed to convert column to an integer bin index
    :param bin_offset: bin_offset value needed to convert column to an integer bin index
    """
    try:
        # NOTE this notation also works for timestamps
        bin_width = kwargs.get('bin_width', 1)
        bin_offset = kwargs.get('bin_offset', 0)
        bin_index = int(np.floor((val - bin_offset) / bin_width))
        return bin_index
    except BaseException:
        pass
    return val


def value_to_bin_center(val, **kwargs):
    """Convert value to bin center.

    Convert a numeric or timestamp column to a common bin center value.

    :param bin_width: bin_width value needed to convert column to a common bin center value
    :param bin_offset: bin_offset value needed to convert column to a common bin center value
    """
    try:
        # NOTE this notation also works for timestamps, and does not change the
        # unit
        bin_width = kwargs.get('bin_width', 1)
        bin_offset = kwargs.get('bin_offset', 0)
        bin_index = int(np.floor((val - bin_offset) / bin_width))
        obj_type = type(bin_width)
        return bin_offset + obj_type((bin_index + 0.5) * bin_width)
    except BaseException:
        pass
    return val
