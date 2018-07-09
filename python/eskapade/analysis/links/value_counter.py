"""Project: Eskapade - A python-based package for data analysis.

Class: ValueCounter

Created: 2017/03/02

Description:
    Algorithm to do value_counts() on single columns of a pandas
    dataframe, or groupby().size() on multiple columns, both returned
    as dictionaries. It is possible to do cleaning of these dicts by
    rejecting certain keys or removing inconsistent data types.
    Numeric and timestamp columns are converted to bin indices before
    the binning is applied.
    Results are stored as 1D Histograms or as ValueCounts objects.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from collections import Counter

import numpy as np

from eskapade import process_manager, DataStore
from eskapade.analysis import histogram_filling as hf
from eskapade.analysis.histogram import Histogram, ValueCounts
from eskapade.analysis.histogram_filling import HistogramFillerBase


class ValueCounter(HistogramFillerBase):
    """Count values in Pandas data frame.

    ValueCounter does value_counts() on single columns of a pandas
    dataframe, or groupby().size() on multiple columns.  Results of both are
    returned as same-style dictionaries.

    Numeric and timestamp columns are converted to bin indices before the
    binning is applied.  The binning can be provided as input.

    It is possible to do cleaning of these dicts by rejecting certain keys
    or removing inconsistent data types.  Results are stored as 1D
    Histograms or as ValueCounts objects.

    Example is available in: tutorials/esk302_histogram_filling_plotting.py
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key_counts: key of output data to store ValueCounts objects in data store
        :param str store_key_hists: key of output data to store histograms in data store
        :param list columns: columns to pick up from input data (default is all columns)
        :param dict bin_specs: dictionaries used for rebinning numeric or timestamp columns

        Example bin_specs dictionary is:

        >>> bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
        >>>              'y': {'bin_edges': [0, 2, 3, 4, 5, 7, 8]},
        >>>              'date': {'bin_width': np.timedelta64(30, 'D'),
        >>>                       'bin_offset': np.datetime64('2010-01-04')}}

        :param dict var_dtype: dict of datatypes of the columns to study from dataframe. If not provided, try to
                               determine datatypes directy from dataframe.
        :param bool store_at_finalize: Store histograms and/or ValueCount object in datastore at finalize(), not at
               execute(). Useful when looping over datasets. Default is False.
        :param bool drop_inconsistent_key_types: cleanup histograms and/or ValueCount objects by removing alls
               bins/keys with inconsistent datatypes. By default compare with data types in var_dtype dictionary.
        :param drop_keys dict: dictionary used for dropping specific keys from created value_counts dictionaries

        Example drop_keys dictionary is:

        >>> drop_keys = {'x': [1, 4, 8, 19],
        >>>              'y': ['apple', 'pear', 'tomato'],
        >>>              'x:y': [(1, 'apple'), (19, 'tomato')]}
        """
        # initialize Link, pass name from kwargs
        if 'name' not in kwargs:
            kwargs['name'] = 'ValueCounter'
        HistogramFillerBase.__init__(self, **kwargs)

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from
        # kwargs.
        self._process_kwargs(kwargs,
                             store_key_counts=None,
                             store_key_hists=None,
                             drop_inconsistent_key_types=True)

        # these get filled during execution
        self._counts = {}
        self._valcnts = {}

    def initialize(self):
        """Initialize the link."""
        # check basic attribute settings
        if self.store_key_hists is not None:
            assert isinstance(self.store_key_hists, str) and len(self.store_key_hists), \
                'store_key_hists has not been set to string'
        if self.store_key_counts is not None:
            assert isinstance(self.store_key_counts, str) and len(self.store_key_counts), \
                'store_key_counts has not been set to string'
        assert self.store_key_hists is not None or self.store_key_counts is not None, \
            'no store key has been set'

        return HistogramFillerBase.initialize(self)

    def process_columns(self, df):
        """Process columns before histogram filling.

        Specifically, convert timestamp columns to integers
        and numeric variables are converted to indices

        :param df: input (pandas) data frame
        :returns: output (pandas) data frame with converted timestamp columns
        :rtype: pandas DataFrame
        """
        # timestamp variables are converted to ns here
        # make temp df for value counting (used below)

        idf = df[self.str_cols].copy(deep=False)
        for col in self.dt_cols:
            self.logger.debug('Converting column "{column}" of type "{type}" to nanosec.',
                              column=col, type=self.var_dtype[col])
            idf[col] = df[col].apply(hf.to_ns)

        # numerical variables are converted to indices here
        for col in self.num_cols + self.dt_cols:
            self.logger.debug('Converting column "{column}" of type "{type}" to index.',
                              column=col, type=self.var_dtype[col])
            # find column specific bin_specs. if not found, use dict of default
            # values.
            dt = df[col].dtype
            is_number = isinstance(dt.type(), np.number)
            is_timestamp = isinstance(dt.type(), np.datetime64)
            sf = idf if is_timestamp else df
            bin_specs = self.bin_specs.get(col, self._unit_bin_specs if is_number else self._unit_timestamp_specs)
            idf[col] = sf[col].apply(hf.value_to_bin_index, **bin_specs)

        return idf

    def fill_histogram(self, idf, columns):
        """Fill input histogram with column(s) of input dataframe.

        :param idf: input data frame used for filling histogram
        :param list columns: histogram column(s)
        """
        name = ':'.join(columns)
        if name not in self._counts:
            # create an (empty) value counts dict
            self._counts[name] = Counter()
        # value_counts() is faster than groupby().size(), but only works for series (1d).
        # else use groupby() for multi-dimensions
        g = idf.groupby(by=columns).size() if len(columns) > 1 else idf[columns[0]].value_counts()
        counts = Counter(g.to_dict())
        # remove specific keys from histogram before merging, if so requested
        counts = self.drop_requested_keys(name, counts)
        self._counts[name].update(counts)

    def process_and_store(self):
        """Make, clean, and store ValueCount objects."""
        # nothing to do?
        if self.store_key_hists is None and self.store_key_counts is None:
            return

        ds = process_manager.service(DataStore)

        # 1. construct value counts
        for col in self.columns:
            name = ':'.join(col)
            vc = ValueCounts(col, col, self._counts[name])
            # remove all items from Counters where the key is not of correct datatype.
            # e.g. in Counter dict of ints, remove any non-ints that may arise
            # from dq issues.
            if self.drop_inconsistent_key_types:
                vc = self.drop_inconsistent_keys(col, vc)
            self._valcnts[name] = vc

        if self.store_key_counts is not None:
            ds[self.store_key_counts] = self._valcnts

        # 2. construct hists from value counts
        if self.store_key_hists is None:
            return

        for col in self.columns:
            if len(col) != 1:
                continue
            name = ':'.join(col)
            dt = np.dtype(self.var_dtype[name]).type()
            is_number = isinstance(dt, np.number)
            is_timestamp = isinstance(dt, np.datetime64)

            # bin_specs is used for converting index back to original var in
            # histogram class.
            bin_specs = {}
            if is_number:
                bin_specs = self.bin_specs.get(name, self._unit_bin_specs)
            elif is_timestamp:
                bin_specs = self.bin_specs.get(name, self._unit_timestamp_specs)
            h = Histogram(self._valcnts[name], variable=name, datatype=self.var_dtype[name],
                          bin_specs=bin_specs)
            self._hists[name] = h
        # and store
        ds[self.store_key_hists] = self._hists

    def drop_inconsistent_keys(self, columns, obj):
        """Drop inconsistent keys.

        Drop inconsistent keys from a ValueCounts or Histogram object.

        :param list columns: columns key to retrieve desired datatypes
        :param object obj: ValueCounts or Histogram object to drop inconsistent keys from
        """
        # has array been converted first? if so, set correct comparison
        # datatype
        comp_dtype = []
        for col in columns:
            dt = np.dtype(self.var_dtype[col]).type()
            is_converted = isinstance(dt, (np.number, np.datetime64))
            if is_converted:
                comp_dtype.append(np.int64)
            else:
                comp_dtype.append(self.var_dtype[col])
        # keep only keys of types in comp_dtype
        obj.remove_keys_of_inconsistent_type(prefered_key_type=comp_dtype)
        return obj

    def finalize(self):
        """Finalize ValueCounter."""
        status = HistogramFillerBase.finalize(self)
        # cleanup
        if self.store_key_counts is None:
            del self._valcnts
        if self.store_key_hists is None:
            del self._hists

        return status
