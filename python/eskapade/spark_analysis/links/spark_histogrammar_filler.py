"""Project: Eskapade - A python-based package for data analysis.

Class: SparkHistogrammarFiller

Created: 2017/06/09

Description:
    Algorithm to fill histogrammar sparse-bin histograms from a Spark
    dataframe. It is possible to do cleaning of these histograms by
    rejecting certain keys or removing inconsistent data types.
    Timestamp columns are converted to nanoseconds before
    the binning is applied.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import histogrammar
import histogrammar.sparksql
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from eskapade.analysis import HistogrammarFiller, histogram_filling as hf


def unit_func(x):
    """Doesn't do anything. Used as a default new quantity function.

    :param x: value
    :return: the same value
    """
    return x


class SparkHistogrammarFiller(HistogrammarFiller):
    """Fill histogrammar sparse-bin histograms with Spark.

    Algorithm to fill histogrammar style sparse-bin and category histograms
    with Spark.  It is possible to do after-filling cleaning of these
    histograms by rejecting certain keys or removing inconsistent data
    types. Timestamp columns are converted to nanoseconds before the binning
    is applied. Final histograms are stored in the datastore.

    Example is available in: tutorials/esk605_hgr_filler_plotter.py.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store and do basic check on the attributes of link HistogrammarFiller.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store histograms in data store
        :param list columns: colums to pick up from input data (default is all columns)
        :param dict bin_specs: dictionaries used for rebinning numeric or timestamp columns

        Example bin_specs dictionary is:

        >>> bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
        >>>              'y': {'bin_edges': [0, 2, 3, 4, 5, 7, 8]}}

        :param dict var_dtype: dict of datatypes of the columns to study from dataframe
                               (if not provided, try to determine datatypes directy from dataframe)
        :param dict quantity: dictionary of lambda functions of how to pars certain columns

        Example quantity dictionary is:

        >>> quantity = {'y': lambda x: x}

        :param bool store_at_finalize: store histograms in datastore at finalize(), not at execute()
            (useful when looping over datasets, default is False)
        :param drop_keys dict: dictionary used for dropping specific keys from bins dictionaries of histograms

        Example drop_keys dictionary is:

        >>> drop_keys = {'x': [1, 4, 8, 19],
        >>>              'y': ['apple', 'pear', 'tomato'],
        >>>              'x:y': [(1, 'apple'), (19, 'tomato')]}
        """
        # initialize Link, pass name from kwargs
        if 'name' not in kwargs:
            kwargs['name'] = 'SparkHistogrammarFiller'
        HistogrammarFiller.__init__(self, **kwargs)

        self._unit_timestamp_specs = {'bin_width': float(pd.Timedelta(days=30).value),
                                      'bin_offset': float(pd.Timestamp('2010-01-04').value)}

    def fill_histogram(self, idf, columns):
        """Fill input histogram with column(s) of input dataframe.

        :param idf: input data frame used for filling histogram
        :param list columns: histogram column(s)
        """
        name = ':'.join(columns)
        if name not in self._hists:
            # create an (empty) histogram of right type
            self._hists[name] = self.construct_empty_hist(idf, columns)
        hist = self._hists[name]

        # do the actual filling
        hist.fill.sparksql(idf)

        # remove specific keys from histogram before merging, if so requested
        hist.bins = self.drop_requested_keys(name, hist.bins)
        self._hists[name] = hist

    def construct_empty_hist(self, df, columns):
        """Create an (empty) histogram of right type.

        Create a multi-dim histogram by iterating through the columns in
        reverse order and passing a single-dim hist as input to the next
        column.

        :param df: input dataframe
        :param list columns: histogram columns
        :returns: created histogram
        :rtype: histogrammar.Count
        """
        hist = histogrammar.Count()

        # create a multi-dim histogram by iterating through the columns in reverse order
        # and passing a single-dim hist as input to the next column
        for col in reversed(columns):
            # histogram type depends on the data type
            dt = np.dtype(self.var_dtype[col])

            is_number = isinstance(dt.type(), np.number)
            is_timestamp = isinstance(dt.type(), np.datetime64)

            if is_number or is_timestamp:
                # numbers and timestamps are put in a sparse binned histogram
                specs = self.bin_specs.get(col, self._unit_bin_specs if is_number else self._unit_timestamp_specs)
                hist = histogrammar.SparselyBin(binWidth=specs['bin_width'], origin=specs['bin_offset'],
                                                quantity=df[col],
                                                value=hist)
            else:
                # string and boolians are treated as categories
                hist = histogrammar.Categorize(quantity=df[col], value=hist)

        # FIXME stick data types and number of dimension to histogram
        dta = [self.var_dtype[col] for col in columns]
        hist.datatype = dta[0] if len(columns) == 1 else dta
        hist.n_dim = len(columns)

        @property
        def n_bins(self):
            """Get number of bins."""
            if hasattr(self, 'num'):
                return self.num
            elif hasattr(self, 'size'):
                return self.size
            else:
                raise RuntimeError('Cannot retrieve number of bins from hgr hist.')

        hist.n_bins = n_bins

        return hist

    def assert_dataframe(self, df):
        """Check that input data is a filled Spark data frame.

        :param df: input Spark data frame
        """
        if not isinstance(df, pyspark.sql.dataframe.DataFrame):
            raise TypeError('Retrieved object not of type Spark DataFrame.')
        # assert df.count() > 0, 'input dataframe is empty'

    def get_all_columns(self, data):
        """Retrieve all columns / keys from input data.

        :param data: input data sample (pandas dataframe or dict)
        :returns: list of columns
        :rtype: list
        """
        if not isinstance(data, pyspark.sql.dataframe.DataFrame):
            raise TypeError('Retrieved object not of type Spark DataFrame.')
        return sorted(data.columns)

    def get_data_type(self, df, col):
        """Get data type of dataframe column.

        :param df: input data frame
        :param str col: column
        """
        if col not in df.columns:
            raise KeyError('Column "{0:s}" not in input dataframe.'.format(col))
        dt = dict(df.dtypes)[col]
        # spark conversions to numpy or python equivalent
        if dt == 'string':
            dt = 'str'
        elif dt == 'timestamp':
            dt = np.datetime64
        return np.dtype(dt)

    def process_columns(self, df):
        """Process columns before histogram filling.

        Specifically, convert timestamp columns to integers

        :param df: input data frame
        :returns: output data frame with converted timestamp columns
        :rtype: DataFrame
        """
        # histogrammar does not yet support long integers
        def to_ns(x):
            """Convert to ns."""
            return float(hf.to_ns(x))

        udf_to_ns = udf(to_ns, FloatType())

        # udf_to_ns = udf(hf.to_ns, LongType())

        # make alias df for value counting (used below)
        idf = df.alias('')

        # timestamp variables are converted to ns here
        for col in self.dt_cols:
            self.logger.debug('Converting column "{col}" of type "{type}" to nanosec.',
                              col=col, type=self.var_dtype[col])
            idf = idf.withColumn(col, udf_to_ns(*[col]))

        histogrammar.sparksql.addMethods(idf)

        return idf

    def process_and_store(self):
        """Process and store spark-based histogram objects."""
        # if quantity refers to a spark df, the histogram cannot be pickled,
        # b/c we cannot pickle a spark df.
        # HACK: patch the quantity pickle bug here before storage into the datastore
        for name, hist in self._hists.items():
            self.logger.debug('Processing histogram {name}.', name=name)
            self.reset_hgr_quantity(hist)
            continue

        # put hists in datastore as normal
        HistogrammarFiller.process_and_store(self)

    def reset_hgr_quantity(self, hist, new_quantity=unit_func):
        """Reset quantity attribute of histogrammar histogram.

        If quantity refers to a Spark df the histogram cannot be pickled,
        b/c we cannot pickle a Spark df.
        Here we reset the quantity of a (filled) histogram to a neutral lambda function.

        :param hist: histogrammar histogram to reset quantity of.
        :param new_quantity: new quantity function to reset hist.quantity to. default is lambda x: x.
        """
        # nothing left to reset?
        if isinstance(hist, histogrammar.Count):
            return
        # reset quantity
        if hasattr(hist, 'quantity'):
            # self.logger.debug('Reset quantity of obj-type {type}', type=type(hist))
            hist.quantity = new_quantity
        # process histogram's bins for their quantity attributes
        if not hasattr(hist, 'bins'):
            return
        if not isinstance(hist.bins, dict):
            return
        # 1. loop through bins
        for i_bin in hist.bins.values():
            self.reset_hgr_quantity(i_bin, new_quantity)
        # 2. process value attribute if present
        if not hasattr(hist, 'value'):
            return
        self.reset_hgr_quantity(hist.value, new_quantity)
