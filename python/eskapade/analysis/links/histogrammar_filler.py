"""Project: Eskapade - A python-based package for data analysis.

Class: HistogrammarFiller

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

import histogrammar as hg
import numpy as np

from eskapade.analysis import histogram_filling as hf
from eskapade.analysis.histogram_filling import HistogramFillerBase


class HistogrammarFiller(HistogramFillerBase):
    """Fill histogrammar sparse-bin histograms.

    Algorithm to fill histogrammar style sparse-bin and category histograms.

    It is possible to do after-filling cleaning of these histograms by rejecting certain
    keys or removing inconsistent data types. Timestamp columns are
    converted to nanoseconds before the binning is applied. Final histograms
    are stored in the datastore.

    Example is available in: tutorials/esk303_hgr_filler_plotter.py
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store and do basic check on the attributes of link HistogrammarFiller.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store histograms in data store
        :param list columns: colums to pick up from input data. (default is all columns)
        :param dict bin_specs: dictionaries used for rebinning numeric or timestamp columns

        Example bin_specs dictionary is:

        >>> bin_specs = {'x': {'bin_width': 1, 'bin_offset': 0},
                         'y': {'bin_edges': [0, 2, 3, 4, 5, 7, 8]}}

        :param dict var_dtype: dict of datatypes of the columns to study from dataframe
                               If not provided, try to determine datatypes directy from dataframe.
        :param dict quantity: dictionary of lambda functions of how to pars certain columns

        Example quantity dictionary is:

        >>> quantity = {'y': lambda x: x}

        :param bool store_at_finalize: Store histograms in datastore at finalize(), not at
                                       execute(). Useful when looping over datasets. Default is False.
        :param drop_keys dict: dictionary used for dropping specific keys from bins dictionaries of histograms

        Example drop_keys dictionary is:

        >>> drop_keys = {'x': [1, 4, 8, 19],
                         'y': ['apple', 'pear', 'tomato'],
                         'x:y': [(1, 'apple'), (19, 'tomato')]}
        """
        # initialize Link, pass name from kwargs
        if 'name' not in kwargs:
            kwargs['name'] = 'HistogrammarFiller'
        HistogramFillerBase.__init__(self, **kwargs)

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             quantity={})

    def fill_histogram(self, idf, columns):
        """Fill input histogram with column(s) of input dataframe.

        :param idf: input data frame used for filling histogram
        :param list columns: histogram column(s)
        """
        name = ':'.join(columns)
        if name not in self._hists:
            # create an (empty) histogram of right type
            self._hists[name] = self.construct_empty_hist(columns)
        hist = self._hists[name]
        # do the actual filling
        clm = columns[0] if len(columns) == 1 else columns
        hist.fill.numpy(idf[clm])
        # remove specific keys from histogram before merging, if so requested
        hist.bins = self.drop_requested_keys(name, hist.bins)
        self._hists[name] = hist

    def construct_empty_hist(self, columns):
        """Create an (empty) histogram of right type.

        Create a multi-dim histogram by iterating through the columns in
        reverse order and passing a single-dim hist as input to the next
        column.

        :param list columns: histogram columns
        :returns: created histogram
        :rtype: histogrammar.Count
        """
        hist = hg.Count()

        # create a multi-dim histogram by iterating through the columns in reverse order
        # and passing a single-dim hist as input to the next column
        for col in reversed(columns):
            # histogram type depends on the data type
            dt = np.dtype(self.var_dtype[col])

            # processing function, e.g. only accept boolians during filling
            f = self.quantity.get(col, hf.QUANTITY[dt.type])
            if len(columns) == 1:
                # df[col] is a pd.series
                quant = lambda x, fnc=f: fnc(x)  # noqa
            else:
                # df[columns] is a pd.Dataframe
                # fix column to col
                quant = lambda x, fnc=f, clm=col: fnc(x[clm])  # noqa

            is_number = isinstance(dt.type(), np.number)
            is_timestamp = isinstance(dt.type(), np.datetime64)

            if is_number or is_timestamp:
                # numbers and timestamps are put in a sparse binned histogram
                bs = self.bin_specs.get(col, self._unit_bin_specs if is_number else self._unit_timestamp_specs)
                hist = hg.SparselyBin(binWidth=bs['bin_width'], origin=bs['bin_offset'], quantity=quant, value=hist)
            else:
                # string and boolians are treated as categories
                hist = hg.Categorize(quantity=quant, value=hist)

        # FIXME stick data types and number of dimension to histogram
        dta = [self.var_dtype[col] for col in columns]
        hist.datatype = dta[0] if len(columns) == 1 else dta
        hist.n_dim = len(columns)

        @property
        def n_bins(self):
            """`Get number of bins."""
            if hasattr(self, 'num'):
                return self.num
            elif hasattr(self, 'size'):
                return self.size
            else:
                raise RuntimeError('Cannot retrieve number of bins from hgr hist')

        hist.n_bins = n_bins

        return hist
