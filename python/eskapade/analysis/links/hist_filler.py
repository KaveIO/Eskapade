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
        revcols = list(reversed(columns))
        for idx,col in enumerate(revcols):
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

            # decorators; adding them here doesn't seem to work!
            #hist.n_dim = get_n_dim(hist)
            #selected_cols = revcols[:idx+1]
            #dta = [self.var_dtype[col] for col in reversed(selected_cols)]
            #hist.datatype = dta[0] if hist.n_dim==1 else dta

        # FIXME stick data types and number of dimension to histogram
        dta = [self.var_dtype[col] for col in columns]
        hist.datatype = dta[0] if len(columns) == 1 else dta
        hist.n_dim = len(columns)

        return hist

    def process_and_store(self):
        """Process and store histogrammar objects."""
        # fix histogrammar contentType bug for n-dim histograms
        # convert boolean keys to string
        for name, hist in self._hists.items():
            hgr_fix_contentType(hist)
            hgr_convert_bool_to_str(hist)
            hist.n_bins = get_n_bins(hist)

        # put hists in datastore as normal
        HistogramFillerBase.process_and_store(self)


def hgr_fix_contentType(hist):
    """Fix missing contentType attribute of histogrammar histogram.

    Patch up missing contentType where needed; needed for toJson() call

    :param hist: input histogrammar histogram
    """
    # nothing left to fix?
    if isinstance(hist, hg.Count):
        return
    # patch up missing contentType where needed; needed for toJson() call
    if hist is not None:
        if not hasattr(hist, 'contentType'):
            hist.contentType = 'Count'
    # 1. loop through bins
    if hasattr(hist, 'bins'):
        for h in hist.bins.values():
            hgr_fix_contentType(h)
    # 2. loop through values
    elif hasattr(hist, 'values'):
        for h in hist.values:
            hgr_fix_contentType(h)
    # 3. process attributes if present
    if hasattr(hist, 'value'):
        hgr_fix_contentType(hist.value)
    if hasattr(hist, 'underflow'):
        hgr_fix_contentType(hist.underflow)
    if hasattr(hist, 'overflow'):
        hgr_fix_contentType(hist.overflow)
    if hasattr(hist, 'nanflow'):
        hgr_fix_contentType(hist.nanflow)

def hgr_convert_bool_to_str(hist):
    """Convert boolean keys to string.

    Convert boolean keys to string; needed for toJson() call

    :param hist: input histogrammar histogram
    """
    # nothing left to fix?
    if isinstance(hist, hg.Count):
        return
    # 1. loop through bins
    if hasattr(hist, 'bins'):
        kys = list(hist.bins.keys())
        for k in kys:
            if isinstance(k, (bool, np.bool_)):
                hist.bins[str(k)] = hist.bins.pop(k)
        for h in hist.bins.values():
            hgr_convert_bool_to_str(h)
    # 2. loop through values
    elif hasattr(hist, 'values'):
        for h in hist.values:
            hgr_convert_bool_to_str(h)
    # 3. process attributes if present
    if hasattr(hist, 'value'):
        hgr_convert_bool_to_str(hist.value)
    if hasattr(hist, 'underflow'):
        hgr_convert_bool_to_str(hist.underflow)
    if hasattr(hist, 'overflow'):
        hgr_convert_bool_to_str(hist.overflow)
    if hasattr(hist, 'nanflow'):
        hgr_convert_bool_to_str(hist.nanflow)

def get_n_dim(cls):
    """Histogram dimension

    :returns: dimension of the histogram
    :rtype: int
    """
    if isinstance(cls, hg.Count):
        return 0
    # histogram may have a subhistogram. Extract it and recurse
    if hasattr(cls, 'values'):
        hist = cls.values[0] if cls.values else hg.Count()
    elif hasattr(cls, 'bins'):
        hist = list(cls.bins.values())[0] if cls.bins else hg.Count()
    else:
        hist = hg.Count()
    return 1 + get_n_dim(hist)

def get_n_bins(cls):
    """Get number of bins."""
    if hasattr(cls, 'num'):
        return cls.num
    elif hasattr(cls, 'size'):
        return cls.size
    else:
        raise RuntimeError('Cannot retrieve number of bins from hgr hist.')
