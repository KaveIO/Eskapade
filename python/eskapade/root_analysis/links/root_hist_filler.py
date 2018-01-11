"""Project: Eskapade - A python-based package for data analysis.

Class: RootHistFiller

Created: 2017/02/25

Description:
    Algorithm to create ROOT histograms from colums in pandas dataframe

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ROOT
import numpy as np
import root_numpy

from eskapade.analysis.histogram_filling import HistogramFillerBase
from eskapade.root_analysis import root_helper


class RootHistFiller(HistogramFillerBase):
    """Create ROOT histograms from colums in Pandas dataframe.

    Histograms can be up to 3 dimensions. The data type for the histogram
    can be automatically assessed from the column(s), or set as input. For
    each histogram (axis) one can set the label, number of bins, min and max
    values, and data type. String based columns are not allowed, for now.

    The histograms are creates at initialize() and can be filled
    iteratively, while looping over multiple dataframes.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store. Default is 'hist'
        :param list columns: required list of column (variables) from input data to turn into histograms.
                             Eg. ['x','y', ['y','z'], ['x','y','z']].
                             A list of columns is interpreted as a multi-dimensional histogram.
                             Up to 3 dimensions is allowed.
        :param list pair_up_columns: required list of column (variables) to pair up for 2-dim histograms
        :param dict var_label: title for histogram of certain variable (optional)
        :param dict var_number_of_bins: number of bins for histogram of certain variable (optional)
        :param dict var_min_value: min value for histogram of certain variable (optional)
        :param dict var_max_value: max value for histogram of certain variable (optional)
        :param dict var_dtype: impose data type of variable (optional)
        :param str weight: name of weight column (optional)
        """
        # initialize Link, pass name from kwargs
        if 'name' not in kwargs:
            kwargs['name'] = 'RootHistFiller'
        if not kwargs.get('store_key', None):
            kwargs['store_key'] = 'hist'
        HistogramFillerBase.__init__(self, **kwargs)

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             store_key='hist',
                             columns=[],
                             pair_up_columns=[],
                             var_label={},
                             var_number_of_bins={},
                             var_min_value={},
                             var_max_value={},
                             weight=None)

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

        self._default_min = 0.
        self._default_max = 1.
        self._default_n_bins_1d = 40
        self._default_n_bins_2d = 10
        self._default_n_bins_3d = 5
        self._default_dtype = np.dtype(float)

    def initialize(self):
        """Initialize the link."""
        self.check_arg_types(read_key=str, store_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str, pair_up_columns=str)
        self.check_arg_vals('read_key', 'store_key')

        status = HistogramFillerBase.initialize(self)

        # pair up any columns and add to self.colums
        if self.pair_up_columns:
            assert len(self.pair_up_columns) >= 2, 'pair_up_columns needs at least two column entries.'
        self.pair_up_columns = sorted(self.pair_up_columns)
        for i, c1 in enumerate(self.pair_up_columns):
            for c2 in self.pair_up_columns[i + 1:]:
                self.columns.append([c1, c2])

        # check that columns are set correctly.
        # supports 1d 2d and 3d histograms
        for i, c in enumerate(self.columns):
            assert len(c) <= 3, 'dimension needs to be 1, 2, or 3, not "{:d}"'.format(len(c))

        # check weight variable
        if self.weight and not isinstance(self.weight, str):
            raise TypeError('weight argument needs to be a string')

        return status

    def categorize_columns(self, df):
        """Categorize columns of dataframe by data type.

        :param df: input (pandas) data frame
        """
        # check presence and data type of requested columns
        # sort columns into numerical, timestamp and category based
        HistogramFillerBase.categorize_columns(self, df)

        # verify weight
        if self.weight and self.weight not in df.columns:
            raise KeyError('weight "{0:s}" not in dataframe "{1:s}"'.format(self.weight, self.read_key))

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
        # fill the histograms here
        # include weights if present
        w = idf[self.weight].values if self.weight else None
        root_numpy.fill_hist(hist, idf[columns if len(columns) > 1 else columns[0]].values, w)
        self._hists[name] = hist

    def _title(self, columns):
        n = ':'.join(columns)
        if n in self.var_label:
            return self.var_label[n]
        title = ''
        for i, c in enumerate(columns):
            if i != 0:
                title += ' versus '
            title += self.var_label.get(c, c)
        return title

    def _n_bins(self, c, idx):
        if len(c) == 2:
            default = self._default_n_bins_2d
        elif len(c) == 3:
            default = self._default_n_bins_3d
        else:
            default = self._default_n_bins_1d
        return root_helper.get_variable_value(self.var_number_of_bins, c, idx, default)

    def _min(self, c, idx):
        return root_helper.get_variable_value(self.var_min_value, c, idx, self._default_min)

    def _max(self, c, idx):
        return root_helper.get_variable_value(self.var_max_value, c, idx, self._default_max)

    def _dtype(self, c):
        n = ':'.join(c)
        if n in self.var_dtype:
            return self.var_dtype[n]
        # ranking in order is: float, int, short, char
        elif any(self.var_dtype[col] == np.dtype(float) for col in c if col in self.var_dtype):
            return np.dtype(float)
        elif any(self.var_dtype[col] == np.dtype(int) for col in c if col in self.var_dtype):
            return np.dtype(int)
        elif any(self.var_dtype[col] == np.datetime64 for col in c if col in self.var_dtype):
            return np.dtype(int)
        elif any(self.var_dtype[col] == np.dtype('short') for col in c if col in self.var_dtype):
            return np.dtype('short')
        elif any(self.var_dtype[col] == np.dtype('byte') for col in c if col in self.var_dtype):
            return np.dtype('byte')
        elif any(self.var_dtype[col] == np.dtype(bool) for col in c if col in self.var_dtype):
            return np.dtype(bool)
        # default is float
        return self._default_dtype

    def _extend_axis(self, c, idx):
        n = ':'.join(c)
        if len(c) > 1 and n in self.var_min_value and n in self.var_max_value and \
           len(self.var_min_value[n]) == len(c) and len(self.var_max_value[n]) == len(c):
            return False
        elif c[idx] in self.var_min_value and c[idx] in self.var_max_value:
            return False
        return True

    def construct_empty_hist(self, columns):
        """Create an (empty) histogram of right type.

        Create a multi-dim histogram by iterating through the columns.

        :param list columns: histogram columns
        :returns: created ROOT histogram
        :rtype: ROOT.TH1
        """
        name = ':'.join(columns)
        n_dims = len(columns)

        HISTCLASS = None
        dt = self._dtype(columns)

        if n_dims == 1:
            if dt == np.dtype(float):
                HISTCLASS = ROOT.TH1F
            if dt == np.dtype(int):
                HISTCLASS = ROOT.TH1I
            if dt == np.dtype('short'):
                HISTCLASS = ROOT.TH1S
            if dt == np.dtype('byte') or dt == np.dtype(bool):
                HISTCLASS = ROOT.TH1C
            hist = HISTCLASS(name, self._title(columns),
                             self._n_bins(columns, 0), self._min(columns, 0), self._max(columns, 0))
        elif n_dims == 2:
            if dt == np.dtype(float):
                HISTCLASS = ROOT.TH2F
            if dt == np.dtype(int):
                HISTCLASS = ROOT.TH2I
            if dt == np.dtype('short'):
                HISTCLASS = ROOT.TH2S
            if dt == np.dtype('byte') or dt == np.dtype(bool):
                HISTCLASS = ROOT.TH2C
            hist = HISTCLASS(name, self._title(columns),
                             self._n_bins(columns, 0), self._min(columns, 0), self._max(columns, 0),
                             self._n_bins(columns, 1), self._min(columns, 1), self._max(columns, 1))
        elif n_dims == 3:
            if dt == np.dtype(float):
                HISTCLASS = ROOT.TH3F
            if dt == np.dtype(int):
                HISTCLASS = ROOT.TH3I
            if dt == np.dtype('short'):
                HISTCLASS = ROOT.TH3S
            if dt == np.dtype('byte') or dt == np.dtype(bool):
                HISTCLASS = ROOT.TH3C
            hist = HISTCLASS(name, self._title(columns),
                             self._n_bins(columns, 0), self._min(columns, 0), self._max(columns, 0),
                             self._n_bins(columns, 1), self._min(columns, 1), self._max(columns, 1),
                             self._n_bins(columns, 2), self._min(columns, 2), self._max(columns, 2))
        else:
            raise RuntimeError('number of dimensions not supported: {:d}'.format(n_dims))
        for i in range(n_dims):
            if self._extend_axis(columns, i):
                hist.SetCanExtend(ROOT.TH1.kAllAxes)
                # Bug cannot extend individual y axis ?
                # https://sft.its.cern.ch/jira/browse/ROOT-7389
                # if i == 0:
                #     hist.SetCanExtend(ROOT.TH1.kXaxis)
                # if i == 1:
                #     hist.SetCanExtend(ROOT.TH1.kYaxis)
                # if i == 2:
                #     hist.SetCanExtend(ROOT.TH1.kZaxis)
        # store column names in axes
        for i, col in enumerate(columns):
            if i == 0:
                hist.GetXaxis().SetName(col)
            if i == 1:
                hist.GetYaxis().SetName(col)
            if i == 2:
                hist.GetZaxis().SetName(col)

        # FIXME stick data types to histogram
        dta = [self.var_dtype[col] for col in columns]
        hist.datatype = dta[0] if len(columns) == 1 else dta

        return hist
