# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RootHistFiller                                                        *
# * Created: 2017/02/25                                                            *
# * Description:                                                                   *
# *      Algorithm to create ROOT histograms from colums in pandas dataframe       *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import numpy as np

import ROOT
from ROOT import TH1
import root_numpy

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode


class RootHistFiller(Link):
    """Create ROOT histograms from colums in Pandas dataframe

    Histograms can be up to 3 dimensions. The data type for the histogram
    can be automatically assessed from the column(s), or set as input. For
    each histogram (axis) one can set the label, number of bins, min and max
    values, and data type. String based columns are not allowed, for now.

    The histograms are creates at initialize() and can be filled
    iteratively, while looping over multiple dataframes.
    """

    def __init__(self, **kwargs):
        """Initialize RootHistFiller instance

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
        Link.__init__(self, kwargs.pop('name', 'RootHistFiller'))

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
                             var_dtype={},
                             weight='')

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

        self.hists = {}

        self._default_min = 0.
        self._default_max = 1.
        self._default_n_bins_1d = 40
        self._default_n_bins_2d = 10
        self._default_n_bins_3d = 5
        self._default_dtype = np.dtype(float)

    def initialize(self):
        """Initialize RootHistFiller"""

        self.check_arg_types(read_key=str, store_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str, pair_up_columns=str)
        self.check_arg_vals('read_key', 'store_key')

        # pair up any columns and add to self.colums
        if len(self.pair_up_columns):
            assert len(self.pair_up_columns) >= 2, 'pair_up_columns needs at least two column entries.'
        self.pair_up_columns = sorted(self.pair_up_columns)
        for i, c1 in enumerate(self.pair_up_columns):
            for c2 in self.pair_up_columns[i + 1:]:
                self.columns.append([c1, c2])

        # check that columns are set correctly.
        # supports 1d 2d and 3d histograms
        for i, c in enumerate(self.columns):
            if isinstance(c, str):
                self.columns[i] = [c]
            if not isinstance(self.columns[i], list):
                raise TypeError('columns "%s" needs to be a string or list of strings' % self.columns[i])
            assert len(self.columns[i]) <= 3, 'dimension needs to be 1,2, or 3, not "%d"' % len(self.columns[i])

        # check for supported data types
        for k in self.var_dtype.keys():
            try:
                dt = np.dtype(self.var_dtype[k]).type
                if dt is np.string_ or dt is np.object_:
                    dt = str
                self.var_dtype[k] = dt
            except:
                raise TypeError('unknown assigned datatype to variable "%s".' % k)
            assert self._dtype_check(dt), 'column "%s" is of incorrect data type "%s"' % (k, dt)

        # check weight variable
        if self.weight:
            assert len(self.weight), 'weight needs to be a filled string'

        # construct the empty histograms.
        # filled in execute()
        for c in self.columns:
            name = ':'.join(c)
            self.hists[name] = self._construct_hist(c)

        return StatusCode.Success

    def execute(self):
        """Execute RootHistFiller"""

        proc_mgr = ProcessManager()
        ds = proc_mgr.service(DataStore)

        df = ds[self.read_key]

        # check presence and data types of requested columns
        for c in self.columns:
            for col in c:
                assert col in df.columns, 'column "%s" not in dataframe "%s"' % (col, self.read_key)
                dt = df[col].dtype.type
                if dt is np.string_ or dt is np.object_:
                    dt = str
                assert self._dtype_check(dt), 'column "%s" is of incorrect data type "%s"; not supported' % (col, dt)
                self.var_dtype[col] = dt

        # verify weight
        if self.weight:
            assert self.weight in df.columns, 'weight "%s" not in dataframe "%s"' % (self.weight, self.read_key)

        # fill the histograms here
        # include weights if present
        w = df[self.weight].values if self.weight else None
        for c in self.columns:
            n = ':'.join(c)
            root_numpy.fill_hist(self.hists[n], df[c if len(c) > 1 else c[0]].values, w)

        ds[self.store_key] = self.hists

        return StatusCode.Success

    def _title(self, columns):
        n = ':'.join(columns)
        if n in self.var_label:
            return self.var_label[n]
        title = ''
        for i, c in enumerate(columns):
            if i != 0:
                title += ' versus '
            if c in self.var_label:
                title += self.var_label[c]
            else:
                title += c
        return title

    def _n_dims(self, c):
        return len(c)

    def _n_bins(self, c, idx):
        n = ':'.join(c)
        if len(c) > 1 and n in self.var_number_of_bins and len(self.var_number_of_bins[n]) == len(c):
            return self.var_number_of_bins[n][idx]
        elif c[idx] in self.var_number_of_bins:
            return self.var_number_of_bins[c[idx]]
        # fall back on defaults
        if len(c) == 1:
            return self._default_n_bins_1d
        if len(c) == 2:
            return self._default_n_bins_2d
        if len(c) == 3:
            return self._default_n_bins_3d
        return self._default_n_bins_1d

    def _min(self, c, idx):
        n = ':'.join(c)
        if len(c) > 1 and n in self.var_min_value and len(self.var_min_value[n]) == len(c):
            return self.var_min_value[n][idx]
        elif c[idx] in self.var_min_value:
            return self.var_min_value[c[idx]]
        # fall back on default
        return self._default_min

    def _max(self, c, idx):
        n = ':'.join(c)
        if len(c) > 1 and n in self.var_max_value and len(self.var_max_value[n]) == len(c):
            return self.var_max_value[n][idx]
        elif c[idx] in self.var_max_value:
            return self.var_max_value[c[idx]]
        # fall back on default
        return self._default_max

    def _dtype_check(self, dt):
        dt = np.dtype(dt)
        check = dt == np.dtype(float) or dt == np.dtype(int) or \
            dt == np.dtype('short') or dt == np.dtype('byte') or dt == np.dtype(bool)
        # or dt==np.dtype(str)
        if not check:
            self.log().warning('Data type "%s" should be: float, int, bool', dt)
        return check

    def _dtype(self, c):
        n = ':'.join(c)
        if n in self.var_dtype:
            return self.var_dtype[n]
        # ranking in order is: float, int, short, char
        elif any(self.var_dtype[col] == np.dtype(float) for col in c if col in self.var_dtype):
            return np.dtype(float)
        elif any(self.var_dtype[col] == np.dtype(int) for col in c if col in self.var_dtype):
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

    def _construct_hist(self, c):
        n = ':'.join(c)
        n_dims = len(c)

        HISTCLASS = None
        dt = self._dtype(c)

        if n_dims == 1:
            if dt == np.dtype(float):
                HISTCLASS = ROOT.TH1F
            if dt == np.dtype(int):
                HISTCLASS = ROOT.TH1I
            if dt == np.dtype('short'):
                HISTCLASS = ROOT.TH1S
            if dt == np.dtype('byte') or dt == np.dtype(bool):
                HISTCLASS = ROOT.TH1C
            hist = HISTCLASS(n, self._title(c),
                             self._n_bins(c, 0), self._min(c, 0), self._max(c, 0))
        elif n_dims == 2:
            if dt == np.dtype(float):
                HISTCLASS = ROOT.TH2F
            if dt == np.dtype(int):
                HISTCLASS = ROOT.TH2I
            if dt == np.dtype('short'):
                HISTCLASS = ROOT.TH2S
            if dt == np.dtype('byte') or dt == np.dtype(bool):
                HISTCLASS = ROOT.TH2C
            hist = HISTCLASS(n, self._title(c),
                             self._n_bins(c, 0), self._min(c, 0), self._max(c, 0),
                             self._n_bins(c, 1), self._min(c, 1), self._max(c, 1))
        elif n_dims == 3:
            if dt == np.dtype(float):
                HISTCLASS = ROOT.TH3F
            if dt == np.dtype(int):
                HISTCLASS = ROOT.TH3I
            if dt == np.dtype('short'):
                HISTCLASS = ROOT.TH3S
            if dt == np.dtype('byte') or dt == np.dtype(bool):
                HISTCLASS = ROOT.TH3C
            hist = HISTCLASS(n, self._title(c),
                             self._n_bins(c, 0), self._min(c, 0), self._max(c, 0),
                             self._n_bins(c, 1), self._min(c, 1), self._max(c, 1),
                             self._n_bins(c, 2), self._min(c, 2), self._max(c, 2))
        else:
            raise RuntimeError('number of dimensions not supported: %d' % n_dims)
        for i in range(n_dims):
            if self._extend_axis(c, i):
                hist.SetCanExtend(ROOT.TH1.kAllAxes)
                # Bug cannot extend y axis
                # https://sft.its.cern.ch/jira/browse/ROOT-7389
                # if i == 0:
                #     hist.SetCanExtend(ROOT.TH1.kXaxis)
                # if i == 1:
                #     hist.SetCanExtend(ROOT.TH1.kYaxis)
                # if i == 2:
                #     hist.SetCanExtend(ROOT.TH1.kZaxis)
        # store column names in axes
        for i, col in enumerate(c):
            if i == 0:
                hist.GetXaxis().SetName(col)
            if i == 1:
                hist.GetYaxis().SetName(col)
            if i == 2:
                hist.GetZaxis().SetName(col)

        return hist
