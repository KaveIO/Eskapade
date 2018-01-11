"""Project: Eskapade - A Python-based package for data analysis.

Module: root_analysis.decorators.histograms

Created: 2017/04/24

Description:
    Decorators for PyROOT histogram objects

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy

from ROOT import TH1
from ROOT import TH1C, TH1S, TH1I, TH1F, TH1D
from ROOT import TH2C, TH2S, TH2I, TH2F, TH2D
from ROOT import TH3C, TH3S, TH3I, TH3F, TH3D


# TH1 (Note: all root histograms inherit from TH1)


@property
def datatype(self):  # noqa
    """Data type of histogram variable.

    Return data type of the variable represented by the histogram.  If not
    already set, will determine datatype automatically.

    :returns: data type
    :rtype: type or list(type)
    """
    if not hasattr(self, '_datatype'):
        cn = self.__class__.__name__
        n_dim = int(cn[2:3])
        dt = cn[3:4]
        assert dt in ['C', 'S', 'I', 'F', 'D'], 'data type of histogram variable is unknown.'
        if dt == 'C':
            dtype = numpy.int8
        elif dt == 'S':
            dtype = numpy.int16
        elif dt == 'I':
            dtype = numpy.int32
        elif dt == 'F':
            dtype = numpy.float32
        elif dt == 'D':
            dtype = numpy.float64
        self._datatype = dtype if n_dim == 1 else [dtype] * n_dim

    return self._datatype


@datatype.setter
def datatype(self, dt):
    """Set data type of histogram variable.

    Set data type of the variable represented by the histogram.

    :param type dt: type of the variable represented by the histogram
    :raises RunTimeError: if datatype has already been set, it will not overwritten
    """
    if hasattr(self, '_datatype'):
        raise RuntimeError('datatype already (assessed and) set')
    self._datatype = dt


@property
def n_dim(self):
    """Histogram dimension."""
    cn = self.__class__.__name__
    n_dim = int(cn[2:3])
    return n_dim


TH1.datatype = datatype
TH1.n_dim = n_dim


@property
def n_bins(self):  # noqa
    """Number of bins of x axis.

    :returns: number of bins for x axis
    :rtype: int
    """
    return self.GetNbinsX()


def bin_centers(self, q):
    """Get bin centers of histogram axis.

    :param int q: axis index, should be 0 (=x), 1 (=y), or 2(=z)
    :returns: numpy array of bin centers
    :rtype: numpy.array
    """
    assert q in [0, 1, 2], 'axis index should be 0, 1, or 2'
    if q == 0:
        ax = self.GetXaxis()
    elif q == 1:
        ax = self.GetYaxis()
    elif q == 2:
        ax = self.GetZaxis()
    bin_centers = [ax.GetBinCenter(i + 1) for i in range(ax.GetNbins())]
    return numpy.array(bin_centers)


def bin_edges(self, q):
    """Get bin edges of histogram axis.

    :param int q: axis index, should be 0 (=x), 1 (=y), or 2(=z)
    :returns: numpy array of bin edges
    :rtype: numpy.array
    """
    assert q in [0, 1, 2], 'axis index should be 0, 1, or 2'
    if q == 0:
        ax = self.GetXaxis()
    elif q == 1:
        ax = self.GetYaxis()
    elif q == 2:
        ax = self.GetZaxis()
    bin_edges = [ax.GetBinLowEdge(i + 1) for i in range(ax.GetNbins())]
    bin_edges.append(ax.GetBinUpEdge(ax.GetLast()))
    return numpy.array(bin_edges)


def bin_labels(self, q):
    """Get bin labels of histogram axis.

    :param int q: axis index, should be 0 (=x), 1 (=y), or 2(=z)
    :returns: numpy array of bin labels
    :rtype: numpy.array
    """
    assert q in [0, 1, 2], 'axis index should be 0, 1, or 2'
    if q == 0:
        ax = self.GetXaxis()
    elif q == 1:
        ax = self.GetYaxis()
    elif q == 2:
        ax = self.GetZaxis()
    bin_labels = [ax.GetBinLabel(i + 1) for i in range(ax.GetNbins())]
    return numpy.array(bin_labels)


def bin_entries(self):
    """Get bin values.

    :returns: numpy array of bin entries
    :rtype: numpy.array
    """
    bin_entries = [self.GetBinContent(i + 1) for i in range(self.GetNbinsX())]
    return numpy.array(bin_entries)


def bin_vals(self):
    """Get NumPy-style histogram values.

    :returns: comma-separated np array of bin entries, np array of bin edges
    :rtype: numpy.array, numpy.array
    """
    bin_entries = self.bin_entries()
    bin_edges = self.bin_edges()
    return bin_entries, bin_edges


def bin_range(self, q):
    """Get bin range of histogram axis.

    :param int q: axis index, should be 0 (=x), 1 (=y), or 2(=z)
    :returns: tuple of bin range
    :rtype: tuple
    """
    assert q in [0, 1, 2], 'axis index should be 0, 1, or 2'
    if q == 0:
        ax = self.GetXaxis()
    elif q == 1:
        ax = self.GetYaxis()
    elif q == 2:
        ax = self.GetZaxis()
    return ax.GetXmin(), ax.GetXmax()


@property
def low(self):
    """Low edge of first bin.

    :returns: low edge of first bin
    :rtype: float
    """
    ax = self.GetXaxis()
    return ax.GetXmin()


@property
def high(self):
    """High edge of last bin.

    :returns: up edge of first bin
    :rtype: float
    """
    ax = self.GetXaxis()
    return ax.GetXmax()


for T in [TH1C, TH1S, TH1I, TH1F, TH1D]:
    T.bin_centers = lambda self: bin_centers(self, q=0)
    T.bin_edges = lambda self: bin_edges(self, q=0)
    T.bin_labels = lambda self: bin_labels(self, q=0)
    T.x_lim = lambda self: bin_range(self, q=0)
    T.n_bins = n_bins
    T.bin_entries = bin_entries
    T.bin_vals = bin_vals
    T.low = low
    T.high = high


def bin_entries_2dgrid(self):
    """Get 2-D grid of bin entries.

    :returns: 2-d grid of bin entries
    """
    grid = numpy.zeros((self.GetNbinsY(), self.GetNbinsX()))
    for j in range(self.GetNbinsX()):
        for i in range(self.GetNbinsY()):
            grid[i, j] = self.GetBinContent(j + 1, i + 1)
    return grid


def xy_ranges_grid(self):
    """Get x and y ranges and x, y grid.

    :returns: x and y ranges and x,y grid
    """
    x_ranges = self.x_bin_edges()
    y_ranges = self.y_bin_edges()
    grid = self.bin_entries()
    return x_ranges, y_ranges, grid


for T in [TH2C, TH2S, TH2I, TH2F, TH2D]:
    T.x_bin_centers = lambda self: bin_centers(self, q=0)
    T.x_bin_edges = lambda self: bin_edges(self, q=0)
    T.x_bin_labels = lambda self: bin_labels(self, q=0)
    T.x_lim = lambda self: bin_range(self, q=0)
    T.y_bin_centers = lambda self: bin_centers(self, q=1)
    T.y_bin_edges = lambda self: bin_edges(self, q=1)
    T.y_bin_labels = lambda self: bin_labels(self, q=1)
    T.y_lim = lambda self: bin_range(self, q=1)
    T.bin_entries = bin_entries_2dgrid
    T.xy_ranges_grid = xy_ranges_grid

for T in [TH3C, TH3S, TH3I, TH3F, TH3D]:
    T.x_bin_centers = lambda self: bin_centers(self, q=0)
    T.x_bin_edges = lambda self: bin_edges(self, q=0)
    T.x_bin_labels = lambda self: bin_labels(self, q=0)
    T.x_lim = lambda self: bin_range(self, q=0)
    T.y_bin_centers = lambda self: bin_centers(self, q=1)
    T.y_bin_edges = lambda self: bin_edges(self, q=1)
    T.y_bin_labels = lambda self: bin_labels(self, q=1)
    T.y_lim = lambda self: bin_range(self, q=1)
    T.z_bin_centers = lambda self: bin_centers(self, q=2)
    T.z_bin_edges = lambda self: bin_edges(self, q=2)
    T.z_bin_labels = lambda self: bin_labels(self, q=2)
    T.z_lim = lambda self: bin_range(self, q=2)
