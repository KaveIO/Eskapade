import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tabulate
from statsmodels.stats.weightstats import DescrStatsW
from collections import Counter
from eskapade.analysis.histogram import BinningUtil

NUM_NS_DAY = 24 * 3600 * int(1e9)


class ArrayStats(object):
    """Create summary of an array

    Class to calculate statistics (mean, standard deviation, percentiles,
    etc.) and create a histogram of values in an array.
    The statistics can be returned as values in a dictionary, a
    printable string, or as a LaTeX string.
    """

    def __init__(self, data, col_name, weights=None, unit='', label=''):
        """Initialize for a single column in data frame

        :param data: Input array
        :type data: iterable
        :type data: pandas Dataframe
        :type data: (keys of) dict
        :param col_name: column name
        :param weights: Input array (default None)
        :type weights: iterable
        :type weights: string (column of data)
        :param unit: Unit of column
        :param str label: Label to describe column variable
        :raises: TypeError
        """

        # set initial values of attributes
        self.stat_vars = []
        self.stat_vals = {}
        self.print_lines = []
        self.latex_table = []

        # parse arguments
        self.name = str(col_name)
        self.unit = str(unit)
        self.label = str(label)
        self.col = data[self.name] if isinstance(data, pd.DataFrame) else data
        self.weights = data[weights] if isinstance(weights, str) else weights
        if isinstance(data, dict):
            self.col = sorted(data.keys())
            self.weights = [data[k] for k in self.col]

        # check if column is iterable
        try:
            iter(self.col)
        except:
            raise TypeError('Specified data object is not iterable')
        if self.weights is not None:
            try:
                iter(self.weights)
            except:
                raise TypeError('Specified weights object is not iterable')

        # check sizes of data and weights
        if self.weights is not None:
            assert len(self.col) == len(self.weights), \
                'weights and data do not have the same length.'

        # store data and weights in a Pandas Series
        if not isinstance(self.col, pd.Series):
            self.col = pd.Series(val for val in self.col)
        if self.weights is not None:
            if not isinstance(self.weights, pd.Series):
                self.weights = pd.Series(w for w in self.weights)

        # store non-null column values
        self.col_nn = self.col[self.col.notnull()]
        self.weights_nn = self.weights[
            self.col.notnull()] if self.weights is not None else None

        # to be filled in make_histogram
        self.hist = None

    def get_col_props(self):
        """Get column properties

        :returns dict: Column properties
        """
        return get_col_props(self.col.dtype)

    def create_stats(self):
        """Compute statistical properties of column variable

        This function computes the statistical properties of values in the
        specified column.  It is called by other functions that use the
        resulting figures to create a statistical overview.
        """

        # reset stats containers
        self.stat_vars = []
        self.stat_vals = {}
        self.print_lines = []
        self.latex_table = []

        # determine column properties
        col_props = self.get_col_props()

        # get value counts
        cnt, var_cnt, dist_cnt = (len(self.col), len(
            self.col_nn), self.col.nunique())
        if self.weights_nn is not None:
            cnt, var_cnt = sum(self.weights), sum(self.weights_nn)
        for stat_var, stat_val in zip(
                ('count', 'filled', 'distinct'), (cnt, var_cnt, dist_cnt)):
            self.stat_vars.append(stat_var)
            self.stat_vals[stat_var] = (stat_val, '{:d}'.format(stat_val))

        # add value counts to print lines
        self.print_lines.append(
            '{}:'.format(
                self.label if self.label else self.name))
        self.print_lines.append(
            '{0:d} entries ({1:.0f}%)'.format(
                var_cnt, var_cnt / cnt * 100))
        self.print_lines.append('{0:d} unique entries'.format(dist_cnt))

        # convert time stamps to integers
        if col_props['is_ts']:
            col_num = self.col_nn.astype(int)
        else:
            col_num = self.col_nn

        # get additional statistics for numeric variables
        if col_props['is_num']:
            stat_vars = (
                'mean',
                'std',
                'min',
                'max',
                'p01',
                'p05',
                'p16',
                'p50',
                'p84',
                'p95',
                'p99')
            # two lines below also work if weights are None
            des = DescrStatsW(col_num, self.weights_nn)
            stat_vals = (des.mean, des.std) + \
                tuple(weighted_quantile(col_num, self.weights_nn,
                                        (0, 1, 0.01, 0.05, 0.16, 0.50, 0.84, 0.95, 0.99)))
            self.stat_vars += stat_vars
            for stat_var, stat_val in zip(stat_vars, stat_vals):
                if not col_props['is_ts']:
                    # value entry for floats and integers
                    self.stat_vals[stat_var] = (
                        stat_val, '{:+g}'.format(stat_val))
                else:
                    if stat_var != 'std':
                        # display time stamps as date/time strings
                        self.stat_vals[stat_var] = (
                            pd.Timestamp(
                                int(stat_val)), str(
                                pd.Timestamp(
                                    int(stat_val))))
                    else:
                        # display time-stamp range as number of days
                        stat_val /= NUM_NS_DAY
                        self.stat_vals[stat_var] = (
                            stat_val, '{:g}'.format(stat_val))

            # append statistics to print lines
            name_len = max(len(n) for n in stat_vars)
            for stat_var in stat_vars:
                self.print_lines.append('{{0:{:d}s}} : {{1:s}}'.format(name_len)
                                        .format(stat_var, self.stat_vals[stat_var][1]))

    def get_print_stats(self, to_output=False):
        """Get statistics in printable form

        :param bool to_output: Print statistics to output stream?
        :returns str: Printable statistics string
        """

        # create statistics print lines
        if not self.stat_vals:
            self.create_stats()

        # create printable string
        print_str = '\n'.join(self.print_lines) + '\n'
        if to_output:
            print(print_str)

        return print_str

    def get_latex_table(self):
        """Get LaTeX code string for table of stats values"""

        # create statistics print lines
        if not self.stat_vals:
            self.create_stats()

        # create LaTeX string
        table = [(stat_var, self.stat_vals[stat_var][1])
                 for stat_var in self.stat_vars]
        return tabulate.tabulate(table, tablefmt='latex')

    def get_x_label(self):
        x_lab = self.label if self.label else self.name
        if self.unit:
            x_lab += ' [{}]'.format(self.unit)
        return x_lab

    def make_histogram(self, var_bins=30, var_range=None, bin_edges=None):
        """Create histogram of column values

        :param int var_bins: Number of histogram bins
        :param tuple var_range: Range of histogram variable
        :param list bin_edges: predefined bin edges to use for histogram. Overrules var_bins.
        """
        # create statistics overview
        if not self.stat_vals:
            self.create_stats()

        # determine column properties
        col_props = self.get_col_props()

        if col_props['is_num']:
            col_num = self.col_nn

            # determine histogram range for numeric variable
            if var_range:
                # get minimum and maximum of variable for histogram from
                # specified range
                var_min, var_max = var_range
                if col_props['is_ts']:
                    # convert minimum and maximum to Unix time stamps
                    var_min, var_max = pd.Timestamp(
                        var_min).value, pd.Timestamp(var_max).value
            else:
                # determine minimum and maximum of variable for histogram from
                # percentiles
                var_min, var_max = self.stat_vals.get(
                    'p05')[0], self.stat_vals.get('p95')[0]
                if col_props['is_ts']:
                    var_min, var_max = pd.Timestamp(
                        var_min).value, pd.Timestamp(var_max).value
                var_min -= 0.1 * (var_max - var_min)
                var_max += 0.1 * (var_max - var_min)
                if var_min > 0. and var_min < +0.2 * (var_max - var_min):
                    var_min = 0.
                elif var_max < 0. and var_max > -0.2 * (var_max - var_min):
                    var_max = 0.

            if col_props['is_ts']:
                # np.histogram cannot deal with timestamps, so convert to ints
                # and convert them back below.
                to_timestamp = np.vectorize(lambda x: pd.Timestamp(x).value)
                col_num = to_timestamp(self.col_nn)
                if bin_edges is not None:
                    bin_edges = (to_timestamp(bin_edges)).tolist()

            if bin_edges is not None:
                bin_util = BinningUtil(bin_edges=bin_edges)
                idx_min = bin_util.value_to_bin_label(var_min)
                var_min = bin_util.get_left_bin_edge(idx_min)
                idx_max = bin_util.value_to_bin_label(var_max)
                var_max = bin_util.get_right_bin_edge(idx_max)
                var_bins = bin_util.truncated_bin_edges(
                    variable_range=[var_min, var_max])
            else:
                if col_props['is_int'] or col_props['is_ts']:
                    # for ints and ts use bins around integer values
                    bin_width = np.max(
                        (np.round((var_max - var_min) / float(var_bins)), 1.))
                    var_min = np.floor(var_min - 0.5) + 0.5
                    var_bins = int((var_max - var_min) // bin_width) + \
                        int((var_max - var_min) % bin_width > 0.)
                    var_max = var_min + var_bins * bin_width

            # make (weighted) histogram
            values, bins = np.histogram(
                col_num, bins=var_bins, range=(
                    var_min, var_max), weights=self.weights_nn)

            if col_props['is_ts']:
                # convert Unix time stamps to Pandas time stamps
                bins = [pd.Timestamp(ts) for ts in bins]
            self.hist = values, bins
        else:
            # get data from data frame for categorical column
            if self.weights_nn is None:
                val_counts = self.col_nn.value_counts(
                    sort=True).iloc[:var_bins].to_dict()
            else:
                val_counts = Counter()
                for k, v in zip(self.col_nn, self.weights_nn):
                    val_counts[k] += v
                val_counts = dict(val_counts.most_common(var_bins))
            labels = sorted(lab for lab in val_counts.keys())
            values = [val_counts[lab] for lab in labels]
            self.hist = values, labels

        return self.hist


def get_col_props(var_type):
    """Get column properties

    :returns dict: Column properties
    """
    npdtype = np.dtype(var_type)

    # determine data-type categories
    is_int = isinstance(npdtype.type(), np.integer)
    is_ts = isinstance(npdtype.type(), np.datetime64)
    is_num = is_ts or isinstance(npdtype.type(), np.number)

    return dict(dtype=npdtype, is_num=is_num, is_int=is_int, is_ts=is_ts)


def weighted_quantile(data, weights=None, probability=[0.5]):
    """
    Compute the weighted quantile of a 1D numpy array.

    :param ndarray data: input array (one dimension).
    :param ndarray weights: array with the weights of the same size of `data`.
    :param ndarray probability: array of quantiles to compute. Each probablity must have a value between 0 and 1.
    :return: list of the output value(s).
    """

    # Weighted quantiles, inspired by:
    # https://github.com/nudomarinero/wquantiles/blob/master/wquantiles.py
    # written by Jose Sabater
    # Here updated to: return multiple quantiles in one go. Now also works
    # when weight is None.

    # Check the inputs
    if not isinstance(data, np.matrix):
        data = np.asarray(data)
    if isinstance(probability, float):
        probability = [probability]
    if not isinstance(probability, np.matrix):
        probability = np.asarray(probability)
    for p in probability:
        if p > 1 or p < 0:
            raise ValueError("probability must have a value between 0 and 1")
    if data.ndim != 1:
        raise TypeError("data must be a one dimensional array")
    if weights is None:
        weights = np.ones(len(data))
    if not isinstance(weights, np.matrix):
        weights = np.asarray(weights)
    if weights.ndim != 1:
        raise TypeError("weights must be a one dimensional array")
    if data.shape != weights.shape:
        raise TypeError("the length of data and weights must be the same")

    # Sort data and compute auxiliary arrays
    sorted_index = np.argsort(data)
    sorted_data = data[sorted_index]
    sorted_weights = weights[sorted_index]
    cumsum = np.cumsum(sorted_weights)
    pn = (cumsum - 0.5 * sorted_weights) / np.sum(sorted_weights)
    # Get the values of the quantiles
    quantiles = [np.interp(p, pn, sorted_data) for p in probability]

    return quantiles