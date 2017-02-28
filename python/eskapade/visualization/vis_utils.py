import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tabulate

NUM_NS_DAY = 24 * 3600 * int(1e9)


class ColumnStats:
    """Create summary of column in data frame

    Class to calculate statistics (mean, standard deviation, percentiles,
    etc.) and create a histogram of values in the columns of a Pandas data
    frame.  The statistics can be returned as values in a dictionary, a
    printable string, or as a LaTeX string.  Histograms are plotted with
    Matplotlib.
    """

    def __init__(self, data, col_name, unit='', label=''):
        """Initialize for a single column in data frame

        :param data: Input data frame
        :type data: pandas.DataFrame
        :type data: iterable
        :param col: Column key
        :param unit: Unit of column variable
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

        # check if column is iterable
        try:
            iter(self.col)
        except:
            raise TypeError('Specified data object is not iterable')

        # store data in a Pandas Series
        if not isinstance(self.col, pd.Series):
            self.col = pd.Series(val for val in self.col)

        # store non-null column values
        self.col_nn = self.col[self.col.notnull()]

    def get_col_props(self):
        """Get column properties

        :returns dict: Column properties
        """

        # determine data-type categories
        is_int = isinstance(self.col.dtype.type(), np.integer)
        is_ts = isinstance(self.col.dtype.type(), np.datetime64)
        is_num = is_ts or isinstance(self.col.dtype.type(), np.number)

        return dict(dtype=self.col.dtype, is_num=is_num, is_int=is_int, is_ts=is_ts)

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
        cnt, var_cnt, dist_cnt = (len(self.col), len(self.col_nn), self.col.nunique())
        for stat_var, stat_val in zip(('count', 'filled', 'distinct'), (cnt, var_cnt, dist_cnt)):
            self.stat_vars.append(stat_var)
            self.stat_vals[stat_var] = (stat_val, '{:d}'.format(stat_val))

        # add value counts to print lines
        self.print_lines.append('{}:'.format(self.label if self.label else self.name))
        self.print_lines.append('{0:d} entries ({1:.0f}%)'.format(var_cnt, var_cnt / cnt * 100))
        self.print_lines.append('{0:d} unique entries'.format(dist_cnt))

        # convert time stamps to integers
        if col_props['is_ts']:
            col_num = self.col_nn.astype(int)
        else:
            col_num = self.col_nn

        # get additional statistics for numeric variables
        if col_props['is_num']:
            stat_vars = ('mean', 'std', 'min', 'max', 'p01', 'p05', 'p16', 'p50', 'p84', 'p95', 'p99')
            stat_vals = (col_num.mean(), col_num.std(), col_num.min(), col_num.max())\
                + tuple(col_num.quantile((0.01, 0.05, 0.16, 0.50, 0.84, 0.95, 0.99)))
            self.stat_vars += stat_vars
            for stat_var, stat_val in zip(stat_vars, stat_vals):
                if not col_props['is_ts']:
                    # value entry for floats and integers
                    self.stat_vals[stat_var] = (stat_val, '{:+g}'.format(stat_val))
                else:
                    if stat_var != 'std':
                        # display time stamps as date/time strings
                        self.stat_vals[stat_var] = (stat_val, str(pd.Timestamp(int(stat_val))))
                    else:
                        # display time-stamp range as number of days
                        stat_val /= NUM_NS_DAY
                        self.stat_vals[stat_var] = (stat_val, '{:g}'.format(stat_val))

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
        table = [(stat_var, self.stat_vals[stat_var][1]) for stat_var in self.stat_vars]
        return tabulate.tabulate(table, tablefmt='latex')

    def plot_histogram(self, var_bins=30, var_range=None, y_label=None):
        """Create and plot histogram of column values

        :param int var_bins: Number of histogram bins
        :param tuple var_range: Range of histogram variable
        :param str y_label: Label for histogram y-axis
        """

        # create statistics overview
        if not self.stat_vals:
            self.create_stats()

        # determine column properties
        col_props = self.get_col_props()

        if col_props['is_num']:
            # determine histogram range for numeric variable
            if var_range:
                # get minimum and maximum of variable for histogram from specified range
                var_min, var_max = var_range
                if col_props['is_ts']:
                    # convert minimum and maximum to Unix time stamps
                    var_min, var_max = pd.Timestamp(var_min).value, pd.Timestamp(var_max).value
            else:
                # determine minimum and maximum of variable for histogram from percentiles
                var_min, var_max = self.stat_vals.get('p05')[0], self.stat_vals.get('p95')[0]
                var_min -= 0.1 * (var_max - var_min)
                var_max += 0.1 * (var_max - var_min)
                if var_min > 0. and var_min < +0.2 * (var_max - var_min):
                    var_min = 0.
                elif var_max < 0. and var_max > -0.2 * (var_max - var_min):
                    var_max = 0.
            if col_props['is_int'] or col_props['is_ts']:
                # use bins around integer values
                bin_width = np.max((np.round((var_max - var_min) / float(var_bins)), 1.))
                var_min = np.floor(var_min - 0.5) + 0.5
                var_bins = int((var_max - var_min) // bin_width) + int((var_max - var_min) % bin_width > 0.)
                var_max = var_min + var_bins * bin_width
                if col_props['is_ts']:
                    # convert Unix time stamps to Pandas time stamps
                    var_min, var_max = pd.Timestamp(var_min), pd.Timestamp(var_max)

            # plot histogram
            self.col_nn.hist(bins=var_bins, range=(var_min, var_max))

            # set x-axis properties
            plt.xlim((var_min, var_max))
            plt.xticks(fontsize=12, rotation=90 if col_props['is_ts'] else 0)

        else:
            # get data from data frame for categorical column
            val_counts = self.col_nn.value_counts(sort=True).iloc[:var_bins].to_dict()
            labels = sorted(lab for lab in val_counts.keys())
            values = [val_counts[lab] for lab in labels]

            # plot histogram
            tick_pos = np.arange(len(labels)) + 0.5
            plt.bar(tick_pos - 0.4, values, width=0.8)

            # set x-axis properties
            def xtick(lab):
                lab = str(lab)
                if len(lab) > 20:
                    lab = lab[:17] + '...'
                return lab
            plt.xlim((0., float(len(labels))))
            plt.xticks(tick_pos, [xtick(lab) for lab in labels], fontsize=12, rotation=90)

        # set common histogram properties
        x_lab = self.label if self.label else self.name
        if self.unit:
            x_lab += ' [{}]'.format(self.unit)
        plt.xlabel(x_lab, fontsize=14)
        plt.ylabel(str(y_label) if y_label is not None else 'Bin count', fontsize=14)
        plt.yticks(fontsize=12)
