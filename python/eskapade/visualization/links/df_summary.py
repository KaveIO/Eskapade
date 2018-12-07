"""Project: Eskapade - A python-based package for data analysis.

Class : DfSummary

Created: 2017/02/17

Description:
    Link to create a statistics summary of data frame columns
    or of a set of histograms.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd
import tabulate

from eskapade import resources, visualization
from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.analysis import statistics
from escore.core import persistence

NUMBER_OF_BINS = 30


class DfSummary(Link):
    """Create a summary of a dataframe.

    Creates a report page for each variable in data frame, containing:

    * a profile of the column dataset
    * a nicely scaled plot of the column dataset

    Example 1 is available in: tutorials/esk301_dfsummary_plotter.py

    Example 2 is available in: tutorials/esk303_histogram_filling_plotting.py
    Empty histograms are automatically skipped from processing.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input dataframe (or histogram-dict) to read from data store
        :param str results_path: output path of summary result files
        :param list columns: columns (or histogram keys) pick up from input data to make & plot summaries for
        :param list hist_keys: alternative to columns (optional)
        :param dict var_labels: dict of column names with a label per column
        :param dict var_units: dict of column names with a unit per column
        :param dict var_bins: dict of column names with the number of bins per column. Default per column is 30.
        :param str hist_y_label: y-axis label to plot for all columns. Default is 'Bin Counts'.
        :param str pages_key: data store key of existing report pages
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'df_summary'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', results_path='', columns=[],
                             hist_keys=[],
                             var_labels={}, var_units={}, var_bins={},
                             hist_y_label='Bin counts', pages_key='')
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.pages = []
        self.nan_counts = []

    def _process_results_path(self):
        """Process results_path argument."""
        if not self.results_path:
            self.results_path = persistence.io_path('results_data', 'report')
        persistence.create_dir(self.results_path)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, pages_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str, hist_keys=str, var_labels=str, var_units=str)
        self.check_arg_vals('read_key')

        # read report templates
        with open(resources.template('df_summary_report.tex')) as templ_file:
            self.report_template = templ_file.read()
        with open(resources.template('df_summary_report_page.tex')) as templ_file:
            self.page_template = templ_file.read()

        self._process_results_path()
        # add hist_keys to columns, ensure sum is a unique set
        self.columns += self.hist_keys
        self.columns = sorted(list(set(self.columns)))

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Creates a report page for each variable in data frame.

        * create statistics object for column
        * create overview table of column variable
        * plot histogram of column variable
        * store plot

        :returns: execution status code
        :rtype: StatusCode
        """
        ds = process_manager.service(DataStore)

        # fetch and check input data frame
        data = ds.get(self.read_key, None)
        if data is None:
            self.logger.fatal('No input data "{self.read_key}" found in data store for {self!s}.', self=self)
            raise RuntimeError('no input data found for {!s}'.format(self))
        else:
            self.assert_data_type(data)

        # create report page for histogram
        if self.pages_key:
            self.pages = ds.get(self.pages_key, [])
            if not isinstance(self.pages, list):
                raise TypeError('pages key "{}" does not refer to a list'.format(self.pages_key))

        # determine all possible columns, used for comparison below
        all_columns = self.get_all_columns(data)
        if not self.columns:
            self.columns = all_columns

        for name in self.columns[:]:
            # check if column is in data frame
            if name not in all_columns:
                self.logger.warning('Key "{key}" not in input data; skipping.', key=name)
                self.columns.remove(self.columns.index(name))
                continue
            self.logger.debug('Processing "{col}".', col=name)
            sample = self.get_sample(data, name)
            self.process_sample(name, sample)

        # add nan histogram to summary if present
        if self.nan_counts:
            nan_hist = self.nan_counts, self.columns
            self.process_nan_histogram(nan_hist, self.get_length(data))

        # storage
        if self.pages_key:
            ds[self.pages_key] = self.pages

        return StatusCode.Success

    def finalize(self):
        """Finalize the link."""
        # write report file
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(self.report_template.replace('INPUT_PAGES', ''.join(self.pages)))

        return StatusCode.Success

    def assert_data_type(self, data):
        """Check type of input data.

        :param data: input data sample (pandas dataframe or dict)
        """
        if not isinstance(data, pd.DataFrame) and not isinstance(data, dict):
            raise AssertionError('input data should be pandas dataframe or dict (with histograms)')

    def get_all_columns(self, data):
        """Retrieve all columns / keys from input data.

        :param data: input data sample (pandas dataframe or dict)
        :returns: list of columns
        :rtype: list
        """
        if isinstance(data, pd.DataFrame):
            all_columns = sorted(data.columns.tolist())
        elif isinstance(data, dict):
            # dict of histograms
            all_columns = sorted(data.keys())
        else:
            raise RuntimeError('cannot determine columns in input data found for {!s}'.format(self))

        return all_columns

    def get_sample(self, data, key):
        """Retrieve speficic column or item from input data.

        :param data: input data (pandas dataframe or dict)
        :param str key: column key
        :returns: data series or item
        """
        return data[key]

    def get_length(self, data):
        """Get length of data set.

        :param data: input data (pandas dataframe or dict)
        :returns: length of data set
        """
        return len(data)

    def process_sample(self, name, sample):
        """Process various possible data samples.

        :param str name: name of sample
        :param sample: input pandas series object or histogram
        """
        # process pandas series (plot and make summary table)
        if isinstance(sample, pd.core.series.Series):
            self.process_series(name, sample)
        # else process histograms
        elif hasattr(sample, 'n_dim'):
            if sample.n_dim == 1:
                self.process_1d_histogram(name, sample)
            elif sample.n_dim == 2:
                self.process_2d_histogram(name, sample)

    def process_series(self, col, sample):
        """Create statistics of and plot input pandas series.

        :param str col: name of the series
        :param sample: input pandas series object
        """
        # skip columns consisting entirely of nans
        nan_cnt = sample.isnull().sum()
        self.nan_counts.append(nan_cnt)
        if nan_cnt == len(sample.index):
            self.logger.debug('Column "{col}" consists of nans only; skipping.', col=col)
            return

        # 1. create statistics object for column
        var_label = self.var_labels.get(col, col)
        stats = statistics.ArrayStats(sample, col, unit=self.var_units.get(col, ''), label=var_label)
        # evaluate statitical properties of array
        stats.create_stats()

        # make histogram
        nphist = stats.make_histogram(var_bins=self.var_bins.get(col, NUMBER_OF_BINS))

        # determine histogram properties for plotting
        x_label = stats.get_x_label()
        y_label = self.hist_y_label if self.hist_y_label else None
        is_num = stats.get_col_props()['is_num']
        is_ts = stats.get_col_props()['is_ts']
        hist_file_name = 'hist_{}.pdf'.format(col)
        pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, hist_file_name)

        # 3. plot histogram of column variable
        visualization.vis_utils.plot_histogram(nphist, x_label=x_label, y_label=y_label, is_num=is_num, is_ts=is_ts,
                                               pdf_file_name=pdf_file_name)

        # create overview table of column variable
        stats_table = stats.get_latex_table()

        # create page string for report
        self.pages.append(self.page_template.replace('VAR_LABEL', var_label).replace('VAR_STATS_TABLE', stats_table)
                          .replace('VAR_HISTOGRAM_PATH', hist_file_name))

    def process_1d_histogram(self, name, hist):
        """Create statistics of and plot input 1d histogram.

        :param str name: name of the histogram
        :param hist: input histogram object
        """
        # datatype properties
        datatype = hist.datatype
        col_props = statistics.get_col_props(datatype)
        is_num = col_props['is_num']
        is_ts = col_props['is_ts']

        # skip empty histograms
        n_bins = hist.n_bins
        if n_bins == 0:
            self.logger.warning('Histogram "{name}" is empty; skipping.', name=name)
            return

        bin_labels = hist.bin_centers() if is_num else hist.bin_labels()
        bin_counts = hist.bin_entries()
        bin_edges = hist.bin_edges() if is_num else None

        if is_ts:
            to_timestamp = np.vectorize(lambda x: pd.Timestamp(x))
            bin_labels = to_timestamp(bin_labels)
            bin_edges = to_timestamp(bin_edges)

        # create statistics object for histogram
        var_label = self.var_labels.get(name, name)
        stats = statistics.ArrayStats(bin_labels, name, weights=bin_counts, unit=self.var_units.get(name, ''),
                                      label=var_label)
        # evaluate statitical properties of array
        stats.create_stats()

        # make nice plots here ...
        # for numbers and timestamps, make cropped histogram, between percentiles 5-95%
        # ... and project on existing binning.
        # for categories, accept top N number of categories in bins.
        # NB: bin_edges overrules var_bins (if it is not none)
        nphist = stats.make_histogram(var_bins=self.var_bins.get(name, NUMBER_OF_BINS), bin_edges=bin_edges)

        # determine histogram properties for plotting below
        x_label = stats.get_x_label()
        y_label = self.hist_y_label if self.hist_y_label else None
        hist_file_name = 'hist_{}.pdf'.format(name.replace(' ', '_'))
        pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, hist_file_name)

        # matplotlib plot of histogram
        visualization.vis_utils.plot_histogram(nphist, x_label=x_label, y_label=y_label, is_num=is_num, is_ts=is_ts,
                                               pdf_file_name=pdf_file_name)

        # create overview table of histogram statistics
        stats_table = stats.get_latex_table()

        # create page string
        page_templ = self.page_template
        for kv in [('VAR_LABEL', var_label), ('VAR_STATS_TABLE', stats_table), ('VAR_HISTOGRAM_PATH', hist_file_name)]:
            page_templ = page_templ.replace(*kv)
        self.pages.append(page_templ)

    def process_2d_histogram(self, name, hist):
        """Create statistics of and plot input 2d histogram.

        :param str name: name of the histogram
        :param hist: input histogram object
        """
        if not hasattr(hist, 'xy_ranges_grid'):
            self.logger.warning('No plot for 2d hist "{name}"; cannot extract binning and values.', name=name)
            return
        try:
            nphist = hist.xy_ranges_grid()
        except BaseException:
            raise RuntimeError('Cannot extract binning and values from input histogram.')

        # calc some basic histogram statistics
        sum_entries = 0
        try:
            grid = nphist[2]
            ynum = len(grid)
            xnum = len(grid[0])
            for i in range(xnum):
                for j in range(ynum):
                    sum_entries += grid[i, j]
        except BaseException:
            pass
        sum_entries = int(sum_entries)

        # create LaTeX string of statistics
        table = [('count', '{:d}'.format(sum_entries))]
        stats_table = tabulate.tabulate(table, tablefmt='latex')

        # histogram attributes for plotting
        var_label = self.var_labels.get(name, name.replace(':', '_vs_'))
        try:
            xlab = name.split(':')[0]
            ylab = name.split(':')[1]
        except BaseException:
            xlab = 'unknown x'
            ylab = 'unknown y'
        hist_file_name = 'hist_{}.pdf'.format(name.replace(':', '_vs_').replace(' ', '_'))
        pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, hist_file_name)

        # plot the 2d histogram
        visualization.vis_utils.plot_2d_histogram(nphist, x_lim=hist.x_lim(), y_lim=hist.y_lim(), title=var_label,
                                                  x_label=xlab, y_label=ylab, pdf_file_name=pdf_file_name)

        # create page string for report
        page_templ = self.page_template
        for kv in [('VAR_LABEL', var_label), ('VAR_STATS_TABLE', stats_table), ('VAR_HISTOGRAM_PATH', hist_file_name)]:
            page_templ = page_templ.replace(*kv)
        self.pages.append(page_templ)

    def process_nan_histogram(self, nphist, n_data):
        """Process nans histogram.

        Add nans histogram to pdf list

        :param nphist: numpy-style input histogram, consisting of comma-separaged bin_entries, bin_edges
        :param int n_data: number of entries in the processed data set
        """
        var_label = 'NaN count'
        x_label = 'Column name'
        y_label = self.hist_y_label if self.hist_y_label else None
        hist_file_name = 'hist_NaNs.pdf'
        pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, hist_file_name)
        visualization.vis_utils.plot_histogram(nphist, x_label=x_label, y_label=y_label, is_num=False, is_ts=False,
                                               pdf_file_name=pdf_file_name)
        table = [('count', '{:d}'.format(n_data))]
        stats_table = tabulate.tabulate(table, tablefmt='latex')
        self.pages.append(self.page_template.replace('VAR_LABEL', var_label).replace('VAR_STATS_TABLE', stats_table)
                          .replace('VAR_HISTOGRAM_PATH', hist_file_name))
