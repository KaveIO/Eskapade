# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : HistSummary                                                           *
# * Created: 2017/03/06                                                            *
# * Description:                                                                   *
# *      Link to create a statistics summary of a set of histograms                *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
import pandas as pd
import numpy as np
import tabulate

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade import core, visualization
from eskapade.analysis import statistics

NUMBER_OF_BINS = 30


class HistSummary(Link):
    """Create a summary document of input histograms

    The summary contains a page for each histogram, containing:

    * a profile of the histogram
    * a nicely scaled plot of the histogram

    Example is available in: tutorials/esk303_histogram_filling_plotting.py
    """

    def __init__(self, **kwargs):
        """Initialize HistSummary instance

        Store and do basic check on the attributes of link HistSummary.

        :param str name: name of link
        :param str read_key: key of input histograms dictionary to read from data store
        :param str results_path: output path of summary result files
        :param list hist_keys: histograms keys pick up from input histogram dict to make & plot summaries for
        :param dict var_labels: dict of column names with a label per column
        :param dict var_units: dict of column names with a unit per column
        :param dict var_bins: dict of column names with the number of bins per column. Default per column is 30.
        :param str hist_y_label: y-axis label to plot for all columns. Default is 'Bin Counts'.
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'HistSummary'))

        # process keyword arguments
        self._process_kwargs(kwargs,
                             read_key='',
                             results_path='',
                             hist_keys=None,
                             var_labels={},
                             var_units={},
                             var_bins={},
                             hist_y_label='Bin counts')
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.pages = []

    def initialize(self):
        """Initialize HistSummary

        :returns: initialization status code
        :rtype: StatusCode
        """

        # check input arguments
        self.check_arg_types(read_key=str)
        self.check_arg_types(recurse=True, allow_none=True, hist_keys=str,
                             var_labels=str, var_units=str)
        self.check_arg_vals('read_key')

        # get I/O configuration
        io_conf = ProcessManager().service(ConfigObject).io_conf()

        # read report templates
        with open(core.persistence.io_path('templates', io_conf, 'df_summary_report.tex')) as templ_file:
            self.report_template = templ_file.read()
        with open(core.persistence.io_path('templates', io_conf, 'df_summary_report_page.tex')) as templ_file:
            self.page_template = templ_file.read()

        # get path to results directory
        if not self.results_path:
            self.results_path = core.persistence.io_path('results_data', io_conf, 'report')

        # check if output directory exists
        if os.path.exists(self.results_path):
            # check if path is a directory
            if not os.path.isdir(self.results_path):
                self.log().critical('output path "%s" is not a directory', self.results_path)
                raise AssertionError('output path is not a directory')
        else:
            # create directory
            self.log().debug('Making output directory %s', self.results_path)
            os.makedirs(self.results_path)

        return StatusCode.Success

    def execute(self):
        """Create a report of the data frame variables

        * create statistics object for column
        * create overview table of column variable
        * plot histogram of column variable
        * store plot

        :returns: execution status code
        :rtype: StatusCode
        """

        # fetch and check input data frame
        hist_dict = ProcessManager().service(DataStore).get(self.read_key, None)
        if not isinstance(hist_dict, dict):
            self.log().critical('no histograms "%s" found in data store for %s', self.read_key, str(self))
            raise RuntimeError('no input data found for %s' % str(self))

        if self.hist_keys is None:
            self.hist_keys = hist_dict.keys()

        # create report page for histogram
        self.pages = []
        for name in self.hist_keys:
            # histogram name
            self.log().info('processing histogram "%s"', name)

            # check if histogram is in dict
            if name not in hist_dict:
                self.log().warning('histogram "%s" not in dictionary "%s"', name, self.read_key)
                continue
            hist = hist_dict[name]

            # process each histogram (plot and make summary table)
            if hist.n_dim == 1:
                self.process_1d_histogram(name, hist)
            elif hist.n_dim == 2:
                self.process_2d_histogram(name, hist)

        # write out accumulated histogram statistics into report file
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(self.report_template.replace('INPUT_PAGES', ''.join(self.pages)))

        return StatusCode.Success

    def process_1d_histogram(self, name, hist):
        """Create statistics of and plot input 1d histogram

        :param hist: input histogram object
        :param str name: name of the histogram
        """

        # datatype properties
        datatype = hist.datatype
        col_props = statistics.get_col_props(datatype)
        is_num = col_props['is_num']
        is_ts = col_props['is_ts']

        # retrieve _all_ filled bins to evaluate statistics
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
        hist_file_name = 'hist_{}.pdf'.format(name)
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
        """Create statistics of and plot input 2d histogram

        :param hist: input histogram object
        :param str name: name of the histogram
        """

        if not hasattr(hist, 'xy_ranges_grid'):
            self.log().warning('No plot for 2d hist <%s>. Cannot extract binning and values from input histogram.')
            return
        try:
            nphist = hist.xy_ranges_grid()
        except:
            raise RuntimeError('Cannot extract binning and values from input histogram')

        # calc some basic histogram statistics
        sum_entries = 0
        try:
            grid = nphist[2]
            ynum = len(grid)
            xnum = len(grid[0])
            for i in range(xnum):
                for j in range(ynum):
                    sum_entries += grid[i, j]
        except:
            pass
        sum_entries = int(sum_entries)

        # create LaTeX string of statistics
        table = [('count', '%d' % sum_entries)]
        stats_table = tabulate.tabulate(table, tablefmt='latex')

        # histogram attributes for plotting
        var_label = self.var_labels.get(name, name.replace(':', '_vs_'))
        try:
            xlab = name.split(':')[0]
            ylab = name.split(':')[1]
        except:
            xlab = 'unknown x'
            ylab = 'unknown y'
        hist_file_name = 'hist_{}.pdf'.format(name.replace(':', '_vs_'))
        pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, hist_file_name)

        # plot the 2d histogram
        visualization.vis_utils.plot_2d_histogram(nphist, x_lim=hist.x_lim(), y_lim=hist.y_lim(), title=var_label,
                                                  x_label=xlab, y_label=ylab, pdf_file_name=pdf_file_name)

        # create page string for report
        page_templ = self.page_template
        for kv in [('VAR_LABEL', var_label), ('VAR_STATS_TABLE', stats_table), ('VAR_HISTOGRAM_PATH', hist_file_name)]:
            page_templ = page_templ.replace(*kv)
        self.pages.append(page_templ)
