# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : DfSummary                                                             *
# * Created: 2017/02/17                                                            *
# *                                                                                *
# * Description:                                                                   *
# *      Link to create a statistics summary of data frame columns                 *
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
import tabulate

from eskapade import StatusCode, DataStore, Link, ProcessManager, ConfigObject
from eskapade import core, visualization
from eskapade.analysis import statistics

NUMBER_OF_BINS = 30


class DfSummary(Link):
    """Create a summary of a dataframe

    Creates a report page for each variable in data frame, containing:

    * a profile of the column dataset
    * a nicely scaled plot of the column dataset

    Example is available in: tutorials/esk301_dfsummary_plotter.py
    """

    def __init__(self, **kwargs):
        """Initialize the DfSummary link

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str results_path: output path of summary result files
        :param list columns: columns pick up from input data to make & plot summaries for
        :param dict var_labels: dict of column names with a label per column
        :param dict var_units: dict of column names with a unit per column
        :param dict var_bins: dict of column names with the number of bins per column. Default per column is 30.
        :param str hist_y_label: y-axis label to plot for all columns. Default is 'Bin Counts'.
        """

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'df_summary'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', results_path='', columns=None,
                             var_labels={}, var_units={}, var_bins={},
                             hist_y_label='Bin counts')
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.pages = []

    def initialize(self):
        """Inititialize DfSummary link"""

        # check input arguments
        self.check_arg_types(read_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str,
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
            self.results_path = core.persistence.io_path(
                'results_data', io_conf, 'report')

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
        """Execute DfSummary

        Creates a report page for each variable in data frame.

        * create statistics object for column
        * create overview table of column variable
        * plot histogram of column variable
        * store plot
        """

        # import matplotlib here to prevent import before setting backend in
        # core.execution.run_eskapade
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages

        # fetch and check input data frame
        data = ProcessManager().service(DataStore).get(self.read_key, None)
        if not isinstance(data, pd.DataFrame):
            self.log().critical('No Pandas data frame "%s" found in data store for %s', self.read_key, str(self))
            raise RuntimeError('no input data found for %s' % str(self))

        # create report page for each variable in data frame
        self.pages = []
        nan_counts = []
        all_columns = sorted((data if self.columns is None else self).columns.tolist())
        for col in all_columns[:]:
            # output column name
            self.log().debug('processing column "%s"', col)

            # check if column is in data frame
            if col not in data:
                self.log().warning('column "%s" not in data frame', col)
                all_columns.remove(all_columns.index(col))
                continue

            # skip columns consisting entirely of nans
            nan_cnt = data[col].isnull().sum()
            nan_counts.append(nan_cnt)
            if nan_cnt == len(data.index):
                self.log().debug('column "%s" consists of nans only. Skipping.', col)
                continue

            # 1. create statistics object for column
            var_label = self.var_labels.get(col, col)
            stats = statistics.ArrayStats(data, col, unit=self.var_units.get(col, ''), label=var_label)
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
            visualization.vis_utils.plot_histogram(nphist,
                                                   x_label=x_label,
                                                   y_label=y_label,
                                                   is_num=is_num, is_ts=is_ts,
                                                   pdf_file_name=pdf_file_name)

            # create overview table of column variable
            stats_table = stats.get_latex_table()

            # create page string for report
            self.pages.append(self.page_template.replace('VAR_LABEL', var_label)
                                                .replace('VAR_STATS_TABLE', stats_table)
                                                .replace('VAR_HISTOGRAM_PATH', hist_file_name))

        # add nan histogram to summary
        nan_hist = nan_counts, all_columns
        self.process_nan_histogram(nan_hist, len(data.index))

        # write report file
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(
                self.report_template.replace(
                    'INPUT_PAGES', ''.join(
                        self.pages)))

        return StatusCode.Success

    def process_nan_histogram(self, nphist, n_data):
        """Process nans histogram

        Add nans histogram to pdf list

        :param nphist: numpy-style input histogram, consisting of comma-separaged bin_entries, bin_edges
        :param int n_data: number of entries in the processed data set
        """

        var_label = 'NaN count'
        x_label = 'Column name'
        y_label = self.hist_y_label if self.hist_y_label else None
        hist_file_name = 'hist_NaNs.pdf'
        pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, hist_file_name)
        visualization.vis_utils.plot_histogram(nphist,
                                               x_label=x_label,
                                               y_label=y_label,
                                               is_num=False, is_ts=False,
                                               pdf_file_name=pdf_file_name)
        table = [('count', '%d' % n_data)]
        stats_table = tabulate.tabulate(table, tablefmt='latex')
        self.pages.append(self.page_template.replace('VAR_LABEL', var_label)
                          .replace('VAR_STATS_TABLE', stats_table)
                          .replace('VAR_HISTOGRAM_PATH', hist_file_name))
