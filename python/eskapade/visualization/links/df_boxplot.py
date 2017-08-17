# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : DfBoxplot                                                             *
# * Created: 2017/02/17                                                            *
# *                                                                                *
# * Description:                                                                   *
# *      Link to create a boxplot of data frame columns                            *
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

from eskapade import StatusCode, DataStore, Link, process_manager, ConfigObject
from eskapade import core, visualization
from eskapade.analysis import statistics

NUMBER_OF_RECORDS = 1000


class DfBoxplot(Link):
    """Create a boxplot of one column of a DataFrame that is grouped by values from a second column.

    Creates a report page for each variable in DataFrame, containing:

    * a profile of the column dataset
    * a nicely scaled plot of the boxplots per group of the column

    Example is available in: tutorials/esk304_df_boxplot.py
    """

    def __init__(self, **kwargs):
        """Initialize the DfBoxplot link

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str results_path: output path of summary result files
        :param str column: column pick up from input data to use as boxplot input
        :param list cause_columns: list of columns (str) to group-by, and per unique value plot a boxplot
        :param list statistics: a list of strings of the statistics you want to generate for the boxplot
               the full list is taken from statistics.ArrayStats.get_latex_table
               defaults to: ['count', 'mean', 'min', 'max']
        :param str pages_key: data store key of existing report pages
        """

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'df_boxplot'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', results_path='', column=None, cause_columns=None,
                             var_labels={}, var_units={}, statistics=['count', 'mean', 'min', 'max'], pages_key='')
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.pages = []

    def initialize(self):
        """Inititialize DfBoxplot link"""

        # check input arguments
        self.check_arg_types(read_key=str, pages_key=str)
        self.check_arg_types(recurse=True, allow_none=True, column=str, cause_columns=list, statistics=list)
        self.check_arg_vals('read_key')

        # get I/O configuration
        io_conf = process_manager.service(ConfigObject).io_conf()

        # read report templates, we use the summary_report template from the df summary link
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
        """Execute DfBoxplot

        Creates a report page for each column that we group-by in the data frame.

        * create statistics object for group
        * create overview table of column variable
        * plot boxplot of column variable per group
        * store plot
        """

        # fetch and check input data frame
        ds = process_manager.service(DataStore)
        data = ds.get(self.read_key, None)
        if not isinstance(data, pd.DataFrame):
            self.log().critical('No Pandas data frame "%s" found in data store for %s', self.read_key, str(self))
            raise RuntimeError('no input data found for %s' % str(self))

        # fetch any existing report pages
        if self.pages_key:
            self.pages = ds.get(self.pages_key, [])
            assert isinstance(self.pages, list), 'Pages key %s does not refer to a list' % self.pages_key

        # create report page for each plot
        for col in self.cause_columns:
            # output column name
            self.log().debug('processing cause column "%s"', col)

            # check if column is in data frame
            if col not in data.columns:
                self.log().warning('column "%s" not in data frame', col)
                continue

            # 1. create statistics object for column
            var_label = self.column

            # 2. Calculate the statistical properties per group
            # Notice that in this link we call GroupByStats and in df_summary we call ArrayStats
            stats = statistics.GroupByStats(data, self.column, groupby=col, unit=self.var_units.get(self.column, ''),
                                            label=var_label)

            # 3. plot and store histogram of column variable
            box_file_name = 'boxplot_{}.pdf'.format(col)
            pdf_file_name = '{0:s}/{1:s}'.format(self.results_path, box_file_name)
            visualization.vis_utils.box_plot(data, col, self.column, pdf_file_name=pdf_file_name)

            # 4. create overview table of column variable with a group-by applied by GroupByStats
            stats_table = stats.get_latex_table(get_stats=self.statistics)

            # 5. create page string
            self.pages.append(self.page_template.replace('VAR_LABEL', var_label)
                                                .replace('VAR_STATS_TABLE', stats_table)
                                                .replace('VAR_HISTOGRAM_PATH', box_file_name))

        # storage
        if self.pages_key:
            ds[self.pages_key] = self.pages

        return StatusCode.Success

    def finalize(self):
        """Finalize DfBoxplot"""

        # write report file from the strings in self.pages
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(self.report_template.replace('INPUT_PAGES', ''.join(self.pages)))

        return StatusCode.Success
