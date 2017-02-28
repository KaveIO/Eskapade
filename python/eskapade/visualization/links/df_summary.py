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

from eskapade import StatusCode, DataStore, Link, ProcessManager, ConfigObject
from eskapade import core, visualization


class DfSummary(Link):
    """Create summary of a a dataframe"""

    def __init__(self, **kwargs):
        """Initialize link instance"""

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'df_summary'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', results_path='', columns=None, var_labels={}, var_units={},
                             hist_y_label='Bin counts')
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.pages = []

    def initialize(self):
        """Inititialize DfSummary"""

        # check input arguments
        self.check_arg_types(read_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str, var_labels=str, var_units=str)
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
        """Execute DfSummary"""

        # import matplotlib here to prevent import before setting backend in core.execution.run_eskapade
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages

        # fetch and check input data frame
        data = ProcessManager().service(DataStore).get(self.read_key, None)
        if not isinstance(data, pd.DataFrame):
            self.log().critical('no Pandas data frame "%s" found in data store for %s', self.read_key, str(self))
            raise RuntimeError('no input data found for %s' % str(self))

        # create report page for each variable in data frame
        self.pages = []
        for col in (data if self.columns is None else self).columns:
            # output column name
            self.log().debug('processing column "%s"', col)

            # check if column is in data frame
            if col not in data:
                self.log().warning('column "%s" not in data frame', col)
                continue

            # create statistics object for column
            var_label = self.var_labels.get(col, col)
            stats = visualization.vis_utils.ColumnStats(data, col, unit=self.var_units.get(col, ''), label=var_label)

            # create overview table of column variable
            stats_table = stats.get_latex_table()

            # plot histogram of column variable
            fig = plt.figure(figsize=(7, 5))
            stats.plot_histogram(y_label=self.hist_y_label if self.hist_y_label else None)

            # store plot
            hist_file_name = 'hist_{}.pdf'.format(col)
            pdf_file = PdfPages('{0:s}/{1:s}'.format(self.results_path, hist_file_name))
            plt.savefig(pdf_file, format='pdf', bbox_inches='tight', pad_inches=0)
            plt.close()
            pdf_file.close()

            # create page string
            self.pages.append(self.page_template.replace('VAR_LABEL', var_label)\
                                                .replace('VAR_STATS_TABLE', stats_table)\
                                                .replace('VAR_HISTOGRAM_PATH', hist_file_name))

        # write report file
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(self.report_template.replace('INPUT_PAGES', ''.join(self.pages)))

        return StatusCode.Success