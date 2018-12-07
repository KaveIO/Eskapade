"""Project: Eskapade - A python-based package for data analysis.

Class : correlation_summary

Created: 2017/03/13

Description:
    Algorithm to do create correlation heatmaps.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import os

import numpy as np
import pandas as pd
import tabulate

from eskapade import process_manager, resources, Link, DataStore, StatusCode
from eskapade import visualization
from escore.core import persistence
from eskapade.analysis.correlation import calculate_correlations

ALL_CORRS = ['pearson', 'kendall', 'spearman', 'correlation_ratio', 'phik', 'significance']
LINEAR_CORRS = ['pearson', 'kendall', 'spearman']


class CorrelationSummary(Link):
    """Create a heatmap of correlations between dataframe variables."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input dataframe to read from data store
        :param str store_key: key of correlations dataframe in data store
        :param str results_path: path to save correlation summary pdf
        :param list methods: method(s) of computing correlations
        :param str pages_key: data store key of existing report pages
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'correlation_summary'))

        # process arguments
        self._process_kwargs(kwargs, read_key='', store_key='', results_path='', methods=ALL_CORRS, pages_key='')
        self.check_extra_kwargs(kwargs)

    def _process_results_path(self):
        """Process results_path argument."""
        if not self.results_path:
            self.results_path = persistence.io_path('results_data', 'report')
        persistence.create_dir(self.results_path)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, store_key=str, results_path=str, methods=list, pages_key=str)
        self.check_arg_vals('read_key')

        # read report templates
        with open(resources.template('df_summary_report.tex')) as templ_file:
            self.report_template = templ_file.read()
        with open(resources.template('df_summary_report_page.tex')) as templ_file:
            self.page_template = templ_file.read()

        self._process_results_path()

        # check methods
        for method in self.methods:
            if method not in ALL_CORRS:
                logstring = '"{}" is not a valid correlation method, please use one of {}'
                logstring = logstring.format(method, ', '.join(['"' + m + '"' for m in ALL_CORRS]))
                raise AssertionError(logstring)

        # initialize attributes
        self.pages = []

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        # fetch and check input data frame
        # drop all-nan columns right away
        df = ds.get(self.read_key, None).dropna(how='all', axis=1)
        if not isinstance(df, pd.DataFrame):
            self.logger.fatal('No Pandas data frame "{self.read_key}" found in data store for {self!s}.', self=self)
            raise RuntimeError('No input data found for {!s}.', self)
        n_df = len(df.index)
        assert n_df, 'Pandas data frame "{}" frame has zero length.'.format(self.read_key)

        # create report pages
        if self.pages_key:
            self.pages = ds.get(self.pages_key, [])
            assert isinstance(self.pages, list), 'Pages key {} does not refer to a list.'.format(self.pages_key)

        # below, create report pages
        # for each correlation create resulting heatmap
        cors_list = []

        for method in self.methods:
            # compute correlations between all numerical variables
            self.logger.debug('Computing "{method}" correlations of dataframe "{key}".',
                              method=method, key=self.read_key)

            cors, cols = calculate_correlations(df, method)

            # replace column names with indices, as with numpy matrix, for plotting function below
            n = len(cols)
            cors.columns = range(n)

            # keep for potential later usage
            cors_list.append(cors)

            # plot settings
            title = '{0:s} correlation matrix'.format(method.capitalize())
            vmin = -1 if method in LINEAR_CORRS else 0
            vmax = 10 if method == 'significance' else 1
            color_map = 'RdYlGn' if method in LINEAR_CORRS else 'YlGn'
            fname = '_'.join(['correlations', self.read_key.replace(' ', ''), method]) + '.pdf'
            fpath = os.path.join(self.results_path, fname)

            # create nice looking plot
            self.logger.debug('Saving correlation heatmap as "{path}".', path=fpath)
            visualization.vis_utils.plot_correlation_matrix(cors, cols, cols, fpath, title, vmin, vmax, color_map)

            # statistics table for report page
            n_unique = (n * n - n) / 2 if method is not 'correlation_ratio' else n * n
            stats = [('entries', n_df), ('bins', n * n), ('unique', n_unique),
                     ('> 0', (cors.values.ravel() > 0).sum()),
                     ('< 0', (cors.values.ravel() < 0).sum()),
                     ('avg', np.average(cors.values.ravel())),
                     ('max', max(cors.values.ravel())),
                     ('min', min(cors.values.ravel()))] if n > 0 else []
            stats_table = tabulate.tabulate(stats, tablefmt='latex')

            # add plot and table as page to report
            self.pages.append(self.page_template.replace('VAR_LABEL', title)
                              .replace('VAR_STATS_TABLE', stats_table)
                              .replace('VAR_HISTOGRAM_PATH', fpath))

        # save correlations to datastore if requested
        if self.store_key:
            ds[self.store_key] = cors_list
        if self.pages_key:
            ds[self.pages_key] = self.pages

        return StatusCode.Success

    def finalize(self):
        """Finalize the link."""
        # write report file
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(
                self.report_template.replace(
                    'INPUT_PAGES', ''.join(
                        self.pages)))

        return StatusCode.Success
