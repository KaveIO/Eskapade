"""Project: Eskapade - A python-based package for data analysis.

Class: mimic_report

Created: 2018-10-04

Description:
    Algorithm to ...(fill in one-liner here)

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode, resources
from eskapade.core import persistence
from eskapade.visualization import vis_utils as plt

import numpy as np
import os
import tabulate


class MimicReport(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'mimic_report'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs,
                             read_key=None,
                             resample_read_key=None,
                             store_key=None,
                             plot_as_histograms=None,
                             new_column_order_read_key=None,
                             results_path='',
                             chi2_read_key=None,
                             p_value_read_key=None,
                             )

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off the line above, and on the line below if you wish to keep these extra kwargs.
        # self._process_kwargs(kwargs)

    def _process_results_path(self):
        """Process results_path argument."""
        if not self.results_path:
            self.results_path = persistence.io_path('results_data', 'report')
        persistence.create_dir(self.results_path)

    def initialize(self):
        """Initialize the link.

        :returns: status code of initialization
        :rtype: StatusCode
        """
        # -- initialize the report template and page template
        with open(resources.template('df_summary_report.tex')) as tmpl_file:
            self.report_template = tmpl_file.read()
        with open(resources.template('df_summary_report_page.tex')) as page_tmp_file:
            self.page_template = page_tmp_file.read()

        # -- Process the results path
        self._process_results_path()

        # --  List to save the pdf pages
        self.pages = []

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        # Make sure we're dealing with an array not a dataframe
        try:
            assert type(ds[self.read_key]) == np.ndarray
        except AssertionError as e:
            e.args += (type(ds[self.read_key]), 'Wrong type, must be np.ndarray')
            raise

        # -- plot the first page

        title = "PairPlot of Original and Resampled data"
        fname = 'PairPlot.png'
        fpath = os.path.join(self.results_path, fname)

        plt.plot_pair_grid(data=ds[self.read_key][:, ds['continuous_i']],
                           data2=ds[self.resample_read_key][:, ds['continuous_i']],
                           column_names=np.array(ds[self.new_column_order_read_key])[ds['continuous_i']],
                           fpath=fpath,
                           title=title)

        stats = [('Entries', len(ds[self.read_key])),
                 ("Chi2", ds[self.chi2_read_key]),
                 ("P value", ds[self.p_value_read_key]), ]
        stats_table = tabulate.tabulate(stats, tablefmt='latex')

        self.pages.append(
            self.page_template.replace("VAR_LABEL", title)
                              .replace('VAR_STATS_TABLE', stats_table)
                              .replace("VAR_HISTOGRAM_PATH", fpath))

        # -- plot the histograms
        for thing in range(ds[self.read_key].shape[1]):

            data = ds[self.read_key][:, thing]
            data_r = ds[self.resample_read_key][:, thing]

            # -- remove nans
            data = data[~np.isnan(data)]
            data_r = data_r[~np.isnan(data_r)]

            # --- save plot
            if thing in ds['continuous_i']:
                str_title = 'Continuous variable'
            elif thing in ds['unordered_categorical_i']:
                str_title = 'Unordered Catagorical variable'
            elif thing in ds['ordered_categorical_i']:
                str_title = 'Ordered Categorical variable'

            title = f'Histogram for {str_title} {ds[self.new_column_order_read_key][thing]}.'
            fname = f'{ds[self.new_column_order_read_key][thing]}_histogram.pdf'
            fpath = os.path.join(self.results_path, fname)

            if thing in ds['continuous_i']:
                hist_original = np.histogram(data, bins='auto')
                hist_resampled = np.histogram(data_r, bins=len(hist_original[0]))
                width = None
                xlim = None
            elif (thing in ds['unordered_categorical_i']) | (thing in ds['ordered_categorical_i']):

                bin_edges, bin_counts = np.unique(data, return_counts=True)
                bin_edges = np.append(bin_edges, max(bin_edges) + 1)
                hist_original = (bin_counts, bin_edges)

                bin_edges, bin_counts = np.unique(data_r, return_counts=True)
                bin_edges = np.append(bin_edges, max(bin_edges) + 1)
                hist_resampled = (bin_counts, bin_edges)
                width = 0.9
                xlim = (bin_edges[0] - 1, bin_edges[-1])

                # chi, p = chisquare(hist_resampled[0].flatten(), hist_original[0].flatten(), ddoff=)

            stats = [('Entries', len(data), len(data_r)),
                     ('bins', len(hist_original[0]), len(hist_resampled[0])),
                     ('avg', np.average(data), np.average(data_r)),
                     ('max', np.max(data), np.max(data_r)),
                     ('min', np.min(data), np.min(data_r)), ]

            stats_table = tabulate.tabulate(stats, ['Original', 'Resampled'], tablefmt='latex')
            plt.plot_overlay_histogram([hist_original, hist_resampled], hist_names=['Original', 'Resampled'],
                                       x_label=ds[self.new_column_order_read_key][thing], pdf_file_name=fpath,
                                       width_in=width, xlim=xlim)

            # -- add plot to page and page to pages
            self.pages.append(
                self.page_template.replace("VAR_LABEL", title)
                                  .replace('VAR_STATS_TABLE', stats_table)
                                  .replace('VAR_HISTOGRAM_PATH', fpath))

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(
                self.report_template.replace(
                    'INPUT_PAGES', ''.join(
                        self.pages)))

        return StatusCode.Success
