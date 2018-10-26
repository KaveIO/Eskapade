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
from eskapade.data_mimic.dm_vis_util import plot_heatmap

import numpy as np
import pandas as pd
import os
import tabulate


class MimicReport(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of original data to read from data store
        :param str resample_read_key: key of the resampled data to read from the data store
        :param str store_key: key of output data to store in data store
        :param str new_column_order_read_key: key of the column order to read from the data store
        :param str results_path: where to save the report
        :param str chi2_read_key: key of the saved chi-square value to read from the data store
        :param str p_value_read_key: key of the saved p-value to read from the data store
        :param str maps: key of the saved maps to read from the data store

        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'mimic_report'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs,
                             read_key=None,
                             resample_read_key=None,
                             store_key=None,
                             new_column_order_read_key=None,
                             results_path='',
                             chi2_read_key=None,
                             p_value_read_key=None,
                             maps_read_key=None,
                             key_data_normalized=None,
                             distance_read_key=None
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
        with open(resources.template('df_summary_table_page.tex')) as page_tmp_file:
            self.page_table_template = page_tmp_file.read()

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

        maps = ds[self.maps_read_key]

        # Make sure we're dealing with an array not a dataframe
        try:
            assert type(ds[self.read_key]) == np.ndarray
            orig_data = ds[self.read_key]
            resa_data = ds[self.resample_read_key]
        except AssertionError:
            orig_df = ds[self.read_key].copy()
            resa_df = ds[self.resample_read_key].copy()

            orig_df = orig_df.dropna()
            resa_df = resa_df.dropna()
            # -- make sure the order is the same
            orig_df = orig_df[ds[self.new_column_order_read_key]]
            resa_df = resa_df[ds[self.new_column_order_read_key]]

            orig_data = orig_df.values
            resa_data = resa_df.values

        # -- plot the histograms

        for thing in range(orig_data.shape[1]):

            data = orig_data[:, thing]
            data_r = resa_data[:, thing]

            # -- remove nans
            try:
                data = data[~np.isnan(data)]
                data_r = data_r[~np.isnan(data_r)]
            except TypeError:
                pass

            # --- save plot
            if thing in ds['continuous_i']:
                str_title = 'Continuous variable'
            elif thing in ds['unordered_categorical_i']:
                str_title = 'Unordered Categorical variable'
            elif thing in ds['ordered_categorical_i']:
                str_title = 'Ordered Categorical variable'

            title = f'Histogram for {str_title} {ds[self.new_column_order_read_key][thing]}.'
            fname = f'{ds[self.new_column_order_read_key][thing]}_histogram.pdf'
            fpath = os.path.join(self.results_path, fname)

            if thing in ds['continuous_i']:
                hrange = (np.round(np.min(data) + np.min(data) * .2), np.round(np.max(data) + np.max(data) * .2))
                # hrange[0] += hrange[0] * .2
                # hrange[1] += hrange[1] * .2

                hist_original = np.histogram(data, range=hrange, bins='auto')
                hist_resampled = np.histogram(data_r, range=hrange, bins=len(hist_original[0]))
                width = None
                xlim = None
                is_num = True
            # elif (thing in ds['unordered_categorical_i']) | (thing in ds['ordered_categorical_i']):
            else:

                bin_edges, bin_counts = np.unique(data, return_counts=True)
                bin_edges_r, bin_counts_r = np.unique(data_r, return_counts=True)

                if ds[self.new_column_order_read_key][thing] in maps.keys():
                    is_num = False
                    xlim = None
                else:
                    is_num = True
                    bin_edges = np.append(bin_edges, max(bin_edges) + 1)
                    bin_edges_r = np.append(bin_edges_r, max(bin_edges_r) + 1)
                    xlim = (bin_edges_r[0] - 1, bin_edges_r[-1])

                hist_original = (bin_counts, bin_edges)
                hist_resampled = (bin_counts_r, bin_edges_r)

                width = 0.9

                # chi, p = chisquare(hist_resampled[0].flatten(), hist_original[0].flatten(), ddoff=)
            try:
                stats = [('Entries', len(data), len(data_r)),
                         ('bins', len(hist_original[0]), len(hist_resampled[0])),
                         ('avg', np.mean(data), np.mean(data_r)),
                         ('max', np.max(data), np.max(data_r)),
                         ('min', np.min(data), np.min(data_r)), ]

            except (TypeError, AttributeError):
                stats = [('Entries', len(data), len(data_r)),
                         ('bins', len(hist_original[0]), len(hist_resampled[0])),
                         ('max', f'{bin_edges[np.argmax(bin_counts)]}: {np.max(bin_counts)}',
                          f'{bin_edges_r[np.argmax(bin_counts_r)]}: {np.min(bin_counts_r)}'),
                         ('min', f'{bin_edges[np.argmin(bin_counts)]}: {np.max(bin_counts)}',
                          f'{bin_edges_r[np.argmin(bin_counts_r)]}: {np.min(bin_counts_r)}'), ]

            stats_table = tabulate.tabulate(stats, ['Original', 'Resampled'], tablefmt='latex')

            plt.plot_overlay_histogram([hist_original, hist_resampled], hist_names=['Original', 'Resampled'],
                                       x_label=ds[self.new_column_order_read_key][thing], pdf_file_name=fpath,
                                       width_in=width, xlim=xlim, is_num=is_num)

            # -- add plot to page and page to pages
            self.pages.append(
                self.page_template.replace("VAR_LABEL", title)
                                  .replace('VAR_STATS_TABLE', stats_table)
                                  .replace('VAR_HISTOGRAM_PATH', fpath))

        # -- plot the normal distr for the numerical data

        for i, key in enumerate(ds['continuous_i']):

            title = "Normalized data for varibale {}".format(ds[self.new_column_order_read_key][key])
            fname = f'Normalized_{ds[self.new_column_order_read_key][key]}_hist.pdf'
            fpath = os.path.join(self.results_path, fname)

            data = ds[self.key_data_normalized][:, i].copy()
            normal = np.random.normal(size=data.shape)

            hrange = (np.round(np.min(data) + np.min(data) * .2), np.round(np.max(data) + np.max(data) * .2))
            # hrange[0] = hrange[0] + hrange[0] * .2
            # hrange[1] = hrange[1] + hrange[1] * .2
            hist = np.histogram(data, range=hrange, bins='auto')
            hist_normal = np.histogram(normal, range=hrange, bins=len(hist[0]))

            plt.plot_overlay_histogram(hists=[hist, hist_normal],
                                       hist_names=['Normalized data', 'Standard Normal distribution'],
                                       x_label=str(ds[self.new_column_order_read_key][key]),
                                       pdf_file_name=fpath,
                                       is_num=True)
            stats = [('Entries', len(data)),
                     ('bins', len(hist[0])),
                     ('avg', np.mean(data)),
                     ('max', np.max(data)),
                     ('min', np.min(data)), ]
            stats_table = tabulate.tabulate(stats, tablefmt='latex')

            self.pages.append(
                self.page_template.replace("VAR_LABEL", title)
                                  .replace('VAR_STATS_TABLE', stats_table)
                                  .replace('VAR_HISTOGRAM_PATH', fpath))

        # -- plot the correlation heatmaps

        # -- reconstruct as dataframe
        title = "Correlation heatmaps"
        fname = f'Heatmaps.pdf'
        fpath = os.path.join(self.results_path, fname)

        o_cont = pd.DataFrame(orig_data[:, ds['continuous_i']],
                              columns=np.array(ds['new_column_order'])[ds['continuous_i']],
                              dtype='float')
        df_o = o_cont.merge(pd.DataFrame(orig_data[:, ds['ordered_categorical_i'] + ds['unordered_categorical_i']],
                            columns=np.array(ds['new_column_order'])[ds['ordered_categorical_i'] +
                                                                     ds['unordered_categorical_i']]),
                            left_index=True, right_index=True)

        r_cont = pd.DataFrame(resa_data[:, ds['continuous_i']],
                              columns=np.array(ds['new_column_order'])[ds['continuous_i']],
                              dtype='float')
        df_r = r_cont.merge(pd.DataFrame(resa_data[:, ds['ordered_categorical_i'] + ds['unordered_categorical_i']],
                            columns=np.array(ds['new_column_order'])[ds['ordered_categorical_i'] +
                                                                     ds['unordered_categorical_i']]),
                            left_index=True, right_index=True)

        plot_heatmap(df_o, df_r, pdf_file_name=fpath)

        stats_table = ''

        self.pages.append(
            self.page_template.replace("VAR_LABEL", title)
                              .replace('VAR_STATS_TABLE', stats_table)
                              .replace('VAR_HISTOGRAM_PATH', fpath))

        # --  metrics
        A = pd.DataFrame([ds['chis'][x] for x in ds['chis'].keys() if x is not 'total'])
        stats_table = ''

        for col in A.columns:

            d = pd.DataFrame([[A[col][i]['chi'], A[col][i]['p-value']] for i in A[col].index],
                             columns=['Chi', 'p-value'], index=A.columns).T
            d['param'] = col.capitalize()

            try:
                fin_df = fin_df.append(d)
            except NameError:
                fin_df = d

            clms = d.columns.values

        stats_table = tabulate.tabulate(fin_df.round(5), headers=clms, tablefmt='latex')

        self.pages.append(
            self.page_table_template.replace("VAR_LABEL", "Chi-square values")
                                    .replace('VAR_STATS_TABLE', stats_table))

        A = pd.DataFrame.from_dict(ds['kss'])
        stats_table = tabulate.tabulate(A.round(5), headers=A.columns, tablefmt='latex')

        self.pages.append(
            self.page_table_template.replace("VAR_LABEL", "KS values")
                                    .replace('VAR_STATS_TABLE', stats_table))

        stats_table = tabulate.tabulate(pd.DataFrame(ds[self.distance_read_key]),
                                        headers=['Description', 'Values'], tablefmt='latex')

        self.pages.append(
            self.page_table_template.replace("VAR_LABEL", "Distance metric. Max distance: 1")
                                    .replace('VAR_STATS_TABLE', stats_table))

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
