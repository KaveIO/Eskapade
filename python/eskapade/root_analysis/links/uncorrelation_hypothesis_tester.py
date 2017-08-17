# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : UncorrelationHypothesisTester
# * Created: 2017/05/27
# * Description:                                                                   *
# *      Algorithm to test for correlations between categorical observables.       *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import copy
import fnmatch
import os
from collections import OrderedDict

import ROOT
import numpy as np
import pandas as pd
import root_numpy
import tabulate
from numba import jit

from eskapade import process_manager, ConfigObject, Link, DataStore, StatusCode
from eskapade.core import persistence
from eskapade.root_analysis import data_conversion, roofit_utils
from eskapade.root_analysis.roofit_manager import RooFitManager
from eskapade.visualization import vis_utils


class UncorrelationHypothesisTester(Link):
    """Link to test for correlations between categorical observables.

    Test of correlation between categorical observables taking into account the effects
    of low statistics. The test is performed on two levels.

    Significance test: tests correlation between observables.
    This test calculates the (significance of the) p-value of the hypothesis that
    the observables in the input dataset are not correlated. A detailed discription
    of the method can be found in ABCDutils.h

    Residuals test: tests correlation between values of observables.
    This test calculates the normalized residuals (pull values) for each bin in the
    dataset, under the hypothesis of no correlation. A detailed discription of the method
    can be found in ABCDutils.h

    Both tests compare the measured frequency per bin with the expected frequency per bin. The
    expected frequency is calculated on the assumption of no correlation. A detailed description
    of the calculation of the expected frequency can be found in RooABCDHistPDF.cxx.

    Two reports are generated containing the results of the above tests. The long report
    (report.tex) contains all the results. The short report (report_client.tex) contains only
    the most non-correlating measurements. The results are also saved in the DataStore.
    """

    def __init__(self, **kwargs):
        """Initialize UncorrelationHypothesisTester instance

        :param str name: name of link

        :param list columns: list of columns to pick up from dataset and pair. Default is all columns. (optional)
        :param list x_columns: list of columns to be paired with right pair columns (left x right).
        :param list y_columns: list of columns to be paired with left pair columns (left x right).
        :param bool inproduct: if true, take inproduct of x_columns and y_columns. (default is false)
        :param list combinations: list of column combinations from dataset to test.
        :param float z_threshold: significance threshold (in s.d.) for selecting outliers for tables in report.
                                  Default is 3 s.d..
        :param bool verbose_plots: if true, print both number of entries and significance in correlation plots
        :param dict map_to_original: dictiorary or key to dictionary to map back factorized columns to original.
                                     map_to_original is a dict of dicts, one dict for each column.
        :param dict var_number_of_bins: number of bins for certain variable (optional)
        :param int default_number_of_bins: default number of bins for continuous observables. Default setting is 10.
        :param dict var_ignore_categories: ignore category (list of categories) for certain variable (optional)
        :param list ignore_categories: ignore list of categories for all variables if present (optional)
        :param int nsims_per_significance: number of simulation per significance evaluation. Default is 500.
        :param str read_key: key of input data to read from data store
        :param str read_key_vars: key of input rooargset of observables in data store (optional)
        :param bool from_ws: if true, pick up input roodataset from workspace, not datastore. Default is false.
        :param str significance_key: key of calculated significance matrix to store in data store
        :param str sk_significance_map: key of calculated significance map to store in data store
        :param str sk_residuals_map: key of calculated residuals map to store in data store
        :param str sk_residuals_overview: key of overview of calculated residuals to store in data store
        :param str hist_dict_key: key of histograms dictionary to store in data store
        :param str pages_key: data store key of existing report pages (optional)
        :param str clientpages_key: data store key of existing report pages for client (optional)
        :param str results_path: path to save correlation summary pdf (optional)
        :param str prefix: prefix to add to file name (optional)
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'UncorrelationHypothesisTester'))

        # Process and register keyword arguments.  All arguments are popped from
        # kwargs and added as attributes of the link.  The values provided here
        # are defaults.
        self._process_kwargs(kwargs,
                             read_key=None,
                             read_key_vars='',
                             combinations=[],
                             columns=[],
                             x_columns=[],
                             y_columns=[],
                             inproduct=False,
                             from_ws=False,
                             map_to_original={},
                             var_number_of_bins={},
                             default_number_of_bins=10,
                             var_ignore_categories={},
                             ignore_categories=[],
                             nsims_per_significance=250,
                             significance_key='significance_matrix',
                             sk_significance_map='',
                             sk_residuals_map='',
                             sk_residuals_overview='',
                             results_path='',
                             prefix='',
                             z_threshold=3.,
                             pages_key='',
                             clientpages_key='',
                             hist_dict_key='',
                             verbose_plots=False)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)

        # initialize attributes
        self.pages = []
        self.clientpages = []
        self.hist_dict = OrderedDict()
        self.significance_map = {}
        self.residuals_map = {}
        self.mto = {}

    def initialize(self):
        """Initialize UncorrelationHypothesisTester"""

        # check input arguments
        self.check_arg_types(read_key=str, significance_key=str, sk_significance_map=str, sk_residuals_map=str,
                             sk_residuals_overview=str, default_number_of_bins=int, nsims_per_significance=int,
                             prefix=str,
                             z_threshold=float, pages_key=str, clientpages_key=str, hist_dict_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str)
        self.check_arg_types(recurse=True, allow_none=True, x_columns=str)
        self.check_arg_types(recurse=True, allow_none=True, y_columns=str)
        self.check_arg_types(recurse=True, allow_none=True, ignore_categories=str)
        self.check_arg_types(recurse=True, allow_none=True, var_ignore_categories=str)
        self.check_arg_vals('read_key')
        self.check_arg_vals('significance_key')

        if self.map_to_original and not isinstance(self.map_to_original, str) \
                and not isinstance(self.map_to_original, dict):
            raise TypeError('map_to_original needs to be a dict or string (to fetch a dict from the datastore)')

        # get I/O configuration
        io_conf = process_manager.service(ConfigObject).io_conf()

        # read report templates
        with open(persistence.io_path('templates', io_conf, 'df_summary_report.tex')) as templ_file:
            self.report_template = templ_file.read()
        with open(persistence.io_path('templates', io_conf, 'df_summary_report_page.tex')) as templ_file:
            self.page_template = templ_file.read()
        with open(persistence.io_path('templates', io_conf, 'df_summary_table_page.tex')) as templ_file:
            self.table_template = templ_file.read()

        # get path to results directory
        if not self.results_path:
            self.results_path = persistence.io_path('results_data', io_conf, 'report')
        if self.results_path and not self.results_path.endswith('/'):
            self.results_path = self.results_path + '/'

        # check if output directory exists
        if os.path.exists(self.results_path):
            # check if path is a directory
            if not os.path.isdir(self.results_path):
                self.log().critical('output path "%s" is not a directory', self.results_path)
                raise AssertionError('output path is not a directory')
        else:
            # create directory
            self.log().debug('Making output directory "%s"', self.results_path)
            os.makedirs(self.results_path)

        # prefix for file storage
        if self.prefix and not self.prefix.endswith('_'):
            self.prefix = self.prefix + '_'

        # check provided columns
        if len(self.columns):
            assert len(self.x_columns) == 0 and len(self.y_columns) == 0, \
                'Set either columns OR x_columns and y_columns.'
        if len(self.x_columns):
            assert len(self.columns) == 0 and len(self.y_columns) > 0, \
                'Set either columns OR x_columns and y_columns.'
        self._all_columns = []

        # check that var_ignore_categories are set correctly.
        for col, ic in self.var_ignore_categories.items():
            if isinstance(ic, str):
                self.var_ignore_categories[col] = [ic]
            elif not isinstance(ic, list):
                raise TypeError('var_ignore_categories key "%s" needs to be a string or list of strings' % col)

        # load roofit classes
        roofit_utils.load_libesroofit()

        return StatusCode.Success

    def execute(self):
        """Execute UncorrelationHypothesisTester"""

        proc_mgr = process_manager
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        # 1a. basic checks on contents of the roodataset
        if self.from_ws:
            ws = proc_mgr.service(RooFitManager).ws
            rds = ws.data(self.read_key)
            assert rds is not None, 'Key %s not in workspace' % self.read_key
        else:
            assert self.read_key in ds, 'key "%s" not found in datastore' % self.read_key
            rds = ds[self.read_key]
        if not isinstance(rds, ROOT.RooDataSet):
            raise TypeError('retrieved object "%s" not of type RooDataSet, but: %s' % (self.read_key, type(rds)))
        assert rds.numEntries() > 0, 'RooDataSet "%s" is empty' % self.read_key

        # 1b. retrieve read_key_vars rooargset from datastore
        if self.read_key_vars:
            assert isinstance(self.read_key_vars, str) and len(self.read_key_vars), \
                'read_key_vars should be a filled string'
            assert self.read_key_vars in ds, 'read_key_vars not in datastore'
            varset = ds[self.read_key_vars]
            assert isinstance(varset, ROOT.RooArgSet), 'read_key_vars is not a RooArgSet'
        else:
            # first record in dataset
            varset = rds.get(0)
        self._all_columns = [rv.GetName() for rv in varset]
        assert len(self._all_columns) >= 2, 'need at least two variables in roodataset %s.' % self.read_key

        # 1c. check provided columns
        #     match all columns/pattern in self.columns to _all_columns
        if isinstance(self.columns, bool):
            self.columns = self._all_columns if self.columns else []
        matched_columns = []
        for c in self.columns:
            match_c = fnmatch.filter(self._all_columns, c)
            if not match_c:
                raise AssertionError('column or pattern "%s" not present in roodataset' % (c, self.read_key))
            matched_columns += match_c
        self.columns = sorted(list(set(matched_columns)))  # sorted unique list

        # 1d. retrieve left and right pair columns (multiplied as left x right)
        matched_columns = []
        for c in self.x_columns:
            match_c = fnmatch.filter(self._all_columns, c)
            if not match_c:
                raise AssertionError('column or pattern "%s" not present in roodataset' % (c, self.read_key))
            matched_columns += match_c
        self.x_columns = sorted(list(set(matched_columns)))  # sorted unique list
        matched_columns = []
        for c in self.y_columns:
            match_c = fnmatch.filter(self._all_columns, c)
            if not match_c:
                raise AssertionError('column or pattern "%s" not present in roodataset' % (c, self.read_key))
            matched_columns += match_c
        self.y_columns = sorted(list(set(matched_columns)))  # sorted unique list
        self.y_columns = sorted([c for c in self.y_columns if c not in self.x_columns])

        # 1e. retrieve map_to_original from ds
        if self.map_to_original:
            if isinstance(self.map_to_original, str):
                assert len(self.map_to_original), 'map_to_original needs to be a filled string'
                assert self.map_to_original in ds, 'map_to_original key not found in datastore'
                mto = ds[self.map_to_original]
            elif isinstance(self.map_to_original, dict):
                mto = self.map_to_original
            assert isinstance(mto, dict), 'map_to_original needs to be a dict'
            # pandas replace() will not do transformations that are identical,
            # including int 0/1 to bool. skip those column-tranformations
            self.mto = copy.copy(mto)
            for c, c_mto in mto.items():
                k = list(c_mto.keys())
                v = list(c_mto.values())
                if set(k) & set(v):
                    # true in case of indentical transformation
                    self.log().debug('Identical transformation for column "%s". Skipping column', c)
                    del self.mto[c]

        # 1f. create report pages
        # data scientis report
        self.pages = []
        if self.pages_key:
            self.pages = ds.get(self.pages_key, [])
            assert isinstance(self.pages, list), 'Pages key %s does not refer to a list' % self.pages_key
        # client report
        self.clientpages = []
        if self.clientpages_key:
            self.clientpages = ds.get(self.clientpages_key, [])
            assert isinstance(self.clientpages,
                              list), 'Client pages key %s does not refer to a list' % self.clientpages_key

        # 1g. initialize significance_matrix
        nx = ny = 0
        x_cols = y_cols = []
        if len(self.columns):
            nx = len(self.columns)
            ny = len(self.columns)
            x_cols = self.columns
            y_cols = self.columns
        if len(self.x_columns) or len(self.y_columns):
            nx = len(self.x_columns)
            ny = len(self.y_columns)
            x_cols = self.x_columns
            y_cols = self.y_columns
        significance_matrix = np.zeros((ny, nx))
        symmetrize = True if self.columns else False
        n_bins = nx * ny if not symmetrize else nx * nx - nx
        n_unique = n_bins if not symmetrize else (nx * nx - nx) / 2

        # 2a. loop over unique column pairs and add to combinations
        for idx, c1 in enumerate(self.columns):
            for c2 in self.columns[idx + 1:]:
                self.combinations.append([c1, c2])
        # add left-right pair combinations
        if self.x_columns and self.inproduct:
            assert len(self.x_columns) == len(self.y_columns)
        for i, c1 in enumerate(self.x_columns):
            if self.inproduct:
                c2 = self.y_columns[i]
                self.combinations.append([c1, c2])
            else:
                for j, c2 in enumerate(self.y_columns):
                    self.combinations.append([c1, c2])

        # 2b. loop over all combinations: calculate significance and residuals
        n_combos = len(self.combinations)
        n_entries = rds.numEntries()
        for i_c, combo in enumerate(self.combinations):
            combo_name = ':'.join(combo)
            # make roodatahist for each combination
            obsset = ROOT.RooArgSet()
            for c in combo:
                obsset.add(varset.find(c))
            catCutStr = '1'
            for j, var in enumerate(obsset):
                if isinstance(var, ROOT.RooRealVar):
                    n_bins = self._n_bins(combo, j)
                    var.setBins(n_bins)
                elif isinstance(var, ROOT.RooCategory):
                    ignore_categories = self._ignore_categories(combo, j)
                    for ic in ignore_categories:
                        if not var.isValidLabel(ic):
                            continue
                        catCutStr += ' && (%s!=%s::%s)' % (var.GetName(), var.GetName(), ic)
            rdh = ROOT.RooDataHist(combo_name, combo_name, obsset)
            # remove specific categories (e.g. nan) if this has been requested so.
            red = rds.reduce(ROOT.RooFit.Cut(catCutStr))
            rdh.add(red)
            del red
            # rdh.add(rds)
            # a) calculate global significance of combo
            self.log().debug('Now processing combination (%d/%d): %s with %d bins and %d entries' %
                             (i_c + 1, n_combos, str(combo), rdh.numEntries(), rdh.sumEntries()))
            Zi = ROOT.Eskapade.ABCD.SignificanceOfUncorrelatedHypothesis(rdh, obsset, self.nsims_per_significance)
            self.significance_map[combo_name] = Zi
            if len(combo) == 2:
                x = x_cols.index(combo[0])
                y = y_cols.index(combo[1])
                if x < nx and y < ny:
                    significance_matrix[y, x] = Zi
                    if symmetrize:
                        significance_matrix[x, y] = Zi
            # b) calculate residuals
            success = ROOT.Eskapade.ABCD.checkInputData(rdh)
            self.log().debug(
                'Combination %s has significance: %f. Can calculate residuals? %s' % (str(combo), Zi, success))
            if not success:
                self.log().warning('Cannot calculate residuals for combination: %s. Skipping.' % str(combo))
                del rdh
                continue
            residi = ROOT.Eskapade.ABCD.GetNormalizedResiduals(rdh, obsset)
            dfri = data_conversion.rds_to_df(residi)
            del rdh
            del residi
            # do the mapping of roofit categories back to original format
            if self.mto:
                dfri.replace(self.mto, inplace=True)
            self.residuals_map[combo_name] = dfri

        # below, create report page for each variable in data frame
        # create resulting heatmaps and histograms

        # 1. make significance heatmap
        f_path = self.results_path + self.prefix + 'all_correlation_significance.pdf'
        var_label = 'Significance correlation matrix (s.d.)'
        vis_utils.plot_correlation_matrix(significance_matrix, x_cols, y_cols, f_path, var_label, -5, 5)
        stats = [('entries', n_entries), ('bins', n_bins), ('unique', n_unique),
                 ('> 0', (significance_matrix.ravel() > 0).sum()),
                 ('< 0', (significance_matrix.ravel() < 0).sum()),
                 ('avg', np.average(significance_matrix.ravel())),
                 ('max', max(significance_matrix.ravel())),
                 ('min', min(significance_matrix.ravel()))] if nx > 0 and ny > 0 else []
        stats_table = tabulate.tabulate(stats, tablefmt='latex')
        self.pages.append(self.page_template.replace('VAR_LABEL', var_label)
                          .replace('VAR_STATS_TABLE', stats_table)
                          .replace('VAR_HISTOGRAM_PATH', f_path))
        significance = self.significance_map.copy()
        for key in list(significance.keys()):
            significance[key] = [significance[key]]
        dfsignificance = pd.DataFrame(significance).stack().reset_index(level=1) \
            .rename(columns={'level_1': 'Questions', 0: 'Significance'}) \
            .sort_values(by='Significance', ascending=False)
        keep_cols = ['Questions', 'Significance']
        table = latex_residuals_table(dfsignificance, keep_cols, self.z_threshold, normResidCol='Significance')
        if table:
            self.clientpages.append(
                self.table_template.replace('VAR_LABEL', 'Significance').replace('VAR_STATS_TABLE', table))

        # 2a. create one residual table containing the top non-noncorrelating answers
        resid_all = []
        if len(self.combinations) > 1:
            # create one dataframe containing all data
            resid_list = []
            ndim_max = 2
            for key in list(self.residuals_map.keys()):
                if abs(self.significance_map[key]) < self.z_threshold:
                    continue
                dftmp = self.residuals_map[key].copy()
                resid_list.append(self._format_df(dftmp, key))
                if len(key.split(':')) > ndim_max:
                    ndim_max = len(key.split(':'))
            # convert top residuals into latex table
            if len(resid_list) >= 1:
                resid_all = resid_list[0]
                if len(resid_list) > 1:
                    resid_all = resid_list[0].append(resid_list[1:], ignore_index=True)
                resid_all = resid_all.reindex(resid_all.normResid.abs().sort_values(ascending=False).index)
                keep_cols = ['question_%d' % i for i in range(ndim_max)] + \
                            ['answer_%d' % i for i in range(ndim_max)] + \
                            ['num_entries', 'abcd', 'abcd_error', 'pValue', 'normResid']
                table = latex_residuals_table(resid_all, keep_cols, self.z_threshold)
                self.pages.append(
                    self.table_template.replace('VAR_LABEL', 'Most significant outliers').replace('VAR_STATS_TABLE',
                                                                                                  table))
                keep_cols = ['question_%d' % i for i in range(ndim_max)] + \
                            ['answer_%d' % i for i in range(ndim_max)] + \
                            ['num_entries', 'abcd', 'normResid']
                table = latex_residuals_table(resid_all, keep_cols, self.z_threshold)
                self.clientpages.append(
                    self.table_template.replace('VAR_LABEL', 'Most significant outliers').replace('VAR_STATS_TABLE',
                                                                                                  table))

        # 2b. make residuals heatmaps
        for combo in self.combinations:
            if len(combo) != 2:
                continue
            combo_name = ':'.join(combo)
            residi = self.residuals_map[combo_name]
            mat_normresiduals, x_vals, y_vals = extract_matrix(residi, combo[0], combo[1])
            mat_observed, x_vals, y_vals = extract_matrix(residi, combo[0], combo[1], 'num_entries')
            f_path = self.results_path + self.prefix + 'normalized_residuals_heatmap_' + '_'.join(combo) + '.pdf'
            vis_utils.plot_correlation_matrix(mat_normresiduals, x_vals, y_vals, f_path, 'significance relation', -5, 5,
                                              x_label=combo[0], y_label=combo[1],
                                              matrix_numbers=mat_observed,
                                              print_both_numbers=self.verbose_plots)
            stats = [('entries', residi['num_entries'].sum()), ('bins', len(residi.index)),
                     ('> 0', (residi['normResid'] > 0).sum()),
                     ('< 0', (residi['normResid'] < 0).sum()),
                     ('avg', residi['normResid'].mean()),
                     ('max', residi['normResid'].max()),
                     ('min', residi['normResid'].min())]
            stats_table = tabulate.tabulate(stats, tablefmt='latex')
            self.pages.append(self.page_template.replace('VAR_LABEL', 'relation (abcd): ' + ' vs '.join(combo))
                              .replace('VAR_STATS_TABLE', stats_table)
                              .replace('VAR_HISTOGRAM_PATH', f_path))

        # 2c. make residuals tables
        for combo in self.combinations:
            combo_name = ':'.join(combo)
            residi = self.residuals_map[combo_name]
            keep_cols = combo + ['num_entries', 'abcd', 'abcd_error', 'pValue', 'normResid']
            table = latex_residuals_table(residi, keep_cols, self.z_threshold)
            if not table:
                continue
            self.pages.append(
                self.table_template.replace('VAR_LABEL', 'outliers: ' + ' vs '.join(combo)).replace('VAR_STATS_TABLE',
                                                                                                    table))

        # 2d. make residuals histograms
        p_all = ROOT.TH1F('p_all', 'p_all', 20, 0, 1)
        z_all = ROOT.TH1F('z_all', 'z_all', 50, -10, 10)
        for combo in self.combinations:
            combo_name = ':'.join(combo)
            residi = self.residuals_map[combo_name]
            root_numpy.fill_hist(p_all, residi['pValue'].values)
            root_numpy.fill_hist(z_all, residi['normResid'].values)
            p_i = ROOT.TH1F('p_' + combo_name, 'p_' + combo_name, 20, 0, 1)
            z_i = ROOT.TH1F('z_' + combo_name, 'z_' + combo_name, 40, -8, 8)
            root_numpy.fill_hist(p_i, residi['pValue'].values)
            root_numpy.fill_hist(z_i, residi['normResid'].values)
            self.hist_dict['normalized residuals: ' + ' vs '.join(combo)] = z_i
            self.hist_dict['p-values: ' + ' vs '.join(combo)] = p_i
        self.hist_dict['all normalized residuals'] = z_all
        self.hist_dict['all p-values'] = p_all

        # 3. storage
        if self.hist_dict_key:
            ds[self.hist_dict_key] = self.hist_dict
        if self.pages_key:
            ds[self.pages_key] = self.pages
        if self.sk_significance_map:
            ds[self.sk_significance_map] = self.significance_map
            self.log().debug('Stored significance map in data store under key: %s' % self.sk_significance_map)
        if self.sk_residuals_map:
            ds[self.sk_residuals_map] = self.residuals_map
            self.log().debug('Stored residuals map in data store under key: %s' % self.sk_residuals_map)
        if self.sk_residuals_overview and len(resid_all) > 0:
            ds[self.sk_residuals_overview] = resid_all
            self.log().debug('Stored residuals list in data store under key: %s' % self.sk_residuals_overview)

        return StatusCode.Success

    def finalize(self):
        """Finalize UncorrelationHypothesisTester"""

        # write report file
        with open('{}/report.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(
                self.report_template.replace(
                    'INPUT_PAGES', ''.join(
                        self.pages)))

        # write client report file
        with open('{}/report_client.tex'.format(self.results_path), 'w') as report_file:
            report_file.write(
                self.report_template.replace(
                    'INPUT_PAGES', ''.join(
                        self.clientpages)))

        return StatusCode.Success

    def _n_bins(self, c, idx=0):
        """ determine number of bins for continues variables

        :param list c: list of variables, or string variable
        :param int idx: index of the variable in c for which to return number of bins
        :return: number of bins
        """
        if isinstance(c, str):
            c = [c]
        n = ':'.join(c)
        if len(c) > 1 and n in self.var_number_of_bins and len(self.var_number_of_bins[n]) == len(c):
            return self.var_number_of_bins[n][idx]
        elif c[idx] in self.var_number_of_bins:
            return self.var_number_of_bins[c[idx]]
        # fall back on defaults
        return self.default_number_of_bins

    def _ignore_categories(self, c, idx=0):
        """ determine list of categories to ignore

        :param list c: list of variables, or string variable
        :param int idx: index of the variable in c, for which to return categories to ignore
        :return: list of categories to ignore
        """

        if isinstance(c, str):
            c = [c]
        n = ':'.join(c)
        if len(c) > 1 and n in self.var_ignore_categories and len(self.var_ignore_categories[n]) == len(c):
            i_c = self.var_ignore_categories[n][idx]
        elif c[idx] in self.var_ignore_categories:
            i_c = self.var_ignore_categories[c[idx]]
        else:
            # fall back on defaults
            i_c = self.ignore_categories
        if not isinstance(i_c, list):
            i_c = [i_c]
        return i_c

    def _format_df(self, df, key):
        """ bring dataframe in format for overview table

        :param pandas.dataframe df: dataframe to be reformatted
        :param string key: column names of observables
        :return: reformatted dataframe
        """

        # rename answer-columns
        questions = key.split(':')
        answers = ['answer_%d' % i for i in range(len(questions))]
        df.rename(columns=dict(list(zip(questions, answers))), inplace=True)
        # add separate columns with question name
        for i, q in enumerate(questions):
            df.insert(i, 'question_%d' % i, [q] * len(df))
        df['combo'] = key
        return df


@jit(cache=True)
def extract_matrix(df, x_col, y_col, v_col='normResid'):
    """ Extract matrix from dataframe

    :param pd.DataFrame df: dataframe from which to extract matrix
    :param str x_col: column name first observable
    :param str y_col: column name second observable
    :param str v_col: column name values
    :return: matrix, categories of first observable, categories of second observable
    """

    # basic checks
    assert isinstance(df, pd.DataFrame), 'df needs to be a pandas data frame.'
    assert len(df.index) > 0, 'df needs to be a filled data frame.'
    assert isinstance(x_col, str) and len(x_col), 'x_col needs to be a filled string.'
    assert isinstance(y_col, str) and len(y_col), 'y_col needs to be a filled string.'
    assert isinstance(v_col, str) and len(v_col), 'v_col needs to be a filled string.'
    assert x_col in df.columns, '%s not a column of provided data frame.' % x_col
    assert y_col in df.columns, '%s not a column of provided data frame.' % y_col
    assert v_col in df.columns, '%s not a column of provided data frame.' % v_col

    x_vals = sorted(df[x_col].unique().tolist())
    y_vals = sorted(df[y_col].unique().tolist())
    nx = len(x_vals)
    ny = len(y_vals)
    assert len(df.index) == nx * ny, 'Residuals dataframe for combination %s has incorrect length.'
    matrix = np.zeros((ny, nx))

    for i, x in enumerate(x_vals):
        for j, y in enumerate(y_vals):
            keep = (df[x_col] == x) & (df[y_col] == y)
            assert len(df[keep].index) == 1, 'Cannot selected more than one row to fill matrix: %d' % len(
                df[keep].index)
            val = df[keep][v_col].values[0]
            matrix[j][i] = val

    return matrix, x_vals, y_vals


def latex_residuals_table(df, keep_cols=[], absZ_threshold=3., n_rows=20, normResidCol='normResid'):
    """ Create Latex table from dataframe

    Create Latex table from dataframe. Options are available to select columns and rows.

    :param pandas.DataFrame df: pandas dataframe from which latex table will be created
    :param list keep_cols: selection on columns. List of columns for which table will be created (optional)
    :param float absZ_threshold: selection on rows. Create only if value in normResidCol >= threshold (optional)
    :param int n_nrows: maximum number of rows (optional)
    :param str normResidCol: latex table is sorted according to values in this column. Also the threshold
                             is applied to this column (optional)
    :return: table in string format
    """

    # basic checks
    assert isinstance(df, pd.DataFrame), 'df needs to be a pandas data frame.'
    assert normResidCol in df.columns, 'Column %s not present in data frame' % normResidCol
    if not keep_cols:
        keep_cols = df.columns

    # make selection
    df_ = df.copy(deep=False)
    df_['absZ'] = abs(df_[normResidCol])
    df_.sort_values(by=['absZ'], ascending=False, inplace=True)
    sel = df_[df_.absZ >= absZ_threshold][keep_cols][:n_rows]
    if len(sel.index) == 0:
        return None

    # convert selection to latex str
    import io
    f = io.StringIO()
    sel.to_latex(f, index=False)
    table_str = f.getvalue()
    f.close()
    return table_str
