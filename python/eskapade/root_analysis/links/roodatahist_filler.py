"""Project: Eskapade - A python-based package for data analysis.

Class: RooDataHistFiller

Created: 2017/03/25

Description:
    Algorithm to fill a RooDataHist with columns from a DataFrame

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import math

import ROOT
import numpy as np
import pandas as pd

try:
    from ROOT import RooFit
except ImportError:
    import ROOT.RooFit as RooFit

from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.root_analysis import data_conversion
from eskapade.root_analysis.roofit_manager import RooFitManager

N_BINS_DEFAULT = 40


class RooDataHistFiller(Link):
    """Fill a RooFit histogram with columns from a Pandas dataframe.

    Histograms can have any number of dimensions. Only numeric observables
    are picked up.  By default all observables are interpreted as continuous
    (not category), except for boolean and numpy category variables.  Other
    category observable first need to be converted ('factorized') to
    numberic values, eg. using the link record_factorizer.  These other
    category variables can be picked up by setting: map_to_factorized, which
    is dictiorary to map columns to factorized ones. map_to_factorized is a
    dict of dicts, ie. one dict for each column.

    RooDataHistFiller stores a roodatahist object, a rooargset containing
    all corresponding roofit observables and roocategories, and a rooargset
    containing only the observables that are roocategories.  Also stored,
    under key sk_map_to_original, is a dictiorary to map all factorized
    columns back to original.

    For each observable one can set the number of bins, and min and max
    values.  The total number of bins in the roodatahist may not exceed
    n_max_total_bins.

    The roodatahist histogram can be filled iteratively, while looping over
    multiple dataframes.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param list columns: list of columns to pick up from dataset. Default is all columns. (optional)
        :param list ignore_columns: list of columns to ignore from dataset. (optional)
        :param str store_key: key of output roodataset to store in data store. (optional)
        :param str store_key_vars: key of output rooargset of all observables to store in data store. (optional)
        :param str store_key_cats: key of output rooargset of category observables to store in data store. (optional)
        :param bool store_at_finalize: if true, store in workspace at finalize(), not at execute(). (optional)
        :param bool into_ws: if true, store in workspace, not datastore. Default is False. (optional)
        :param bool rm_original: if true, remove original dataframe. Default is False. (optional)
        :param dict map_to_factorized: dictiorary or key to dictionary to map columns to factorized ones.
                                       map_to_factorized is a dict of dicts, one dict for each column. (optional)
        :param str sk_map_to_original: store key of dictiorary to map factorized columns to original.
                                       Default is 'key' + '_' + store_key + '_to_original'. (optional)
        :param dict var_number_of_bins: number of bins for histogram of certain variable (optional)
        :param dict var_min_value: min value for histogram of certain variable (optional)
        :param dict var_max_value: max value for histogram of certain variable (optional)
        :param int n_max_total_bins: max number of bins in roodatahist. Default is 1e6. (optional)
        :param str create_hist_pdf: if filled, create hist pdf from rdh with this name and
                                    add to datastore or workspace. (optional)
        :param bool create_new_rdh_in_loop: if true, create a new rdh when running in a loop. (optional)
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'RooDataHistFiller'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             columns=[],
                             ignore_columns=[],
                             store_key='',
                             store_key_vars='',
                             store_key_cats='',
                             store_at_finalize=False,
                             into_ws=False,
                             rm_original=False,
                             map_to_factorized={},
                             sk_map_to_original='',
                             var_number_of_bins={},
                             var_min_value={},
                             var_max_value={},
                             n_max_total_bins=1e6,
                             create_hist_pdf='',
                             create_new_rdh_in_loop=False)

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

        # internal roodatahist and its variables
        self._rdh = None
        self._varset = None
        self._catset = None
        # dict mapping category observables back to original string values
        self._mto = {}

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, store_key=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str)
        self.check_arg_vals('read_key')

        if not self.store_key:
            self.store_key = 'rdh_' + self.read_key.replace('df_', '')
        if not self.store_key_vars:
            self.store_key_vars = self.read_key.replace('df_', '') + '_varset'
        if not self.store_key_cats:
            self.store_key_cats = self.read_key.replace('df_', '') + '_catset'
        if not self.sk_map_to_original:
            self.sk_map_to_original = 'map_' + self.store_key + '_to_original'
            self.logger.info('Storage key "sk_map_to_original" has been set to "{key}".', key=self.sk_map_to_original)

        if not self.map_to_factorized:
            assert isinstance(self.map_to_factorized, (str, dict)), \
                'map_to_factorized needs to be a dict or string (to fetch a dict from the datastore).'

        assert self.n_max_total_bins >= 1, 'Max total number of bins in histogram needs to be greater than one.'

        if self.create_hist_pdf:
            assert isinstance(self.create_hist_pdf, str) and self.create_hist_pdf, \
                'create_hist_pdf needs to be a filled string'

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        Fill a roodatahist object with a pandas dataframe.  It it possible to
        fill the roodatahist iteratively, in a loop over dataframes.

        There are 5 steps to the code:

        1. basic checks of the dataframe
        2. convert the dataframe to a roodataset
        3. instantiate a roodatahist object
        4. fill the roodatahist object with the roodataset
        5. store the roodatahist.
           optionally, at the storage stage a pdf can be created of the roodatahist as well.
        """
        ds = process_manager.service(DataStore)

        # 1a. basic checks on contensts of the data frame
        assert self.read_key in ds, 'key "{key}" not in DataStore'.format(key=self.read_key)
        df = ds[self.read_key]
        if not isinstance(df, pd.DataFrame):
            raise RuntimeError('retrieved object "{}" not of type pandas DataFrame'.format(self.read_key))
        assert len(df.index) > 0, 'dataframe "{}" is empty'.format(self.read_key)

        # 1b. retrieve map_to_factorized from ds if it's a string
        if self.map_to_factorized:
            if isinstance(self.map_to_factorized, str):
                assert self.map_to_factorized, 'map_to_factorized needs to be a filled string'
                assert self.map_to_factorized in ds, 'map_to_factorized key "{}" not found in datastore'
                self.map_to_factorized = ds[self.map_to_factorized]
            assert isinstance(self.map_to_factorized, dict), 'map_to_factorized needs to be a dict'

        # 1c. varset, if already set, overrules provided columns
        if self._varset:
            assert isinstance(self._varset, ROOT.RooArgSet), 'varset is not a rooargset.'
            self.columns = [rv.GetName() for rv in self._varset]

        # 1d. check all columns
        if not self.columns:
            self.columns = df.columns.tolist()
        for col in self.columns[:]:
            assert col in df.columns, 'Column "{}" not in dataframe "{}".'.format(col, self.read_key)
            dt = df[col].dtype.type
            # keep categorical observables -- convert these to roocategories in conversion to tree
            if pd.core.common.is_categorical(dt):
                continue
            # reject all string-based columns
            if (dt is np.string_) or (dt is np.object_):
                self.logger.warning('Skipping string-based column "{col}".', col=col)
                self.columns.remove(col)
            if col in self.ignore_columns:
                self.columns.remove(col)
        self.logger.debug('Picking up columns: {cols}.', cols=self.columns)

        # 2. do conversion of df to roodataset, pass this to roodatahist below.
        #    self.map_to_factorized are categorical variables to be turned into roocategories
        rds, obs, _, map_to_original = data_conversion.df_to_rds(df[self.columns],
                                                                 rf_varset=self._varset,
                                                                 category_vars=self.map_to_factorized,
                                                                 name=self.read_key)

        # 3a. determine max number of bin for continuous observables
        #     (do this at first iteration only.)
        n_max_bins = int(self.n_max_total_bins)
        if not self._varset:
            n_total_bins_in_categories = 1
            for mto in map_to_original.values():
                n_total_bins_in_categories *= len(mto)
            n_total_bins_in_vars = self.n_max_total_bins / n_total_bins_in_categories
            n_vars = len(self.columns) - len(map_to_original)
            assert n_total_bins_in_vars >= 0, 'Total number of bins in vars is negative.'
            assert n_vars >= 0, 'Number of roorealvars is negative.'
            if n_vars >= 1:
                n_max_bins = int(math.pow(n_total_bins_in_vars, 1 / n_vars))
                if n_max_bins < 1:
                    n_max_bins = 1
                elif n_max_bins > int(self.n_max_total_bins):
                    n_max_bins = int(self.n_max_total_bins)
                self.logger.debug('Max number of variable bins set to: {n:d}', n=n_max_bins)

        # 3b. fix the roofit variable set
        if not self._varset:
            self._varset = obs
            self._catset = ROOT.RooArgSet()
            # update variable range and number of binsxs
            for rv in self._varset:
                if isinstance(rv, ROOT.RooCategory):
                    self._catset.add(rv)
                    continue
                if not isinstance(rv, ROOT.RooRealVar):
                    continue
                name = rv.GetName()
                n_bins = self.var_number_of_bins.get(name, N_BINS_DEFAULT)
                if n_bins > n_max_bins:
                    n_bins = n_max_bins
                    self.logger.info('Capping n_bins of column "{col}" to: {n:d}', col=name, n=n_max_bins)
                rv.setBins(n_bins)
                if name in self.var_min_value:
                    min_val = self.var_min_value[name]
                    rv.setMin(min_val)
                if name in self.var_max_value:
                    max_val = self.var_max_value[name]
                    rv.setMax(max_val)
        else:
            assert isinstance(self._varset, ROOT.RooArgSet) and len(self._varset), 'varset is not a filled rooargset'
        # 3c. instantiate roodatahist, to be filled up below.
        if self.create_new_rdh_in_loop:
            if self._rdh:
                del self._rdh
            self._rdh = None
        if not self._rdh:
            name = str(rds.GetName()).replace('rds_', 'rdh_')
            self._rdh = ROOT.RooDataHist(name, name, self._varset)
        else:
            assert isinstance(self._rdh, ROOT.RooDataHist)

        # 4. fill the roodatahist with the roodataset
        try:
            self._rdh.add(rds)
            del rds
            if not self._mto:
                self._mto.update(map_to_original)
        except Exception as exc:
            self.logger.fatal('Could not fill roodatahist object with roodataset.')
            raise exc

        # 5. storage of roodatahist and its variables
        if not self.store_at_finalize:
            self.do_storage()

        return StatusCode.Success

    def finalize(self):
        """Finalize the link."""
        if self.store_at_finalize:
            self.do_storage()

        return StatusCode.Success

    def do_storage(self):
        """Storage of the created RooDataHist object."""
        ds = process_manager.service(DataStore)

        # 1. create pdf of dataset as well?
        if self.create_hist_pdf:
            hpdf_name = self.create_hist_pdf
            hist_pdf = ROOT.RooHistPdf(hpdf_name, hpdf_name, self._varset, self._rdh)

        # 2. remove original df?
        if self.rm_original:
            del ds[self.read_key]

        # 3a. put objects from the datastore into the workspace
        if self.into_ws:
            ws = process_manager.service(RooFitManager).ws
            try:
                ws.put(self._rdh, ROOT.RooFit.Rename(self.store_key))
                ws.defineSet(self.store_key_vars, self._varset)
                ws.defineSet(self.store_key_cats, self._catset)
                if self.create_hist_pdf:
                    ws.put(hist_pdf, RooFit.RecycleConflictNodes())
            except Exception:
                raise RuntimeError('Could not import object "{}" into rooworkspace.'.format(self.read_key))
        # 3b. put objects into datastore
        else:
            ds[self.store_key] = self._rdh
            ds[self.store_key_vars] = self._varset
            ds[self.store_key_cats] = self._catset
            if self.create_hist_pdf:
                ds[hpdf_name] = hist_pdf

        n_rdh = int(self._rdh.sumEntries())
        ds['n_' + self.store_key] = n_rdh
        self.logger.debug('Stored roodatahist "{key}" with sum of weights: {sum:d}', key=self.store_key, sum=n_rdh)
        ds[self.sk_map_to_original] = self._mto
