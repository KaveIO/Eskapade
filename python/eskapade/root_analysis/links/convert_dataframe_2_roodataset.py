"""Project: Eskapade - A python-based package for data analysis.

Class: ConvertDataFrame2RooDataSet

Created: 2017/03/25

Description:
    Algorithm to convert a pandas dataframe into a roodataset

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import fnmatch

import ROOT
import numpy as np
import pandas as pd

from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.root_analysis import data_conversion
from eskapade.root_analysis.roofit_manager import RooFitManager


class ConvertDataFrame2RooDataSet(Link):
    """Convert Pandas dataframe into a RooFit dataset.

    By default all observables of the dataframe are interpreted as
    continuous (not category), except for boolean and numpy category
    variables.  Other category observable first need to be converted
    ('factorized') to numberic values, eg. using the link record_factorizer.
    These other category variables can be picked up by setting:
    map_to_factorized, which is dictiorary to map columns to factorized
    ones. map_to_factorized is a dict of dicts, ie. one dict for each
    column.

    RooDataHistFiller stores a roodatahist object, a rooargset containing
    all corresponding roofit observables and roocategories.  Also stored,
    under key sk_map_to_original, is a dictiorary to map all factorized
    columns back to original.

    For each observable one can set the number of bins, and min and max
    values.  The total number of bins in the roodatahist may not exceed
    n_max_total_bins.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str read_key_vars: key of input rooargset of observables from data store (optional)
        :param list columns: list of columns to pick up from dataset. Default is all columns. (optional)
        :param list ignore_columns: list of columns to ignore from dataset. (optional)
        :param str store_key: key of output roodataset to store in data store (optional)
        :param str store_key_vars: key of output rooargset of observables to store in data store. (optional)
        :param bool into_ws: if true, store in workspace, not datastore. Default is True
        :param bool rm_original: if true, remove original histogram. Default is False
        :param dict map_to_factorized: dictiorary or key to dictionary to map back columns to factorized ones.
                                       map_to_factorized is a dict of dicts, one dict for each column. (optional)
        :param str sk_map_to_original: store key of dictiorary to map factorized columns to original.
                                       Default is 'key' + '_' + store_key + '_to_original'. (optional)
        :param int n_max_total_bins: max number of bins in roodatahist. Default is 1e6.
        :param bool store_index: If true, copy df's index to rds. Default is true.
        :param str create_keys_pdf: if set, create keys pdf from rds with this name and add
                                    to ds or workspace (optional)
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ConvertDataFrame2RooDataSet'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             read_key_vars='',
                             columns=[],
                             ignore_columns=[],
                             store_key='',
                             store_key_vars='',
                             into_ws=False,
                             rm_original=False,
                             map_to_factorized={},
                             sk_map_to_original='',
                             n_max_total_bins=1e6,
                             store_index=True,
                             create_keys_pdf='')

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

        self._varset = None

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, store_key=str, store_key_vars=str)
        self.check_arg_types(recurse=True, allow_none=True, columns=str)
        self.check_arg_vals('read_key')

        if not self.store_key:
            self.store_key = 'rds_' + self.read_key.replace('df_', '')
        if not self.store_key_vars:
            self.store_key_vars = self.read_key.replace('df_', '') + '_varset'
        if not self.sk_map_to_original:
            self.sk_map_to_original = 'map_' + self.store_key + '_to_original'
            self.logger.debug('Storage key "sk_map_to_original" has been set to "{key}".', key=self.sk_map_to_original)

        if not self.map_to_factorized and not isinstance(self.map_to_factorized, (str, dict)):
            raise TypeError('map_to_factorized needs to be a dict or string (to fetch a dict from the datastore).')

        if self.n_max_total_bins < 1:
            raise RuntimeError('Max total number of bins in histogram needs to be greater than one.')

        if self.create_keys_pdf and not isinstance(self.create_keys_pdf, str):
            raise TypeError('create_hist_pdf needs to be a filled string.')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # 1a. basic checks on contensts of the data frame
        assert self.read_key in ds, 'Key "{key}" not in DataStore'.format(key=self.read_key)
        df = ds[self.read_key]
        if not isinstance(df, pd.DataFrame):
            raise TypeError('Retrieved object "{}" not of type pandas DataFrame.'.format(self.read_key))
        assert len(df.index) > 0, 'dataframe "{}" is empty'.format(self.read_key)

        # 1b. retrieve map_to_factorized from ds if it's a string
        if self.map_to_factorized:
            if isinstance(self.map_to_factorized, str):
                assert self.map_to_factorized, 'map_to_factorized needs to be a filled string.'
                assert self.map_to_factorized in ds, \
                    'map_to_factorized key "{}" not found in datastore.'.format(self.map_to_factorized)
                self.map_to_factorized = ds[self.map_to_factorized]
            assert isinstance(self.map_to_factorized, dict), 'map_to_factorized needs to be a dict.'

        # 1c. retrieve read_key_vars rooargset from datastore
        if self.read_key_vars:
            assert isinstance(self.read_key_vars, str) and self.read_key_vars, \
                'read_key_vars should be a filled string.'
            assert self.read_key_vars in ds, 'read_key_vars not in datastore.'
            varset = ds[self.read_key_vars]
            assert isinstance(varset, ROOT.RooArgSet), 'read_key_vars is not a RooArgSet.'
            self._varset = varset
        if self._varset:
            # varset overrules provided columns
            self.columns = [rv.GetName() for rv in self._varset]

        # 1d. check all columns
        if not self.columns:
            self.columns = df.columns.tolist()
        # match all columns/pattern in self.columns to df.columns
        matched_columns = []
        for c in self.columns:
            match_c = fnmatch.filter(df.columns, c)
            if not match_c:
                raise AssertionError('Column or pattern "{}" not in data frame {}.'.format(c, self.read_key))
            matched_columns += match_c
        self.columns = matched_columns
        for col in self.columns[:]:
            dt = df[col].dtype.type
            # keep categorical observables -- convert these to roocategories in conversion
            if pd.core.common.is_categorical(dt):
                continue
            # reject all string-based columns
            if (dt is np.string_) or (dt is np.object_):
                self.logger.warning('Skipping string-based column "{col}".', col=col)
                self.columns.remove(col)
            if col in self.ignore_columns:
                self.columns.remove(col)
        self.logger.debug('Picking up columns: {cols}.', cols=self.columns)

        # 2. do conversion of df to roodataset
        #    self.map_to_factorized are categorical variables to be turned into roocategories
        rds, obs_vars, _, map_to_original = data_conversion.df_to_rds(df[self.columns],
                                                                      rf_varset=self._varset,
                                                                      category_vars=self.map_to_factorized,
                                                                      name=self.read_key,
                                                                      store_index=self.store_index)

        # 3a. remove original df?
        if self.rm_original:
            del ds[self.read_key]

        # 3b. put objects from the datastore into the workspace
        if self.into_ws:
            try:
                ws.put(rds, ROOT.RooFit.Rename(self.store_key))
                ws.defineSet(self.store_key_vars, obs_vars)
            except Exception:
                raise RuntimeError('Could not import object "{}" into rooworkspace.'.format(self.read_key))
        # 3c. put objects into datastore
        else:
            ds[self.store_key_vars] = obs_vars
            ds[self.store_key] = rds

        # create pdf of dataset as well?
        if self.create_keys_pdf:
            if self.into_ws:
                # retrieve for consistency
                obs_vars = ws.set(self.store_key_vars)
            obs_list = ROOT.RooArgList(obs_vars)
            keys_name = self.create_keys_pdf
            keys_pdf = ROOT.RooNDKeysPdf(keys_name, keys_name, obs_list, rds, 'ma')
            ds[keys_name] = keys_pdf

        # 3e.
        ds[self.sk_map_to_original] = map_to_original
        n_rds = rds.numEntries()
        ds['n_' + self.store_key] = n_rds
        self.logger.debug('Stored roodataset "{key}" with length: {length:d}', key=self.store_key, length=n_rds)

        return StatusCode.Success
