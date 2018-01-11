"""Project: Eskapade - A python-based package for data analysis.

Class: ConvertRootHist2RooDataSet

Created: 2017/03/25

Description:
    Algorithm to convert a root histogram into a roodataset (= roofit data)

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ROOT

from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.root_analysis import data_conversion
from eskapade.root_analysis.roofit_manager import RooFitManager


class ConvertRootHist2RooDataSet(Link):
    """Convert a ROOT histogram into a RooFit dataset.

    Input histograms can have up to three dimensions. RooFit observables are
    deduced from the histogram axes.  By default all observables are
    interpreted as continuous.

    ConvertRootHist2RooDataSet stores a roodataset object, a rooargset
    containing all corresponding roofit observables.  Optionally, a keys-pdf
    is created from the roodataset object and stored as well.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: histogram to pick up from datastore (or, if set, from histogram dict)
        :param str hist_dict_key: histograms dictionary from data store.
                                  If set, histogram is read from this dict (optional)
        :param str store_key: key of roodatahist (optional)
        :param str store_key_vars: key of output rooargset of observables to store in data store (optional)
        :param bool into_ws: if true, store in workspace, not datastore. Default is True.
        :param bool rm_original: if true, remove original histogram. Default is False.
        :param str create_keys_pdf: if set, create keys pdf from rds with this name and add to ds or workspace
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ConvertRootHist2RooDataSet'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             hist_dict_key='',
                             store_key='',
                             store_key_vars='',
                             into_ws=False,
                             rm_original=False,
                             create_keys_pdf='')

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, hist_dict_key=str, store_key=str)
        self.check_arg_vals('read_key')

        if not self.store_key:
            self.store_key = 'rds_' + self.read_key.replace(':', '_vs_')
        if not self.store_key_vars:
            self.store_key_vars = 'vars_' + self.read_key.replace(':', '_vs_')

        if self.create_keys_pdf:
            assert isinstance(self.create_keys_pdf, str) and len(self.create_keys_pdf), \
                'create_keys_pdf needs to be a filled string.'

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        # basic checks on contents of the root histogram
        if not self.hist_dict_key:
            assert self.read_key in ds, 'Key "{}" not in DataStore.'.format(self.read_key)
            hist = ds[self.read_key]
        else:
            assert self.hist_dict_key in ds, 'Key "{}" not in DataStore.'.format(self.hist_dict_key)
            hist_dict = ds[self.hist_dict_key]
            assert self.read_key in hist_dict, \
                'Key "{}" not in histogram dictionary "{}".'.format(self.read_key, self.hist_dict_key)
            hist = hist_dict[self.read_key]
        if not isinstance(hist, ROOT.TH1):
            raise TypeError('Retrieved object "{}" not a ROOT histogram.'.format(self.read_key))

        # retrieve observable names from axes titles
        columns = []
        for i in range(hist.n_dim):
            if i == 0:
                col = hist.GetXaxis().GetName()
            if i == 1:
                col = hist.GetYaxis().GetName()
            if i == 2:
                col = hist.GetZaxis().GetName()
            col = col.strip().replace('axis', '').replace(' ', '_')
            assert len(col), 'Could not retrieve valid column name from axis {:d} its name.'.format(i)
            columns.append(col)

        # do conversion
        obs = ':'.join(columns)
        rds, obs_vars = data_conversion.hist_to_rds(hist, obs)

        # create pdf of dataset as well?
        if self.create_keys_pdf:
            obs_list = ROOT.RooArgList(obs_vars)
            keys_name = self.create_keys_pdf
            keys_pdf = ROOT.RooNDKeysPdf(keys_name, keys_name, obs_list, rds, 'ma')

        # 0. remove original histogram?
        if self.rm_original:
            if not self.hist_dict_key:
                del ds[self.read_key]
            else:
                del hist_dict[self.read_key]

        # 1. put object into the workspace
        if self.into_ws:
            try:
                ws = process_manager.service(RooFitManager).ws
                ws.put(rds, ROOT.RooFit.Rename(self.store_key))
                ws.defineSet(self.store_key_vars, obs_vars)
            except Exception:
                raise RuntimeError('Could not import object "{}" into rooworkspace.'.format(self.read_key))
        # 2. put object into datastore
        else:
            ds[self.store_key] = rds
            ds[self.store_key_vars] = obs_vars

        # workspace doesn't like keys pdf, so always keep in ds
        if self.create_keys_pdf:
            ds[keys_name] = keys_pdf
        n_rds = rds.numEntries()
        ds['n_' + self.store_key] = n_rds
        self.logger.debug('Stored roodatahist "{key}" with length: {length:d}.', key=self.store_key, length=n_rds)

        return StatusCode.Success
