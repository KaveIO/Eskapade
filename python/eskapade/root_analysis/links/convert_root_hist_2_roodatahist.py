# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : ConvertRootHist2RooDataHist                                           *
# * Created: 2017/03/25                                                            *
# * Description:                                                                   *
# *      Algorithm to convert a root histogram into a roodatahist (= roofit hist)  *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import pandas as pd
import numpy as np

import ROOT

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade.root_analysis import RooFitManager, data_conversion


class ConvertRootHist2RooDataHist(Link):
    """Convert a ROOT histogram into a RooFit histogram

    Input histograms can have up to three dimensions. RooFit observables are
    deduced from the histogram axes.  By default all observables are
    interpreted as continuous.

    ConvertRootHist2RooDataHist stores a roodatahist object, a rooarglist
    containing all corresponding roofit observables.  Optionally, a
    roohistpdf is created from the roodatahist object and stored as well.
    """

    def __init__(self, **kwargs):
        """Initialize ConvertRootHist2RooDataHist instance

        :param str name: name of link
        :param str read_key: histogram to pick up from datastore (or, if set, from histogram dict)
        :param str hist_dict_key: histograms dictionary from data store.
                                  If set, the histogram is read from this dict (optional)
        :param str store_key: key of roodatahist (optional)
        :param bool into_ws: if true, store in workspace, not datastore. Default is True.
        :param bool rm_original: if true, remove original histogram. Default is False.
        :param str create_hist_pdf: if set, create keys pdf from roodatahist with this name and add to ds or workspace
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ConvertRootHist2RooDataHist'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             hist_dict_key='',
                             store_key='',
                             into_ws=False,
                             rm_original=False,
                             create_hist_pdf='')

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize ConvertRootHist2RooDataHist"""

        # check input arguments
        self.check_arg_types(read_key=str, hist_dict_key=str, store_key=str)
        self.check_arg_vals('read_key')

        if not self.store_key:
            self.store_key = 'rdh_' + self.read_key.replace(':', '_vs_')

        if self.create_hist_pdf:
            assert isinstance(self.create_hist_pdf, str) and len(self.create_hist_pdf), \
                'create_hist_pdf needs to be a filled string'

        return StatusCode.Success

    def execute(self):
        """Execute ConvertRootHist2RooDataHist"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)
        ws = proc_mgr.service(RooFitManager).ws

        # basic checks on contents of the root histogram
        if not self.hist_dict_key:
            assert self.read_key in ds, 'key "%s" not in DataStore' % self.read_key
            hist = ds[self.read_key]
        else:
            assert self.hist_dict_key in ds, 'key "%s" not in DataStore' % self.hist_dict_key
            hist_dict = ds[self.hist_dict_key]
            assert self.read_key in hist_dict, 'key "%s" not in histogram dictionary "%s"' % \
                (self.read_key, self.hist_dict_key)
            hist = hist_dict[self.read_key]
        if not isinstance(hist, ROOT.TH1):
            raise TypeError('retrieved object "%s" not a ROOT histogram' % self.read_key)

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
            assert len(col), 'could not retrieve valid column name from axis %d its name' % i
            columns.append(col)

        # do conversion
        obs = ':'.join(columns)
        rdh, obs_list = data_conversion.hist_to_rdh(hist, obs)

        # create pdf of dataset as well?
        if self.create_hist_pdf:
            obs_set = ROOT.RooArgSet(obs_list)
            hpdf_name = self.create_hist_pdf
            hist_pdf = ROOT.RooHistPdf(hpdf_name, hpdf_name, obs_set, rdh)

        # 0. remove original histogram?
        if self.rm_original:
            if not self.hist_dict_key:
                del ds[self.read_key]
            else:
                del hist_dict[self.read_key]

        # 1. put object into the workspace
        if self.into_ws:
            try:
                ws.put(rdh, ROOT.RooFit.Rename(self.store_key))
                if self.create_hist_pdf:
                    ws.put(hist_pdf, RooFit.RecycleConflictNodes())
            except:
                raise RuntimeError('could not import object "%s" into rooworkspace' % self.read_key)
        # 2. put object into datastore
        else:
            ds[self.store_key] = rdh
            if self.create_hist_pdf:
                ds[hpdf_name] = hist_pdf

        n_rdh = rdh.numEntries()
        ds['n_' + self.store_key] = n_rdh
        self.log().debug('Stored roodatahist "%s" with length: %d', self.store_key, n_rdh)

        return StatusCode.Success
