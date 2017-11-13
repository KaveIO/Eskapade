"""Project: Eskapade - A python-based package for data analysis.

Class: ConvertRooDataSet2RooDataHist

Created: 2017/03/25

Description:
    Algorithm to convert an input RooDataSet to a Pandas data frame

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import uuid

import ROOT

from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.root_analysis import data_conversion
from eskapade.root_analysis.roofit_manager import RooFitManager


class ConvertRooDataSet2RooDataHist(Link):
    """Convert input RooFit dataset into a Pandas dataframe.

    Input RooDataSet can be picked up from either the data store or the
    workspace.  The output data frame is stored in the data store.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input roodataset to read from data store or workspace
        :param str store_key: key of output data to store in data store (optional)
        :param bool from_ws: if true, pick up input roodataset from workspace instead of data store (default is False)
        :param bool rm_original: if true, input roodataset is removed from ds/ws (default is False)
        :param list columns: columns to pick up from input RooDataSet
        :param str binning_name: name of binning configuration with which to construct RooDataHist
        """
        # initialize link and process arguments
        Link.__init__(self, kwargs.pop('name', 'ConvertRooDataSet2RooDataHist'))
        self._process_kwargs(kwargs,
                             read_key='',
                             store_key='',
                             from_ws=False,
                             rm_original=False,
                             columns=[],
                             binning_name='')
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, store_key=str, binning_name=str)
        self.check_arg_types(recurse=True, columns=str)
        self.check_arg_vals('read_key', 'columns')

        if not self.store_key:
            self.store_key = 'rdh_' + self.read_key.replace('rds_', '')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # basic checks on contensts of the data frame
        if self.from_ws:
            rds = ws.data(self.read_key)
            if rds is None:
                raise RuntimeError('No data with key "{}" in workspace.'.format(self.read_key))
        else:
            if self.read_key not in ds:
                raise KeyError('Key "{}" not found in datastore.'.format(self.read_key))
            rds = ds[self.read_key]
        if not isinstance(rds, ROOT.RooDataSet):
            raise TypeError('Retrieved object "{0:s}" not of type RooDataSet (got "{1:s}").'.format(self.read_key,
                                                                                                    str(type(rds))))
        if rds.numEntries() == 0:
            raise AssertionError('RooDataSet "{}" is empty.'.format(self.read_key))

        # check presence of all columns
        for col in self.columns:
            if not ws.var(col):
                raise RuntimeError('Variable "{}" not found in workspace,'.format(col))

        # create a temporary observables set of the columns
        temp_obs = uuid.uuid4().hex
        obs = ','.join(self.columns)
        failure = ws.defineSet(temp_obs, obs)
        if not failure:
            theobs = ws.set(temp_obs)
        else:
            raise RuntimeError('unable to retrieve (/create) observables with name "{}".'.format(obs))

        # do conversion from RooDataSet to RooDataHist
        self.logger.debug('Converting roodataset "{read_key}" into roodatahist "{store_key}".',
                          read_key=self.read_key, store_key=self.store_key)
        rdh = data_conversion.rds_to_rdh(rds, rf_varset=theobs, binning_name=self.binning_name)

        # remove original rds?
        if self.rm_original:
            if self.from_ws:
                # FIXME can datasets be deleted from an rws? dont know how
                pass
            else:
                del ds[self.read_key]

        # put object into the datastore
        ds[self.store_key] = rdh
        n_rdh = rdh.numEntries()
        ds['n_' + self.store_key] = n_rdh
        self.logger.debug('Stored roodatahist "{key}" with number of bins: {n:d}.', key=self.store_key, n=n_rdh)

        # cleanup of temporary observables set
        ws.removeSet(temp_obs)

        return StatusCode.Success
