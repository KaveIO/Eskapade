"""Project: Eskapade - A python-based package for data analysis.

Class: ConvertRooDataSet2DataFrame

Created: 2017/03/25

Description:
    Algorithm to convert an input roodataset to a pandas dataframe

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


class ConvertRooDataSet2DataFrame(Link):
    """Convert an input RooFit dataset into a Pandas dataframe.

    Input roodataset can be picked up from either datastore or rooworkspace.
    The output dataframe is stored in the datastore.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input roodataset to read from data store or workspace
        :param str store_key: key of output data to store in data store (optional)
        :param bool from_ws: if true, pick up input roodataset from workspace, not datastore. Default is false.
        :param bool rm_original: if true, input roodataset is removed from ds/ws. Default is false.
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ConvertRooDataSet2DataFrame'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             read_key='',
                             store_key='',
                             from_ws=False,
                             rm_original=False)

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(read_key=str, store_key=str)
        self.check_arg_vals('read_key')

        if len(self.store_key) == 0:
            self.store_key = 'df_' + self.read_key.replace('rds_', '')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # basic checks on contents of the data frame
        if self.from_ws:
            rds = ws.data(self.read_key)
            assert rds is not None, 'Key {} not in workspace.'.format(self.read_key)
        else:
            assert self.read_key in ds, 'Key "{}" not found in datastore.'.format(self.read_key)
            rds = ds[self.read_key]
        if not isinstance(rds, ROOT.RooDataSet):
            raise TypeError('Retrieved object "{}" not of type RooDataSet, but: {}.'.format(self.read_key, type(rds)))
        assert rds.numEntries() > 0, 'RooDataSet "{}" is empty.'.format(self.read_key)

        # do conversion
        df = data_conversion.rds_to_df(rds)

        # remove original rds?
        if self.rm_original:
            if self.from_ws:
                # FIXME can datasets be deleted from an rws? dont know how
                pass
            else:
                del ds[self.read_key]

        # put object into the datastore
        ds[self.store_key] = df
        n_df = len(df.index)
        ds['n_' + self.store_key] = n_df
        self.logger.debug('Stored dataframe "{key}" with length: {length:d}', key=self.store_key, length=n_df)

        return StatusCode.Success
