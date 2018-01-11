"""Project: Eskapade - A python-based package for data analysis.

Class: RooFitPercentileBinning

Created: 2017/06/28

Description:
    Algorithm to evaluate percentile binning for given set
    of roofit observables. The binning configuration is stored
    in the observable(s)

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ROOT
import numpy as np
import pandas as pd

from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.root_analysis import data_conversion, roofit_utils
from eskapade.root_analysis.roofit_manager import RooFitManager


class RooFitPercentileBinning(Link):
    """Evaluate percentile binning for given variable set."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store; either a RooDataSet or pandas DataFrame
        :param bool from_ws: if true, pick up input roodataset from workspace, not datastore (default is False)
        :param dict var_number_of_bins: number of percentile bins per observable
        :param str binning_name: name of binning configuration to assign to percentile binning
        """
        # initialize link and process arguments
        Link.__init__(self, kwargs.pop('name', 'RooFitPercentileBinning'))
        self._process_kwargs(kwargs, read_key='', from_ws=False, binning_name='percentile', var_number_of_bins={})
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link.."""
        # check input arguments
        self.check_arg_types(read_key=str, binning_name=str)
        self.check_arg_vals('read_key', 'binning_name', 'var_number_of_bins')

        # make sure Eskapade RooFit library is loaded for the RooNonCentralBinning class
        roofit_utils.load_libesroofit()

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # basic checks on contensts of the data frame
        if self.from_ws:
            data = ws.data(self.read_key)
            if data is None:
                raise RuntimeError('No data with key "{}" in workspace.'.format(self.read_key))
        else:
            if self.read_key not in ds:
                raise KeyError('Key "{}" not found in datastore.'.format(self.read_key))
            data = ds[self.read_key]

        # check presence of all columns
        columns = list(self.var_number_of_bins)
        for col in columns:
            var = ws.var(col)
            if not var:
                raise RuntimeError('no variable with key "{}" in workspace.'.format(col))
            if not isinstance(var, ROOT.RooRealVar):
                self.logger.warning('Variable "{var}" is not a RooRealVar; rejecting.', var=col)
                del self.var_number_of_bins[col]

        # check data type
        if isinstance(data, ROOT.RooDataSet):
            if data.numEntries() == 0:
                raise AssertionError('RooDataSet "{}" is empty.'.format(self.read_key))
            varset = data.get(0)
            for col in self.var_number_of_bins:
                if not varset.find(col):
                    raise AssertionError('Column "{}" not found in input roodataset.'.format(col))
            df = data_conversion.rds_to_df(data, list(self.var_number_of_bins), ignore_lost_records=True)
        elif isinstance(data, pd.DataFrame):
            if len(data.index) == 0:
                raise AssertionError('RooDataSet "{}" is empty.'.format(self.read_key))
            for col in self.var_number_of_bins:
                if col not in data.columns:
                    raise AssertionError('Column "{}" not found in input dataframe.'.format(col))
            df = data[list(self.var_number_of_bins.keys())]
        else:
            raise TypeError('Object "{0:s}" not of type RooDataSet/DataFrame (got "{1!s}").'
                            .format(self.read_key, type(data)))
        # continuing below with df

        # evaluate and set quantiles per observable
        for col, nbins in self.var_number_of_bins.items():
            self.logger.debug('Creating {n:d} percentile bins for column "{col}".', n=nbins, col=col)
            # column variable is a RooRealVar
            qs = np.linspace(0, 1, nbins + 1).tolist()
            bin_edges = df[col].quantile(qs)
            binning = ROOT.RooNonCentralBinning() if isinstance(data, ROOT.RooDataSet) else ROOT.RooBinning()
            col_var = ws.var(col)
            binning.setRange(col_var.getMin(), col_var.getMax())
            for b in bin_edges.values[1:nbins]:
                if col_var.getMin() < b < col_var.getMax():
                    binning.addBoundary(b)
            if isinstance(data, ROOT.RooDataSet):
                binning.setAverageFromData(data, col_var)
            col_var.setBinning(binning, self.binning_name)

        # cleanup of temporary df
        if isinstance(data, ROOT.RooDataSet):
            del df

        return StatusCode.Success
