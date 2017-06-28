# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RooFitPercentileBinning
# * Created: 2017/06/28
# * Description:                                                                   *
# *      Algorithm to evaluate percentile binning for given set
# *      of roofit observables. The binning configuration is stored
# *      in the observable(s)                                                      *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade.root_analysis import RooFitManager, data_conversion, roofit_utils

import ROOT

import pandas as pd
import numpy as np

# make sure Eskapade RooFit library is loaded
roofit_utils.load_libesroofit()


class RooFitPercentileBinning(Link):
    """Link to evaluate percentile binning for a given set of (roofit) observables"""

    def __init__(self, **kwargs):
        """Initialize RooFitPercentileBinning instance

        :param str name: name of link
        :param str read_key: key of input data to read from data store. Either a RooDataSet or pandas DataFrame.
        :param bool from_ws: if true, pick up input roodataset from workspace, not datastore. Default is false.
        :param dict var_number_of_bins: number of percentile bins per observable.
        :param str binning_name: name of binning configuration to assign to percentile binning
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'RooFitPercentileBinning'))

        # Process and register keyword arguments.  All arguments are popped from
        # kwargs and added as attributes of the link.  The values provided here
        # are defaults.
        self._process_kwargs(kwargs, read_key='', from_ws=False,
                             binning_name='percentile', var_number_of_bins={})

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize RooFitPercentileBinning"""

        # check input arguments
        self.check_arg_types(read_key=str, binning_name=str)
        self.check_arg_vals('read_key', 'binning_name')

        assert len(self.var_number_of_bins)>0, 'No input columns have been set.'

        return StatusCode.Success

    def execute(self):
        """Execute RooFitPercentileBinning"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)
        ws = proc_mgr.service(RooFitManager).ws

        # basic checks on contensts of the data frame
        if self.from_ws:
            data = ws.data(self.read_key)
            assert data is not None, 'Key %s not in workspace' % self.read_key
        else:
            assert self.read_key in ds, 'key "%s" not found in datastore' % self.read_key
            data = ds[self.read_key]

        # check presence of all columns
        columns = list(self.var_number_of_bins.keys())
        for col in columns:
            var = ws.var(col)
            if not var:
                raise NameError('Variable %s not found in workspace' % col)
            if not isinstance(var, ROOT.RooRealVar):
                self.log().warning('Variable %s is not a RooRealVar. Rejecting.' % col)
                del self.var_number_of_bins[col]

        # check data type
        if isinstance(data, ROOT.RooDataSet):
            assert data.numEntries() > 0, 'RooDataSet "%s" is empty' % self.read_key
            varset = data.get(0)
            for col in self.var_number_of_bins.keys():
                if not varset.find(col):
                    raise AssertionError('column %s not found in input roodataset' % col)
            df = data_conversion.rds_to_df(data, list(self.var_number_of_bins.keys()), ignore_lost_records=True)
        elif isinstance(data, pd.DataFrame):
            assert len(data.index) > 0, 'RooDataSet "%s" is empty' % self.read_key
            for col in self.var_number_of_bins.keys():
                if col not in data.columns:
                    raise AssertionError('column %s not found in input dataframe' % col)
            df = data[list(self.var_number_of_bins.keys())]
        else:
            raise TypeError('retrieved object "%s" not of type RooDataSet of DataFrame, but: %s' % (self.read_key, type(data)))
        # continuing below with df

        # evaluate and set quantiles per observable
        for col, nbins in self.var_number_of_bins.items():
            self.log().debug('Creating %d percentile bins for column "%s"' % (nbins, col))
            # x is a RooRealVar
            qs = np.linspace(0,1,nbins+1).tolist()
            bin_edges = df[col].quantile(qs)
            binning = ROOT.RooNonCentralBinning()
            x = ws.var(col)
            binning.setRange(x.getMin(),x.getMax())
            for b in bin_edges.values[1:nbins]:
                if b > x.getMin() and b < x.getMax():
                    binning.addBoundary(b)
            binning.setAverageFromData(data, x)
            x.setBinning(binning, self.binning_name)

        # cleanup of temporary df
        if isinstance(data, ROOT.RooDataSet):
            del df

        return StatusCode.Success
