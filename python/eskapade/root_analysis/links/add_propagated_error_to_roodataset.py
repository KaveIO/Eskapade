"""Project: Eskapade - A python-based package for data analysis.

Class: AddPropagatedErrorToRooDataSet

Created: 2017/04/12

Description:
    Algorithm to evaluates the error on a provided roofit function
    for each record in the given roodataset.
    The results are added back to the dataset.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import uuid

import ROOT

from eskapade import process_manager, Link, DataStore, StatusCode
from eskapade.root_analysis.roofit_manager import RooFitManager


class AddPropagatedErrorToRooDataSet(Link):
    """Evaluates errors on a provided roofit function.

    The evaluated errors are added as a new column to the input dataset.
    Optionally, the evaluated function values are added as a column to the
    input dataset as well.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str data: key of input data to read from data store or workspace
        :param str function: key of rooabsreal function to read from data store or workspace
        :param str fit_result: key of roofitresult object to read from data store or workspace,
                               used as input for error propagation
        :param bool from_ws: if true, try to retrieve data, function, and fit_result
                             from workspace instead of datastore. Default is false.
        :param str function_error_name: column name assigned to propagated errors that are appended to data
        :param bool add_function_to_data: add column of the function values to the data. Default is true
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'AddPropagatedErrorToRooDataSet'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             from_ws=False,
                             data='',
                             function='',
                             fit_result='',
                             function_error_name='',
                             add_function_to_data=True)

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments are filled strings
        self.check_arg_types(data=str, function=str, fit_result=str, function_error_name=str)
        self.check_arg_vals('data', 'function', 'fit_result', 'function_error_name')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # retrieve data set
        if self.from_ws:
            thedata = ws.data(self.data)
            assert thedata is not None, 'key "{}" not in workspace'.format(self.data)
        else:
            assert self.data in ds, 'key "{}" not found in datastore'.format(self.data)
            thedata = ds[self.data]
        if not isinstance(thedata, ROOT.RooDataSet):
            raise TypeError('retrieved object "{}" not of type RooDataSet, but: {}'.format(self.data, type(thedata)))
        assert thedata.numEntries() > 0, 'RooDataSet "{}" is empty.'.format(self.data)

        # retrieve function to propagate
        if self.from_ws:
            func = ws.function(self.function)
            assert func is not None, 'key {} not in workspace'.format(self.function)
        else:
            assert self.function in ds, 'key "{}" not found in datastore'.format(self.function)
            func = ds[self.function]
        if not isinstance(func, ROOT.RooAbsReal):
            raise TypeError('retrieved object "{}" not of type RooAbsReal, but: {}'.format(self.function, type(func)))

        # retrieve fit result to propagate errors of
        if self.from_ws:
            fit_result = ws.obj(self.fit_result)
            assert fit_result is not None, 'key {} not in workspace'.format(self.fit_result)
        else:
            assert self.fit_result in ds, 'key "{}" not found in datastore'.format(self.fit_result)
            fit_result = ds[self.fit_result]
        if not isinstance(fit_result, ROOT.RooFitResult):
            raise TypeError('retrieved object "{}" not of type RooAbsReal, but: {}'
                            .format(self.fit_result, type(fit_result)))

        # determine the observables of the function (= all non-fit parameters)
        # and check they are present in the input dataset
        pars_obs = [po.GetName() for po in func.getParameters(0)]
        pars = [p.GetName() for p in fit_result.floatParsFinal()]
        obs_names = [item for item in pars_obs if item not in pars]
        assert len(obs_names), 'no observables needed for function "{}"?'.format(self.function)
        obs_set = thedata.get()
        for o in obs_names:
            assert o in obs_set, 'observable "{}" not in dataset "{}"'.format(o, self.data)

        # retrieve observables
        obs = ','.join(obs_names)
        if self.from_ws:
            theobs = ws.set(obs)
        else:
            theobs = ds.get(obs, None)
        if not theobs:
            # try to create a temporary observables set
            temp_obs = uuid.uuid4().hex
            failure = ws.defineSet(temp_obs, obs)
            if not failure:
                theobs = ws.set(temp_obs)
            else:
                raise RuntimeError('unable to retrieve (/create) observables with name "{}"'.format(obs))
        if not theobs:
            raise RuntimeError('unable to retrieve (/create) observables')
        assert isinstance(theobs, ROOT.RooArgSet), 'retrieved object "{}" not of type RooArgSet'.format(obs)

        # all okay, here we go ...

        # 1. add func values column to data?
        if self.add_function_to_data:
            thedata.addColumn(func)

        # 2. calculate uncertainties on function per record
        perror = ROOT.RooRealVar(self.function_error_name, self.function_error_name, 0, 1e+300)
        perror_set = ROOT.RooArgSet(perror)
        perror_data = ROOT.RooDataSet('perror_data', 'perror_data', perror_set)

        for i in range(thedata.numEntries()):
            thedata.get(i)  # this call updates obs_set
            for o in obs_names:
                theobs[o].setVal(obs_set[o].getVal())
            perror.setVal(func.getPropagatedError(fit_result))
            perror_data.add(perror_set)

        # 3. perge uncertainties on pvalues with the original dataset
        thedata.merge(perror_data)

        # cleanup of temporary observables set
        if 'temp_obs' in vars():
            ws.removeSet(temp_obs)

        return StatusCode.Success
