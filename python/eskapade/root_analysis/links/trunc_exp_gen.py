"""Project: Eskapade - A python-based package for data analysis.

Class: TruncExpGen

Created: 2017/04/19

Description:
    Link to generate data with a truncated exponential PDF

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import array

import ROOT
import numpy as np
from ROOT import RooFit

from eskapade import StatusCode, DataStore, Link
from eskapade import process_manager
from eskapade.root_analysis.roofit_manager import RooFitManager
from eskapade.root_analysis.roofit_models import TruncExponential
from eskapade.root_analysis.roofit_utils import create_roofit_opts

EVENT_FRACTION = 0.6
NUM_DUMMY_BOUNDS = 3
NUM_DUMMY_EVENTS = 10000


class TruncExpGen(Link):
    """Generate with truncated exponential PDF.

    Generate data with an exponential PDF in a range with a variable upper
    bound.  That is, the PDF is truncated at a different value for each
    record in the data.

    A dataset with upper bounds must be provided.  Each bound is considered
    to be a sample, for which an event may or not be generated.  The
    fraction of samples for which an event is generated can be set.  For
    each event, a value is generated with the exponential PDF truncated at
    the range upper bound for the corresponding sample.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link instance
        :param str store_key: data-store key of generated data
        :param str max_var_data_key: data-store key of dataset with range upper-bound values
        :param str model_name: name of truncated-exponential model to use
        """
        Link.__init__(self, kwargs.pop('name', 'fit_trunc_exp'))

        # process keyword arguments
        self._process_kwargs(kwargs, store_key='', max_var_data_key='', model_name='', event_frac=EVENT_FRACTION)
        self.kwargs = kwargs
        self._gen_cmd_args = None

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_types(store_key=str, max_var_data_key=str, model_name=str, event_frac=float)
        self.check_arg_vals('store_key', 'max_var_data_key', 'model_name', 'event_frac')

        # check if model exists
        rfm = process_manager.service(RooFitManager)
        model = rfm.model(self.model_name)
        if not model:
            self.logger.warning('Model "{model}" does not exist; creating with default values.', model=self.model_name)
            model = rfm.model(self.model_name, model_cls=TruncExponential)

        # check if model PDF has been built
        if not model.is_built:
            model.build_model()

        # process command arguments for generate function
        self._gen_cmd_args = create_roofit_opts(create_linked_list=False, **self.kwargs)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # get process manager and services
        ds = process_manager.service(DataStore)
        rfm = process_manager.service(RooFitManager)

        # get PDF from RooFitManager
        model = rfm.model(self.model_name)

        # check if dataset with upper bounds exists in data store
        if self.max_var_data_key not in ds:
            self.logger.warning('No range upper-bound data in data store; generating {n:d} dummy bounds',
                                n=NUM_DUMMY_EVENTS)
            ds[self.max_var_data_key] = gen_max_var_data(model)

        # get max-var data
        max_var_data = ds.get(self.max_var_data_key)
        if not isinstance(max_var_data, ROOT.RooAbsData):
            raise TypeError('data with key "{}" are not RooFit data'.format(self.read_key))

        # select max-var data
        mv_sel_data = sel_max_var_data(model, max_var_data, self.event_frac)

        # generate data
        proto_arg = RooFit.ProtoData(mv_sel_data, False, False)
        data = model.pdf.generate(model.var_set, proto_arg, *self._gen_cmd_args.values())
        ds[self.store_key] = data

        return StatusCode.Success


def sel_max_var_data(model, max_var_data, event_frac):
    """Select upper-bound data with PDF integral values."""
    # add column with PDF-integral values
    mv_sel_data = ROOT.RooDataSet(max_var_data)
    pdf_int = model.pdf.createIntegral(model.var_set, ROOT.RooArgSet())
    mv_sel_data.addColumn(pdf_int)

    # check PDF-integral values are in expected range
    int_min = array.array('d', [0.])
    int_max = array.array('d', [0.])
    mv_sel_data.getRange(mv_sel_data.get()[pdf_int.GetName()], int_min, int_max)
    if int_min[0] < 0. or int_max[0] > 1.:
        raise ValueError('unexpected range of integral values: ({0:g}, {1:g})'.format(int_min[0], int_max[0]))

    # generate uniform random values for selection
    uni_var = ROOT.RooRealVar('uni_var', '', 0., 1.)
    uni_set = ROOT.RooArgSet(uni_var)
    uni_pdf = ROOT.RooUniform('uni_pdf', '', uni_set)
    mv_sel_data.merge(uni_pdf.generate(uni_set, mv_sel_data.numEntries()))

    # select max-var values based on integral values
    sel_str = '{0:s}<{2:.10g}*{1:s}'.format(uni_var.GetName(), pdf_int.GetName(), event_frac)
    return mv_sel_data.reduce(sel_str).reduce(model.max_var_set)


def gen_max_var_data(model):
    """Generate range upper-bound data."""
    # create a dataset for upper bounds
    mv_data = ROOT.RooDataSet('max_var_data', 'Max-var data', model.max_var_set)
    mv_set = mv_data.get()

    # determine the max-var range for generation
    mv_var = mv_set.find(mv_set.contentsString())
    mv_range = (mv_var.getMin() + 0.2 * (mv_var.getMax() - mv_var.getMin()),
                mv_var.getMin() + 0.9 * (mv_var.getMax() - mv_var.getMin()))

    # generate upper bounds
    mv_vals = np.random.uniform(*mv_range, NUM_DUMMY_BOUNDS)
    for _ in range(NUM_DUMMY_EVENTS):
        mv_var.setVal(np.random.choice(mv_vals, size=1, replace=True))
        mv_data.add(mv_set)

    return mv_data
