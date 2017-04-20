# *****************************************************************************
# * Project: Eskapade - A python-based package for data analysis              *
# * Class  : TruncExpFit                                                      *
# * Created: 2017/04/19                                                       *
# *                                                                           *
# * Description:                                                              *
# *      Link to fit truncated exponential PDF to data                        *
# *                                                                           *
# * Authors:                                                                  *
# *      KPMG Big Data team, Amstelveen, The Netherlands                      *
# *                                                                           *
# * Redistribution and use in source and binary forms, with or without        *
# * modification, are permitted according to the terms listed in the file     *
# * LICENSE.                                                                  *
# *****************************************************************************

import logging
import numpy as np

import ROOT
from ROOT import RooFit

from eskapade import StatusCode, DataStore, Link, ProcessManager, ConfigObject
from eskapade.core import persistence
from eskapade.root_analysis import RooFitManager
from eskapade.root_analysis.roofit_utils import ROO_INF, create_roofit_opts
from eskapade.root_analysis.roofit_models import TruncExponential


class TruncExpFit(Link):
    """Fit truncated exponential PDF to data

    Fit an exponential PDF in a range with a variable upper bound to data.
    That is, the PDF is truncated at a different value for each record in
    the data.  A dataset with values for the PDF exponential variable and
    the upper bound must be provided.

    Optionally an additional dataset with range upper bounds may be
    provided.  This will be considered to be a dataset of samples, which may
    or may not have a corresponding event in the first dataset.  In this
    case an estimate of the number of events without any range upper bounds
    will be provided, given the samples in the upper-bound dataset.  This
    result is also visualized in a plot of the PDFs with and without range
    upper bounds and histograms of the corresponding event data.
    """

    def __init__(self, **kwargs):
        """Initialize the TruncExpFit instance

        :param str name: name of link instance
        :param str read_key: data-store key of input data
        :param str max_var_data_key: data-store key of dataset with range upper-bound values
        :param str model_name: name of truncated-exponential model to use
        """

        Link.__init__(self, kwargs.pop('name', 'fit_trunc_exp'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', max_var_data_key='', model_name='', results_path='')
        self.kwargs = kwargs
        self.fit_result = None
        self._fit_cmd_args = None

    def initialize(self):
        """Inititialize the TruncExpFit execution"""

        # check input arguments
        self.check_arg_types(read_key=str, max_var_data_key=str, model_name=str, results_path=str)
        self.check_arg_vals('read_key', 'model_name')

        # create process-manager and service instances
        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        rfm = proc_mgr.service(RooFitManager)

        # check if model exists
        model = rfm.model(self.model_name)
        if not model:
            self.log().warning('Model "{}" does not exist; creating with default values'.format(self.model_name))
            model = rfm.model(self.model_name, model_cls=TruncExponential)

        # check if model PDF has been built
        if not model.is_built:
            model.build_model()

        # process command arguments for fit function
        self._fit_cmd_args = create_roofit_opts('fit', ConditionalObservables=model.max_var_set, **self.kwargs)

        # get path to results directory
        if not self.results_path:
            self.results_path = persistence.io_dir('results_data', settings.io_conf())
        persistence.create_dir(self.results_path)

        return StatusCode.Success

    def execute(self):
        """Execute TruncExpFit"""

        # get process manager and services
        proc_mgr = ProcessManager()
        ds = proc_mgr.service(DataStore)
        rfm = proc_mgr.service(RooFitManager)

        # get PDF from RooFitManager
        model = rfm.model(self.model_name)

        # get data
        data = ds.get(self.read_key)
        if not isinstance(data, ROOT.RooAbsData):
            raise TypeError('data with key "{}" are not RooFit data'.format(self.read_key))

        # fit data
        self.fit_result = model.pdf.fitTo(data, self._fit_cmd_args)

        # check fit result
        if self.fit_result.status() != 0:
            self.log().error('Failed fit: status code {:d}'.format(self.fit_result.status()))
            return StatusCode.Failure

        # print fit result
        print_info = self.log().getEffectiveLevel() <= logging.INFO
        self.log().info('Fitted parameter values:')
        if print_info:
            self.fit_result.Print('v')
        self.log().info('Fitted parameter correlation matrix:')
        if print_info:
            self.fit_result.correlationMatrix().Print()

        # check for range upper-bound data
        if not self.max_var_data_key:
            self.log().debug('No range upper-bound samples provided; not estimating number of events without bounds')
            return StatusCode.Success
        mv_data = ds.get(self.max_var_data_key)
        if not isinstance(mv_data, ROOT.RooAbsData):
            raise TypeError('data with key "{}" are not RooFit data'.format(self.read_key))

        # calculate normalization of PDF in full range
        norm_full, norm_data = model.create_norm(data=mv_data, range_min=None, range_max=ROO_INF)

        # estimate total number of events
        num_samp = np.float64(mv_data.sumEntries())
        norm_ratio_val, norm_ratio_err = est_norm_ratio(norm_full, norm_data, self.fit_result)
        ev_frac_val = np.float64(data.sumEntries()) / num_samp
        ev_frac_err = np.sqrt(ev_frac_val * (1. - ev_frac_val) / num_samp)
        n_ev_val = norm_ratio_val * ev_frac_val * num_samp
        n_ev_err = np.sqrt((ev_frac_val * norm_ratio_err)**2 + (norm_ratio_val * ev_frac_err)**2) * num_samp
        self.log().debug('Number of events with current range bounds: %.0f out of %.0f samples (%.1f%% +/- %.1f%%)',
                         ev_frac_val * num_samp, num_samp, ev_frac_val * 100., ev_frac_err * 100.)
        self.log().debug('Estimated PDF-normalization ratio: %.3f +/- %.3f', norm_ratio_val, norm_ratio_err)
        self.log().info('Estimated number of events from these samples in full range: %.0f +/- %.0f (%.0f samples)',
                        n_ev_val, n_ev_err, num_samp)

        # plot data and model
        make_plots(data, model, n_ev_val / norm_full.getVal(), '{0:s}/{1:s}.pdf'.format(self.results_path, model.name))

        return StatusCode.Success


def make_plots(data, model, full_norm_ratio, plots_path):
    """Make plots of data and model"""

    # plot max-var data
    mv_frame = model.max_var.frame(min(50, max(30, data.numEntries() // 100)))
    data.plotOn(mv_frame, create_roofit_opts('data_plot'))

    # calculate normalization of PDF in plot range
    var = model.pdf.getVariables()[model.var]
    model.pdf.getVariables()[model.max_var].setVal(var.getMax())
    norm_plot = model.pdf.createIntegral(model.var_set, ROOT.RooArgSet())

    # plot var data
    v_frame = var.frame(min(50, max(30, data.numEntries() // 100)))
    v_frame.updateNormVars(model.var_set)
    data.plotOn(v_frame, create_roofit_opts('data_plot'))
    norm_fac = norm_plot.getVal() * full_norm_ratio * v_frame.getFitRangeBinW()
    model.pdf.plotOn(v_frame, create_roofit_opts('pdf_plot', Project=model.max_var_set, ProjWData=(data, False)))
    model.pdf.plotOn(v_frame, create_roofit_opts('pdf_plot', Normalization=(norm_fac, 0), LineStyle=ROOT.kDashed))

    # draw max-var plots
    mv_canv = ROOT.TCanvas(model.max_var.GetName())
    mv_frame.Draw()
    mv_canv.Print('{}('.format(plots_path))

    # draw var plots
    v_canv = ROOT.TCanvas(model.var.GetName())
    v_frame.Draw()
    v_canv.Print('{}'.format(plots_path))

    # draw var plots (logarithmic scale)
    v_canv = ROOT.TCanvas(model.var.GetName() + '_log')
    v_canv.SetLogy(1)
    v_frame.Draw()

    v_canv.Print('{})'.format(plots_path))


def est_norm_ratio(norm_full, norm_data, fit_result):
    """Estimate total number of events without range bounds"""

    # assume norm in full range is equal to one
    norm_full_val = norm_full.getVal()
    if not norm_full_val == 1.:
        raise AssertionError('norm in full range is not equal to 1: value = {:g}'.format(norm_full_val))
    norm_full_err = norm_full.getPropagatedError(fit_result)
    if not norm_full_err == 0.:
        raise AssertionError('norm in full range depends on parameters: error = {:g}'.format(norm_full_err))

    # create variable for ratio of PDF normalizations
    norm_list = ROOT.RooArgList(norm_data)
    norm_ratio = ROOT.RooFormulaVar('norm_ratio', '', '1./@0', norm_list)

    # estimate value and error of norm ratio
    norm_ratio_val = np.float64(norm_ratio.getVal())
    norm_ratio_err = norm_ratio.getPropagatedError(fit_result)

    return norm_ratio_val, norm_ratio_err
