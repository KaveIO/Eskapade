"""Project: Eskapade - A Python-based package for data analysis.

Module: root_analysis.roofit_models

Created: 2017/04/24

Description:
    Eskapade models based on RooFit PDFs

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ROOT
import numpy as np
import pandas as pd
from ROOT import RooFit

from eskapade.root_analysis import roofit_utils


class RooFitModel:
    """Base class for RooFit models."""

    def __init__(self, ws, name='', load_libesroofit=False):
        """Initialize model instance.

        :param ROOT.RooWorkspace ws: RooFit workspace
        :param str name: name of model
        :param bool load_libesroofit: load Eskapade RooFit library upon initialization (default is False)
        """
        # check workspace
        if not isinstance(ws, ROOT.RooWorkspace):
            raise TypeError('invalid workspace specified (type "{}")'.format(ws.__class__.__name__))

        # set attributes
        self._name = str(name) if name else self.__class__.__name__
        self._ws = ws
        self._is_built = False
        self._pdf_name = None

        if load_libesroofit:
            # load Eskapade RooFit library
            roofit_utils.load_libesroofit()

    @property
    def name(self):
        """Name of model."""
        return self._name

    @property
    def ws(self):
        """Workspace to create model in."""
        return self._ws

    @property
    def is_built(self):
        """Check if model is built in workspace."""
        return self._is_built

    @property
    def pdf_name(self):
        """Name of model PDF."""
        return self._pdf_name

    @property
    def pdf(self):
        """Model PDF."""
        if not self.pdf_name or self.pdf_name not in self.ws:
            return None
        return self.ws[self.pdf_name]


class TruncExponential(RooFitModel):
    """Exponential model with variable range upper bound."""

    def __init__(self, ws, name='', var_range=None, var=None, max_var=None, exp=None, fracs=None):
        """Initialize an instance.

        :param ROOT.RooWorkspace ws: RooFit workspace
        :param str name: name of model
        :param tuple var_range: range of PDF variable: (min, max)
        :param tuple var: PDF variable: (name, value)
        :param tuple max_var: variable upper limit of PDF range: (name, value)
        :param list exp: list of exponential-rate parameters: [(name0, value0), (name1, value1), ...]
        :param list fracs: list of exponential-fraction parameters: [(name0, value0), ...]
        """
        # initialize RooFitModel instance
        super(TruncExponential, self).__init__(ws, name=name, load_libesroofit=True)
        self._pdf_name = self.name

        # set attributes
        self._var_range = (float(var_range[0]), float(var_range[1])) if var_range else (0., 1.)
        self._var = (str(var[0]), float(var[1])) if var else ('var', 0.)
        self._max_var = (str(max_var[0]), float(max_var[1])) if max_var else ('max_var', 1.)
        self._exp = [(str(v[0]), float(v[1])) for v in exp] if exp else [('exp', -1.)]
        self._fracs = [(str(v[0]), float(v[1])) for v in fracs] if fracs else []

    @property
    def var(self):
        """PDF variable."""
        if not self.is_built:
            return None
        return self.ws[self._var[0]]

    @property
    def max_var(self):
        """Range upper bound of PDF variable."""
        if not self.is_built:
            return None
        return self.ws[self._max_var[0]]

    @property
    def var_set(self):
        """Set containing PDF variable."""
        if not self.is_built:
            return None
        return self.ws.set('var_set')

    @property
    def max_var_set(self):
        """Set containing range upper bound of PDF variable."""
        if not self.is_built:
            return None
        return self.ws.set('max_var_set')

    @property
    def all_vars_set(self):
        """Set containing PDF variables."""
        if not self.is_built:
            return None
        return self.ws.set('all_vars_set')

    def build_model(self):
        """Build truncated exponential model in workspace."""
        # create variable
        if self._var[0] not in self.ws:
            self.ws.factory('{0:s}[{1:.10g}]'.format(self._var[0], self._var[1]))
        self.ws[self._var[0]].setMin(self._var_range[0])
        self.ws[self._var[0]].setMax(self._var_range[1])

        # create variable for upper bound of range
        if self._max_var[0] not in self.ws:
            self.ws.factory('{0:s}[{1:.10g}]'.format(self._max_var[0], self._max_var[1]))
        self.ws[self._max_var[0]].setMin(self._var_range[0])
        self.ws[self._max_var[0]].setMax(self._var_range[1])

        # create variable sets
        self.ws.defineSet('var_set', self._var[0])
        self.ws.defineSet('max_var_set', self._max_var[0])
        self.ws.defineSet('all_vars_set', '{0:s},{1:s}'.format(self._var[0], self._max_var[0]))

        # create rate parameters
        for exp in self._exp:
            if exp[0] not in self.ws:
                self.ws.factory('{0:s}[{1:.10g}]'.format(exp[0], exp[1]))
                self.ws[exp[0]].setConstant(False)
        exp_names = ','.join(exp[0] for exp in self._exp)

        # create fractions
        for frac in self._fracs:
            if frac[0] not in self.ws:
                self.ws.factory('{0:s}[{1:.10g}]'.format(frac[0], frac[1]))
                self.ws[frac[0]].setConstant(False)
        frac_names = ','.join(frac[0] for frac in self._fracs)

        # build model
        bld_str = 'TruncExponential::{0:s}({1:s},{2:s},{{{3:s}}},{{{4:s}}})'.format(self.name, self._var[0],
                                                                                    self._max_var[0], exp_names,
                                                                                    frac_names)
        self.ws.factory(bld_str)

        # check PDF
        if not self.pdf or not isinstance(self.pdf, ROOT.RooAbsPdf):
            raise RuntimeError('PDF of model "{}" not built correctly'.format(self.name))
        self._is_built = True

    def create_norm(self, data=None, range_min=None, range_max=None):
        """Create PDF normalization-integral objects.

        Calculate two objects representing normalization integrals of the PDF
        function.  The first integral is calculated in the range determined by
        the specified minimum value and the range upper bounds in the provided
        dataset.  The second integral is calculated in the specified range.

        :param ROOT.RooDataSet data: dataset with upper bounds of range
        :param float range_min: lower bound of integration range
        :param float range_max: upper bound of integration range
        :returns: normalization integrals
        :rtype: tuple
        """
        # check if model was built
        if not self.is_built:
            self.build_model()

        # check specified range
        if range_min is not None and not isinstance(range_min, float):
            raise TypeError('expected float for minimum of range (got type "{}")'.format(type(range_min).__name__))
        if range_max is not None and not isinstance(range_max, float):
            raise TypeError('expected float for maximum of range (got type "{}")'.format(type(range_max).__name__))
        if range_min is not None and range_max is not None and range_min >= range_max:
            raise ValueError('invalid range specified: ({0:g}, {1:g})'.format(range_min, range_max))

        # create variables with required range
        var = self.pdf.getVariables()[self.var].clone(self.var.GetName())
        if range_min is not None:
            var.setMin(range_min)
        if range_max is not None:
            var.setMax(range_max)
        max_var = ROOT.RooConstVar(self.max_var.GetName(), '', roofit_utils.ROO_INF)

        # create new PDF with required range
        all_vars_set = ROOT.RooArgSet(var, max_var)
        pdf = self.pdf.clone(self.name)
        pdf.redirectServers(all_vars_set, True)

        # build PDF integral object in full range
        pdf_norm_full = pdf.createIntegral(self.var_set, ROOT.RooArgSet())
        pdf_norm_full._created_vars = [var, max_var]

        if not data:
            return pdf_norm_full, pdf_norm_full.clone(pdf_norm_full.GetName())

        # check data
        if not isinstance(data, ROOT.RooDataSet):
            raise TypeError('expected "RooDataSet", got "{}"'.format(type(data).__name__))

        # set max-var range in data
        data = data.reduce(self.max_var_set)
        data.get()[self.max_var].setMin(-roofit_utils.ROO_INF)
        data.get()[self.max_var].setMax(+roofit_utils.ROO_INF)

        # create new PDF with max-var from data
        all_vars_set = ROOT.RooArgSet(var, data.get()[self.max_var])
        pdf = self.pdf.clone(self.name)
        pdf.redirectServers(all_vars_set, True)

        # build PDF integral object in full range
        pdf_norm_data = pdf.createIntegral(self.var_set, ROOT.RooArgSet())
        pdf_norm_data = ROOT.RooDataWeightedAverage('{0:s}_norm_{1:s}'.format(self.name, data.GetName()), '',
                                                    pdf_norm_data, data, ROOT.RooArgSet())

        return pdf_norm_full, pdf_norm_data


class LinearRegression(RooFitModel):
    """Least-squares linear regression model."""

    def __init__(self, ws, name='', fit_intercept=True, minimizer='Minuit2', strategy=2):
        """Initialize LinearRegression instance.

        :param ROOT.RooWorkspace ws: RooFit workspace
        :param bool fit_intercept: build model with variable intercept parameter
        :param str minimizer: minimization tool for fit (minimizer type for ROOT.RooMinimizer)
        :param int stategy: fit strategy (strategy for ROOT.RooMinimizer)
        """
        # initialize RooFitModel instance
        super(LinearRegression, self).__init__(ws, name)

        # set default values for attributes
        self.residues_ = None
        self.fit_intercept = fit_intercept
        self.minimizer = minimizer
        self.strategy = strategy
        self.sample_data = None
        self.target_names = []
        self.feat_names = []
        self.fit_result = None

    @property
    def coef_(self):
        """Return fitted values of coefficients."""
        if not self.fit_result:
            return None
        fit_pars = self.fit_result.floatParsFinal()
        return np.array([np.float64(fit_pars['c_{}'.format(feat_name)]) for feat_name in self.feat_names])

    @property
    def intercept_(self):
        """Return fitted intercept value."""
        if not self.fit_result:
            return None
        return np.float64(self.fit_result.floatParsFinal()['c_int'])

    def fit(self, X, y):
        """Fit model to specified data.

        :param X: samples of features
        :param y: targets
        """
        # load data
        self.load_data(X, y)

        # run fit
        self.fit_dataset()

    def fit_dataset(self):
        """Fit model to pre-loaded data."""
        # check if all model inputs are set
        if not (self.ws and self.sample_data and self.target_names and self.feat_names):
            raise RuntimeError('A workspace, sample data, target names, and feature names are needed to do the fit')

        # build model
        self.build_model()

        # set intercept variable
        self.ws['c_int'].setVal(0.)
        self.ws['c_int'].setConstant(not bool(self.fit_intercept))

        # set coefficient values
        for feat_name in self.feat_names:
            feat = self.ws['c_{}'.format(feat_name)]
            feat.setVal(0.)
            feat.setConstant(False)

        # create minimizer
        minimizer = ROOT.RooMinimizer(self.ws['scaled_chisq_func'])
        minimizer.setMinimizerType(self.minimizer)
        minimizer.setStrategy(self.strategy)

        # minimize chi^2 function
        minimizer.migrad()
        minimizer.hesse()
        self.fit_result = minimizer.save()

    def load_data(self, X, y):
        """Load target and feature data."""
        # get number of targets
        try:
            ntargets = len(y.iloc[0]) if isinstance(y, pd.DataFrame) else len(y[0]) if hasattr(y[0], '__len__') else 1
            self.target_names = ['y{0:d}'.format(tar_it) for tar_it in range(ntargets)]
        except Exception:
            raise RuntimeError('incorrect targets argument (y): (#samples, #targets) sequence expected')

        # check number of targets
        if ntargets > 1:
            raise NotImplementedError('multiple target variables not implemented')

        # get number of features
        try:
            nfeats = len(X.iloc[0]) if isinstance(X, pd.DataFrame) else len(X[0]) if hasattr(X[0], '__len__') else 1
            self.feat_names = ['X{0:d}'.format(feat_it) for feat_it in range(nfeats)]
        except Exception:
            raise RuntimeError('incorrect features argument (X): (#samples, #features) sequence expected')

        # create target, feature, and coefficient variables
        target_set = ROOT.RooArgSet()
        feat_set = ROOT.RooArgSet()
        for name in self.target_names:
            tar_var = self.ws.factory('{}[0]'.format(name)) if name not in self.ws else self.ws[name]
            target_set.add(tar_var)
        for name in self.feat_names:
            feat_var = self.ws.factory('{}[0]'.format(name)) if name not in self.ws else self.ws[name]
            feat_set.add(feat_var)
            if 'c_{}[0]'.format(name) not in self.ws:
                self.ws.factory('c_{}[0]'.format(name))
        if 'c_int' not in self.ws:
            self.ws.factory('c_int[0]')

        # create data set of samples
        sample_set = ROOT.RooArgSet(target_set, feat_set)
        self.sample_data = ROOT.RooDataSet('sample_data', 'sample_data', sample_set, RooFit.StoreError(target_set))
        sample_set = self.sample_data.get()

        # fill dataset with target and feature values
        for targets, feats in zip(y, X):
            if not hasattr(targets, '__iter__'):
                targets = [targets]
            if not hasattr(feats, '__iter__'):
                feats = [feats]
            for name, val in zip(self.target_names, targets):
                target = sample_set.find(name)
                target.setVal(val)
                target.setError(1.)
            for name, val in zip(self.feat_names, feats):
                sample_set.setRealValue(name, val)
            self.sample_data.add(sample_set)

    def build_model(self):
        """Build prediction function and corresponding chi^2."""
        # check if all model inputs are set
        if not (self.ws and self.sample_data and self.target_names and self.feat_names):
            raise RuntimeError('A workspace, sample data, target names, and feature names are needed to build model')

        # create prediction and chi^2 functions
        fact_cmd = 'sum::pred_func(c_int*1,{})'.format(','.join('c_{0:s}*{0:s}'.format(f) for f in self.feat_names))
        pred_func = self.ws.factory(fact_cmd)
        self.ws['chisq_func'] = pred_func.createChi2(self.sample_data, RooFit.YVar(self.ws[self.target_names[0]]))
        self.ws.factory('prod::scaled_chisq_func(chisq_scale[1.],chisq_func)')
        self._is_built = True

    def predict(self, X):
        """Predict values for given samples of features."""
        raise NotImplementedError('LinearRegression.predict not implemented yet')

    def score(self, X, y):
        """Calculate R^2 coefficient for specified samples."""
        raise NotImplementedError('LinearRegression.score not implemented yet')
