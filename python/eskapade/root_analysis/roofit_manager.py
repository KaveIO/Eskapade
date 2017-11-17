"""Project: Eskapade - A Python-based package for data analysis.

Module: root_analysis.roofit_manager

Created: 2017/04/24

Description:
    RooFit-manager process service for Eskapade run

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import ROOT

from eskapade.core.process_services import ProcessService
from eskapade.root_analysis.roofit_models import RooFitModel


class RooFitManager(ProcessService):
    """Process service for managing RooFit operations."""

    _persist = True

    def __init__(self):
        """Initialize RooFit manager instance."""
        self._ws = None
        self._models = {}

    @property
    def ws(self):  # noqa
        """RooFit workspace for Eskapade run."""
        if not self._ws:
            self._ws = ROOT.RooWorkspace('esws', 'Eskapade workspace')
            ROOT.SetOwnership(self._ws, False)

        return self._ws

    def delete_workspace(self):
        """Delete existing workspace."""
        if self._ws:
            del self._ws
            self._ws = None

    def set_var_vals(self, vals):
        """Set values of workspace variables.

        :param dict vals: values and errors to set: {name1: (val1, err1), name2: (val2, err2), ...}
        """
        for var, (val, err) in vals.items():
            self.ws.var(var).setVal(val)
            self.ws.var(var).setError(err)

    def model(self, name, model_cls=None, **kwargs):
        """Get RooFit model.

        Return the RooFit model with the specified name.  Create the model if it
        does not yet exist.  Arguments and key-word arguments are passed on to
        the model when it is initialized.

        :param str name: name of the model
        :param model_cls: model class; must inherit from RooFitModel
        """
        # check name
        name = str(name) if name else ''
        if not name:
            raise ValueError('no valid model name specified')

        # create model if it does not exist and class is specified
        if name not in self._models:
            # check if model should be created
            if not model_cls:
                return None

            # create model
            model = model_cls(self.ws, name, **kwargs)

            # check model type
            if not isinstance(model, RooFitModel):
                raise TypeError('specified model is not a RooFitModel')

            self._models[name] = model

        return self._models[name]
