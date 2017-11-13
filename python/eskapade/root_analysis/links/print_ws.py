"""Project: Eskapade - A python-based package for data analysis.

Class: PrintWs

Created: 2017/03/27

Description:
    Algorithm to print the contents of the rooworkspace

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager
from eskapade.root_analysis.roofit_manager import RooFitManager


class PrintWs(Link):
    """Print the contents of the RooFit workspace."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'PrintWs'))

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute the link."""
        ws = process_manager.service(RooFitManager).ws

        ws.Print('v')

        return StatusCode.Success
