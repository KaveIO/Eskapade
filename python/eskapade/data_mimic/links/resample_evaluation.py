"""Project: Eskapade - A python-based package for data analysis.

Class: ResampleEvaluation

Created: 2018-07-18

Description:
    Algorithm to ...(fill in one-liner here)

    TODO: write good summary with explanation of choices made

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
import numpy as np
import scipy

from eskapade import process_manager, DataStore, Link, StatusCode


class ResampleEvaluation(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ResampleEvaluation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, data_read_key=None, resample_read_key=None, bins=None, n_bins=None,
                             chi2_store_key=None, p_value_store_key=None, dof_read_key=None)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off the line above, and on the line below if you wish to keep these extra kwargs.
        # self._process_kwargs(kwargs)

    def initialize(self):
        """Initialize the link.

        :returns: status code of initialization
        :rtype: StatusCode
        """
        return StatusCode.Success

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        ds = process_manager.service(DataStore)

        data = ds[self.data_read_key]
        resample = ds[self.resample_read_key]

        resample_binned = np.histogramdd(resample, bins=self.bins)
        data_binned = np.histogramdd(data, bins=self.bins)

        if self.dof_read_key is None:
            dof = 2*self.n_bins  # times two because of the reference (simulated) has a DoF per bin as well
        else:
            self.logger.info('Using DoF from DataStore')
            dof = ds[self.dof_read_key]
        ddof = self.n_bins - 1 - dof  # see the docs for ddof from scipy.stats.chisquare
        chi2, p_value = scipy.stats.chisquare(resample_binned[0].flatten(), data_binned[0].flatten(), ddof=ddof)

        self.logger.info('CHI2: {}'.format(chi2))
        self.logger.info('P value: {}'.format(p_value))

        ds[self.chi2_store_key] = chi2
        ds[self.p_value_store_key] = p_value

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
