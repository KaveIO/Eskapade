"""Project: Eskapade - A python-based package for data analysis.

Class: ResampleEvaluation

Created: 2018-07-18

Description:
    Algorithm to evaluate the statistical simularity between 2 data sets using a chiˆ2 test.

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
    """
    Evaluates the statistical simularity between 2 (multi-dimensional) data sets using a chiˆ2 test. The 2 data sets
    are binned and the chi^2 statistic is calculated using the counts per bin. To calculate the p-value, the number
    of degrees of freedom (DoF) is set equal to 2 times the number of bins because data is not compared with a fixed
    model but with another (reference/input) data set contributing with a degree of freedom per bin as well.

    Usually, DoF = number of bins - number of model parameters. In the case of comparing two data sets, the number of
    model parameters is not known. Therefore, the number of model parameters is ignored for now.
    """

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str data_read_key: key of input data to read from data store
        :param str resample_read_key: key of resampled data to read from data store
        :param sequence or int bins: Specification:
                                     * A sequence of arrays describing the bin edges along each dimension
                                     * The number of bins for each dimension (nx, ny, ... =bins)
                                     * The number of bins for all dimensions (nx=ny=...=bins)
        :param int n_bins: number of total bins (for all dimensions)
        :param str chi2_store_key: key of chiˆ2 value to store in data store
        :param str p_value_store_key: key of p-value to store in data store
        :param str dof_read_key: key of DoF to read from data store. If None, DoF is set to 2 * n_bins
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ResampleEvaluation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs,
                             data_read_key=None,
                             resample_read_key=None,
                             bins=None,
                             n_bins=None,
                             chi2_store_key=None,
                             p_value_store_key=None,
                             dof_read_key=None)

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

        # dof is only needed to calculate the p-value, not for chiˆ2
        if self.dof_read_key is None:
            # todo:
            # DoF = 2*number of bins - number of model parameters. Check if (unknown) number of model parameters is
            # relevant.
            # Times 2 because of the reference has a DoF per bin as well.
            dof = 2*self.n_bins
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
