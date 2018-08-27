"""Project: Eskapade - A python-based package for data analysis.

Class: KernelDensityEstimation

Created: 2018-07-18

Description:
    Algorithm to ...(fill in one-liner here)

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
from statsmodels.nonparametric.kernel_density import KDEMultivariate

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode


class KernelDensityEstimation(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'KernelDensityEstimation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, data_no_nans_read_key=None, data_normalized_read_key=None, store_key=None)

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
        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        unordered_categorical_i = ds['unordered_categorical_i']
        ordered_categorical_i = ds['ordered_categorical_i']
        continuous_i = ds['continuous_i']

        data_no_nans = ds[self.data_no_nans_read_key]
        data_normalized = ds[self.data_normalized_read_key]

        # Concatenate normalized data with categorical data
        d = np.concatenate((data_no_nans[:, unordered_categorical_i],
                            data_no_nans[:, ordered_categorical_i], data_normalized),
                           axis=1)

        var_type = 'u' * len(unordered_categorical_i) + 'o' * len(ordered_categorical_i) + \
                   'c' * len(continuous_i)

        # LET OP! statsmodels uses normal reference for unordered categorical variables as well!
        kde = KDEMultivariate(d, var_type=var_type, bw='normal_reference')

        ds[self.store_key] = kde.bw

        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
