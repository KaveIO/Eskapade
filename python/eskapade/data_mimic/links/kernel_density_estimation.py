"""Project: Eskapade - A python-based package for data analysis.

Class: KernelDensityEstimation

Created: 2018-07-18

Description:
    Algorithm to execute kernel density estimation (kernel bandwith fitting) on a data set with mixed data types
    (unordered categorical, ordered categorical and continuous).

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
from statsmodels.nonparametric.kernel_density import KDEMultivariate

from eskapade.data_mimic.data_mimic_util import kde_only_unordered_categorical

from eskapade import process_manager, DataStore, Link, StatusCode


class KernelDensityEstimation(Link):
    """
    Executes kernel density estimation (kernel bandwith fitting) on a data set with mixed data types (unordered
    categorical, ordered categorical and continuous).

    For now, only the normal rule of thumb is implemented using the statsmodels implementation because the
    implementation of statsmodels using least squares or maximum likelihood cross validation is too slow for a data
    set of practical size. We are working on an implementation for least squares cross validation that is significant
    faster then the current implementation in statsmodels for categorical observables.

    Data flow:
    5. concatenation of data_no_nans (unordered categorical and ordered categorical) and data_normalized (only
       continuous) -> d
        + 5b KDEMultivariate() on d -> bw (bandwiths)
    """

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str data_no_nans_read_key: key of data_no_nans to read from data store
        :param str data_normalized_read_key: key of data_normalized to read from data store
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
        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        ds = process_manager.service(DataStore)

        unordered_categorical_i = ds['unordered_categorical_i']
        ordered_categorical_i = ds['ordered_categorical_i']
        continuous_i = ds['continuous_i']

        data_no_nans = ds[self.data_no_nans_read_key]
        data_normalized = ds[self.data_normalized_read_key]


        # Concatenate normalized data with original categorical data
        # if one of unordered_categorical_i, ordered_categorical_i, data_normalized is empty, then concatenating will
        # not work (see next line). We thus make them of the correct lenght
        data_unordered_categorical = data_no_nans[:, unordered_categorical_i]
        data_ordered_categorical = data_no_nans[:, ordered_categorical_i]

        n_obs = len(data_no_nans)

        print(type(data_normalized))
        print((data_unordered_categorical.shape,
                            data_ordered_categorical.shape, data_normalized.shape))

        if data_unordered_categorical.size == 0:
            data_unordered_categorical = np.empty(shape=(n_obs,0))
        if data_ordered_categorical.size == 0:
            data_ordered_categorical = np.empty(shape=(n_obs,0))
        if data_normalized.size == 0:
            data_normalized = np.empty(shape=(n_obs,0))

        print(type(data_normalized))
        print((data_unordered_categorical.shape,
                            data_ordered_categorical.shape, data_normalized.shape))

        d = np.concatenate((data_unordered_categorical,
                            data_ordered_categorical, data_normalized), axis=1)

        var_type = 'u' * len(unordered_categorical_i) + 'o' * len(ordered_categorical_i) + \
                   'c' * len(continuous_i)

        # NB: statsmodels uses normal reference for unordered categorical variables as well!
        # NB: the bandwiths are determined on the normalized continuous data and on the original categorical data

        if (len(continuous_i) == 0) & (len(ordered_categorical_i)==0):
            kde_weights = kde_only_unordered_categorical(d)
            ds[self.store_key] = kde_weights
        else:
            kde = KDEMultivariate(d, var_type=var_type, bw='normal_reference')
            ds[self.store_key] = kde.bw


        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
