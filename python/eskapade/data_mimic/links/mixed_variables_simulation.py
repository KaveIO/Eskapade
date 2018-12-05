"""Project: Eskapade - A python-based package for data analysis.

Class: MixedVariablesSimulation

Created: 2018-07-18

Description:
    Algorithm to generate a pandas dataframe containing columns with unordered categorical, ordered categorical and
    continuous data types.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.data_mimic.data_mimic_util import generate_data


class MixedVariablesSimulation(Link):
    """
    Generates a pandas dataframe containing columns with unordered categorical, ordered categorical and continuous
    data types. The column names are alphabetically ascending starting with the continuous columns, then the unordered
    categorical columns and finally the ordered categorical columns.
    It is possible to include heaping effects and np.nan's.
    """

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str store_key: key of output data to store in data store
        :param int n_obs: the number of rows to generate
        :param np.2darray p_ordered: The probabilities associated with each category per dimension. The length of
                                     p_ordered determines the number of dimensions, the length of the j-th element of
                                     p_ordered is the number of categories for dimension j and p_ordered[j] are the
                                     probabilities for the categories of dimension j.
        :param np.2darray p_unordered: The probabilities associated with each category per dimension. The length of
                                       p_unordered determines the number of dimensions, the length of the j-th element
                                       of p_unordered is the number of categories for dimension j and p_unordered[j]
                                       are the probabilities for the categories of dimension j.
        :param np.2darray means_stds: The length of means_stds determines the number of dimensions. means_stds[0]
                                      are the means for each dimension. means_stds[1] are the standard deviations for
                                      each dimension.
        :param list heaping_columns: The columns to include heaping effects.
        :param list heaping_values: The value where the heaping will be simulated (mean of the heap). The length
                                    should be equal to the length of heaping_columns.
        :param list heaping_sizes: The size of the heap. The length should be equal to the length of heaping_columns.
        :param list nan_columns: The columns to include np.nan's.
        :param list nan_sizes: The size (number) of np.nan's to include per column.
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'MixedVariablesSimulation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs,
                             store_key=None,
                             n_obs=100000,
                             p_ordered=None,
                             p_unordered=None,
                             means_stds=None,
                             heaping_values=None,
                             heaping_columns=None,
                             heaping_sizes=None,
                             nan_sizes=None,
                             nan_columns=None)

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

        df = generate_data(self.n_obs, self.p_unordered, self.p_ordered, self.means_stds)

        # simulate heaping
        if self.heaping_columns is not None:
            for i, heaping_column in enumerate(self.heaping_columns):
                heaping_size = self.heaping_sizes[i]
                heaping_value = self.heaping_values[i]
                df.loc[np.random.randint(0, self.n_obs, size=heaping_size), heaping_column] = \
                    np.ones(3000) * heaping_value

        # simulate nans
        if self.nan_columns is not None:
            for i, nan_column in enumerate(self.nan_columns):
                nan_size = self.nan_sizes[i]
                df.loc[np.random.randint(0, self.n_obs, size=nan_size), nan_column] = np.ones(nan_size) * np.nan

        ds[self.store_key] = df

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
