"""Project: Eskapade - A python-based package for data analysis.

Class: MixedVariablesSimulation

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

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.data_mimic.data_mimic_util import generate_data


class MixedVariablesSimulation(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'MixedVariablesSimulation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, store_key=None, n_obs=100000, p_ordered=None, p_unordered=None,
                             means_stds=None, heaping_values=None, heaping_columns=None, heaping_sizes=None,
                             nan_sizes=None, nan_columns=None)

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
        for i, heaping_column in enumerate(self.heaping_columns):
            heaping_size = self.heaping_sizes[i]
            heaping_value = self.heaping_values[i]
            df.loc[np.random.randint(0, self.n_obs, size=heaping_size), heaping_column] = \
                np.ones(3000) * heaping_value

        # simulate nans
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
