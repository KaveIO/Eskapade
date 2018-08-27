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
import pandas as pd
import string

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode
from eskapade.data_mimic.data_mimic_util import generate_unordered_categorical_random_data, \
    generate_ordered_categorical_random_data, generate_continuous_random_data

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
        self._process_kwargs(kwargs, read_key=None, store_key=None, n_obs=100000, p_ordered=None, p_unordered=None,
                             means_stds=None)

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

        unordered_categorical_data = generate_unordered_categorical_random_data(self.n_obs, self.p_unordered)
        ordered_categorical_data = generate_ordered_categorical_random_data(self.n_obs, self.p_ordered)
        continuous_data = generate_continuous_random_data(self.n_obs, self.means_stds)

        df = pd.DataFrame(np.concatenate((continuous_data,
                                          unordered_categorical_data,
                                          ordered_categorical_data), axis=1))
        alphabet = np.array(list(string.ascii_lowercase))
        df.columns = alphabet[0:df.shape[1]]

        # simulate heaping
        df.loc[np.random.randint(0, 100000, size=3000), 'a'] = np.ones(3000) * 35.1

        # simulate nans
        df.loc[np.random.randint(0, 100000, size=2000), 'b'] = np.ones(2000) * np.nan
        df.loc[np.random.randint(0, 100000, size=2000), 'f'] = np.ones(2000) * np.nan

        ds[self.store_key] = df

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
