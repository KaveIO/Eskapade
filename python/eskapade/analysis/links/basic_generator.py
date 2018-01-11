"""Project: Eskapade - A python-based package for data analysis.

Class: BasicGenerator

Created: 2017/02/26

Description:
    Link to generate random data with basic distributions

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class BasicGenerator(Link):
    """Generate data with basic distributions."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str key: key of output data in data store
        :param list columns: output column names
        :param int size: number of variable values
        :param dict gen_config: generator configuration for each variable
        :param int gen_seed: generator random seed
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'basic_generator'))

        # process keyword arguments
        self._process_kwargs(kwargs, key='', columns=None, size=1, gen_config=None, gen_seed=1)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # check input arguments
        self.check_arg_vals('key', 'size', 'columns')
        self.check_arg_types(key=str, size=int, gen_seed=int)
        self.check_arg_types(recurse=True, allow_none=True, columns=str, gen_config=str)

        # set generator seed
        np.random.seed(self.gen_seed)

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        # generate data
        self.logger.debug('Generating {n:d} rows for columns [{columns}].',
                          n=self.size, columns=', '.join('"{}"'.format(c) for c in self.columns))
        data = {}
        for col in self.columns:
            # get generator configuration for this variable
            conf = self.gen_config.get(col, {}) if self.gen_config else {}
            mu = conf.get('mean', 0.)
            sigma = conf.get('std', 1.)
            dtype = conf.get('dtype', float)
            choice = conf.get('choice', ['a', 'b', 'c'])
            choice_prob = conf.get('choice_prob', None)

            # generate
            if dtype == str:
                data[col] = np.random.choice(choice, size=self.size, p=choice_prob)
            else:
                data[col] = np.random.normal(loc=mu, scale=sigma, size=self.size).astype(dtype)

        # create data frame
        process_manager.service(DataStore)[self.key] = pd.DataFrame(data=data, columns=self.columns)

        return StatusCode.Success
