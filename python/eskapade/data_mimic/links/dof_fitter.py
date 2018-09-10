"""Project: Eskapade - A python-based package for data analysis.

Class: DoFFitter

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
import scipy
import multiprocessing as mp
import sys
from functools import partial

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.data_mimic.data_mimic_util import generate_data


class DoFFitter(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'DoFFitter'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, n_obs=100000, p_ordered=None, p_unordered=None, means_stds=None,
                             bins=None, n_chi2_samples=10000, dof_store_key=None)

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

        df = generate_data(self.n_obs, self.p_unordered, self.p_ordered, self.means_stds,
                           dtype_unordered_categorical_data=np.int)

        unordered_categorical_columns = ['d', 'e']
        ordered_categorical_columns = ['f', 'g']
        continuous_columns = ['a', 'b', 'c']
        new_column_order = unordered_categorical_columns + ordered_categorical_columns + continuous_columns
        data = df[new_column_order].values.copy()
        data_binned = np.histogramdd(data, bins=self.bins)

        chi2s = []
        pool = mp.Pool(processes=(mp.cpu_count() - 1))
        for i, chi2 in enumerate(pool.imap_unordered(partial(sample_chi2,
                                                             n_obs=self.n_obs,
                                                             p_unordered=self.p_unordered,
                                                             p_ordered=self.p_ordered,
                                                             means_stds=self.means_stds,
                                                             bins=self.bins,
                                                             new_column_order=new_column_order),
                                                     [data_binned] * self.n_chi2_samples), 1):
            chi2s.append(chi2)
            sys.stdout.write('\r{0:%} done with sampling chi2s'.format(i / self.n_chi2_samples))
        pool.close()
        pool.join()

        dof, loc, scale = scipy.stats.chi2.fit(chi2s, floc=0, fscale=1)
        self.logger.info('Fitted DoF: {}'.format(dof))

        ds[self.dof_store_key] = dof

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success


def sample_chi2(data_binned, n_obs, p_unordered, p_ordered, means_stds, bins, new_column_order):
    df = generate_data(n_obs, p_unordered, p_ordered, means_stds, dtype_unordered_categorical_data=np.int)

    sample = df[new_column_order].values.copy()
    sample_binned = np.histogramdd(sample, bins=bins)
    chi2 = scipy.stats.chisquare(sample_binned[0].flatten(), data_binned[0].flatten())[0]
    return chi2
