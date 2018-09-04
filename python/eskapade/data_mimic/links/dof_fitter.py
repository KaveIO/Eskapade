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
from eskapade.data_mimic.data_mimic_util import sample_chi2, generate_data


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
        self._process_kwargs(kwargs, n_obs=100000, p_ordered=None, p_unordered=None,
                             means_stds=None, continuous_columns=None, string_columns=None, maps_read_key=None,
                             new_column_order_read_key=None, bins=None, dof_store_key=None)

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
        maps = ds[self.maps_read_key]

        df = generate_data(self.n_obs, self.p_unordered, self.p_ordered, self.means_stds)

        for c in self.continuous_columns:
            df[c] = df[c].astype(np.float)

        for c in self.string_columns:
            m = maps[c]
            df[c] = df[c].map(m)

        new_column_order = ds[self.new_column_order_read_key]
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
                                                             continuous_columns=self.continuous_columns,
                                                             string_columns=self.string_columns,
                                                             new_column_order=new_column_order,
                                                             maps=maps), [data_binned] * 100), 1):
            chi2s.append(chi2)
            sys.stdout.write('\r{0:%} done with sampling chi2s'.format(i / 100))
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
