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
import multiprocessing as mp
import sys

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
        Link.__init__(self, kwargs.pop('name', 'Resampler'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, maps_read_key=None, continuous_columns=None, bins=None, n_bins=None)

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
        ds = process_manager.service(DataStore)

        df = generate_data(100000, np.array([[0.2, 0.2, 0.3, 0.3], [0.3, 0.7]]),
                           np.array([[0.1, 0.2, 0.7], [0.15, 0.4, 0.05, 0.3, 0.1]]),
                           np.array([[8, 8, 3], [2, 5, 2]]))

        for c in self.continuous_columns:
            df[c] = df[c].astype(np.float)

        for c in self.string_columns:
            m = self.maps[c]
            df[c] = df[c].map(m)

        data = df[self.new_column_order].values.copy()
        data_binned = np.histogramdd(data, bins=[np.array([-10, 1.5, 10]),
                                                 np.array([-10, 0.5, 10]),
                                                 np.array([-10, 0.5, 10]),
                                                 np.array([-10, 1.5, 10]),
                                                 np.array([-100, 0, 100]),
                                                 np.array([-100, 0, 100]),
                                                 np.array([-100, 0, 100])])

        chi2s = []
        pool = mp.Pool(processes=(mp.cpu_count() - 1))
        for i, chi2 in enumerate(pool.imap_unordered(sample_chi2, [(data_binned, self.bins, self.continuous_columns,
                                                                    self.string_columns, self.new_column_order,
                                                                    self.maps)] * 10000), 1):
            chi2s.append(chi2)
            sys.stderr.write('\rdone {0:%}'.format(i / 10000))
        pool.close()
        pool.join()

        # todo fit chi2 distribution with dof as variable

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
