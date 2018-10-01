"""Project: Eskapade - A python-based package for data analysis.

Class: Resampler

Created: 2018-07-18

Description:
    Algorithm to ...(fill in one-liner here)

    TODO: write good summary with explanation of choices made

    Data flow:
    6. insert_back_nans() on data_smoothed, data_normalized and data -> data_to_resample. Data_smoothed is used to
       determine the original index of the nans for the continuous columns. Data_normalized is used to insert the
       non-nans for the continuous columns. We want to use data_normalized because we want to resample in the
       transformed space because the bandwiths are determined in the transformed space. Data is used to insert to
       the nans and non-nans for the categorical column.
    7. kde_resample() on data_to_resample -> resample_normalized_unscaled
    8. scale_and_invert_normal_transformation() on resample_normalized_unscaled -> resample

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.data_mimic.data_mimic_util import insert_back_nans, kde_resample, scale_and_invert_normal_transformation


class Resampler(Link):

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
        self._process_kwargs(kwargs,
                             data_normalized_read_key=None,
                             data_read_key=None,
                             bws_read_key=None,
                             qts_read_key=None,
                             new_column_order_read_key=None,
                             maps_read_key=None,
                             ids_read_key=None,
                             df_resample_store_key=None,
                             resample_store_key=None)

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

        data_normalized = ds[self.data_normalized_read_key]
        data = ds[self.data_read_key]
        band_widths = ds[self.bws_read_key]
        qts = ds[self.qts_read_key]
        new_column_order = ds[self.new_column_order_read_key]
        maps = ds[self.maps_read_key]
        ids = ds[self.ids_read_key]

        # because the bandwiths are determined on the normalized continuous data and on the original categorical data,
        # resampling is done with the input data in the same state.
        data_to_resample = insert_back_nans(data_normalized, data, unordered_categorical_i,
                                            ordered_categorical_i, continuous_i)

        c_array = []  # list containg all possible categories per u dimension
        for d in unordered_categorical_i:
            c_array.append(np.unique(data[:, d]))
        c_array = np.array(c_array)
        var_type = 'u' * len(unordered_categorical_i) + 'o' * len(ordered_categorical_i) + \
                   'c' * len(continuous_i)
        n_resample = len(data_to_resample)
        resample_normalized_unscaled, indices = kde_resample(n_resample, data_to_resample, band_widths, var_type,
                                                             c_array)

        resample = scale_and_invert_normal_transformation(resample_normalized_unscaled, continuous_i, qts)

        df_resample = pd.DataFrame(resample, columns=new_column_order).copy()
        df_resample['ID'] = ids[indices]

        for c, m in maps.items():
            inv_m = {v: k for k, v in m.items()}
            df_resample[c] = df_resample[c].map(inv_m)

        ds[self.resample_store_key] = resample
        ds[self.df_resample_store_key] = df_resample

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
