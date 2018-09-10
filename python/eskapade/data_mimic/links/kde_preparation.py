"""Project: Eskapade - A python-based package for data analysis.

Class: KDEPreparation

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

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.data_mimic.data_mimic_util import find_peaks, smooth_peaks, remove_nans, append_extremes, \
                                                transform_to_normal


class KDEPreparation(Link):

    """Defines the content of link."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'KDEPreparation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, read_key=None, data_store_key=None, data_smoothed_store_key=None,
                             data_no_nans_store_key=None, data_normalized_store_key=None, maps_store_key=None,
                             qts_store_key=None, new_column_order_store_key=None, ids_store_key=None,
                             unordered_categorical_columns=None, ordered_categorical_columns=None,
                             continuous_columns=None, string_columns=None, count=1, extremes_fraction=0.15,
                             smoothing_fraction=0.0002)

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

        df_to_resample = ds[self.read_key]
        ds[self.ids_store_key] = df_to_resample.index.values

        # map the string columns
        maps = {}
        for c in self.string_columns:
            m = pd.Series(range(0, len(df_to_resample[c].dropna().unique())),
                          index=df_to_resample[c].dropna().unique())
            maps[c] = m
            df_to_resample[c] = df_to_resample[c].map(m)

        # re order columns and save new column order for later use
        new_column_order = self.unordered_categorical_columns + self.ordered_categorical_columns + \
                           self.continuous_columns
        data = df_to_resample[new_column_order].values.copy()
        unordered_categorical_i = [new_column_order.index(c) for c in self.unordered_categorical_columns]
        ordered_categorical_i = [new_column_order.index(c) for c in self.ordered_categorical_columns]
        continuous_i = [new_column_order.index(c) for c in self.continuous_columns]

        peaks = find_peaks(data, continuous_i, count=self.count)
        data_smoothed = smooth_peaks(data, peaks, smoothing_fraction=self.smoothing_fraction)
        # remove nans
        data_no_nans = remove_nans(data_smoothed)
        # select continuous columns
        data_continuous = data_no_nans[:, continuous_i].copy()
        # append extremes
        data_extremes, imin, imax = append_extremes(data_continuous, self.extremes_fraction)
        # transform to normal distribution
        data_normalized, qts = transform_to_normal(data_extremes, imin, imax)

        ds[self.maps_store_key] = maps
        ds[self.new_column_order_store_key] = new_column_order
        ds[self.qts_store_key] = qts
        ds[self.data_store_key] = data
        ds[self.data_smoothed_store_key] = data_smoothed
        ds[self.data_no_nans_store_key] = data_no_nans
        ds[self.data_normalized_store_key] = data_normalized
        ds['unordered_categorical_i'] = unordered_categorical_i
        ds['ordered_categorical_i'] = ordered_categorical_i
        ds['continuous_i'] = continuous_i

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
