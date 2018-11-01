"""Project: Eskapade - A python-based package for data analysis.

Class: KDEPreparation

Created: 2018-07-18

Description:
    Algorithm to prepare a pandas dataframe for kernel density estimation.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import numpy as np
import pandas as pd

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapade.analysis.correlation import calculate_correlations
from eskapade.data_mimic.data_mimic_util import find_peaks, smooth_peaks, remove_nans, append_extremes, \
                                                transform_to_normal


class KDEPreparation(Link):
    """
    Prepares a pandas dataframe for kernel density estimation:
    - peaks are smoothed, i.e., changed to a gaussian distribution with a relative small standard deviation
    - rows including nan's are removed
    - continuous columns are transformed to a normal distribution

    The continuous columns are transformed to a normal distribution because we want to make use of the
    (multi-dimensional) normal rule of thumb for kernel density estimation. The implementation of statsmodels using
    least squares or maximum likelihood cross validation is too slow for a data set of practical size.
    We are working on an implementation for least squares cross validation that is significant faster then the current
    implementation in statsmodels for categorical observables.

    Extremes are added before the transformation to a normal distribution and removed afterwards. This is done
    because the extremes make sure faulty edge effects are excluded in the transformation.

    Data flow:
    1. change column order (unordered categorical, ordered categorical, continuous) on df_to_resample -> data
    2. smooth_peaks() on data -> data_smoothed
    3. remove_nans() on data_smoothed -> data_no_nans
    4. select only continuous columns from data_no_nans -> data_continuous
        + 4b append_extremes() on data_continuous -> data_extremes (contains two data points extra, the extremes)
        + 4c transform_to_normal() on data_extremes -> data_normalized. Extremes are deleted from data_normalized.
    """

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str data_store_key: key of output data to store in data store
        :param str data_smoothed_store_key: key of data_smoothed to store in data store
        :param str data_no_nans_store_key: key of data_no_nans to store in data store
        :param str maps_store_key: key of strings-to-integer maps (dicts) per string column to store in data store
        :param str qts_store_key: key of a list of trained sklearn.preprocessing.QuantileTransformation instances per
                                  continuous columns to store in data store
        :param str new_column_order_store_key: key of the new column order to store in data store
        :param str ids_store_key: key of the original indices to store in data store
        :param list unordered_categorical_columns: the column names of the unordered categorical columns of the input
                                                   dataframe
        :param list ordered_categorical_columns: the column names of the ordered categorical columns of the input
                                                 dataframe
        :param list continuous_columns: the column names of the continuous columns of the input dataframe
        :param list string_columns: the column names of the string columns of the input dataframe
        :param int count: parameter used for finding peaks. See eskapade.data_mimic.data_mimic_util.find_peaks
        :param float extremes_fraction: parameter to calculate the extremes. See
                                        eskapade.data_mimic.data_mimic_util.make_extremes
        :param float smoothing_fraction: parameter to calculate the standard deviation used for peak smoothing. See
                                         eskapade.data_mimic.data_mimic_util.smooth_peaks
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'KDEPreparation'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs,
                             read_key=None,
                             correlation_method='pearson',
                             data_store_key=None,
                             data_smoothed_store_key=None,
                             data_no_nans_store_key=None,
                             data_normalized_store_key=None,
                             maps_store_key=None,
                             qts_store_key=None,
                             new_column_order_store_key=None,
                             ids_store_key=None,
                             unordered_categorical_columns=[],
                             ordered_categorical_columns=[],
                             continuous_columns=[],
                             string_columns=None,
                             count=1,
                             extremes_fraction=0.15,
                             smoothing_fraction=0.0002,
                             input_maps=None)

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
        self.logger.debug('Now executing link: {link}.', link=self.name)

        ds = process_manager.service(DataStore)

        # -- sg: added copy, or it would replace original data in datastore
        df_to_resample = ds[self.read_key].copy()
        ds[self.ids_store_key] = df_to_resample.index.values  # save for later use

        # check for high (>.95) correlations
        cors, cols = calculate_correlations(df_to_resample[self.unordered_categorical_columns +
                                                           self.ordered_categorical_columns + \
                                                           self.continuous_columns], method=self.correlation_method)
        mask = np.ones(cors.shape, dtype='bool')
        mask[np.triu_indices(cors.shape[0], m=cors.shape[1])] = False
        cors = cors.mask(~mask).stack().reset_index()
        cors.columns = ['x', 'y', 'cor']
        for i, row in cors[cors['cor'] > 0.95].iterrows():
            self.logger.warning('The {} correlation between {} and {} is {}. Maybe you want to discard one of these '
                                'columns in the resampling step and construct it afterwards from the resampled data.'
                .format(self.correlation_method, row.x, row.y, row.cor))

        # map the string columns
        if not self.input_maps:
            maps = {}
            for c in self.string_columns:
                m = pd.Series(range(0, len(df_to_resample[c].dropna().unique())),
                              index=df_to_resample[c].dropna().unique())
                maps[c] = m
                df_to_resample[c] = df_to_resample[c].map(m)
        else:
            maps = self.input_maps
            for c in self.string_columns:
                m = maps[c]
                df_to_resample[c] = df_to_resample[c].map(m)

        assert len(maps.keys()) == len(self.string_columns), "Wrong number of maps!"


        # unused columns are now None, this is not going to work when adding lists. We make them lists instead of
        # extensive if/else usage.
        # re order columns and save new column order for later use
        if self.unordered_categorical_columns is None:
            self.unordered_categorical_columns = []
        if self.ordered_categorical_columns is None:
            self.ordered_categorical_columns = []
        if self.continuous_columns is None:
            self.continuous_columns = []

        new_column_order = self.unordered_categorical_columns + self.ordered_categorical_columns + \
                           self.continuous_columns
        data = df_to_resample[new_column_order].values.copy()
        unordered_categorical_i = [new_column_order.index(c) for c in self.unordered_categorical_columns]
        ordered_categorical_i = [new_column_order.index(c) for c in self.ordered_categorical_columns]
        continuous_i = [new_column_order.index(c) for c in self.continuous_columns]

        # find peaks and smooth continuous variables
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

        ds[self.data_smoothed_store_key] = data_smoothed
        ds[self.data_no_nans_store_key] = data_no_nans
        ds[self.data_normalized_store_key] = data_normalized
        ds[self.qts_store_key] = qts
        ds[self.maps_store_key] = maps
        ds[self.new_column_order_store_key] = new_column_order
        ds[self.data_store_key] = data

        # save for later use
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
