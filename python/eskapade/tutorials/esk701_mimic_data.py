"""Project: Eskapade - A python-based package for data analysis.

Macro: esk201_readdata

Created: 2018/09/04

Description:
    This macro illustrates how to resample an existing data set, containing mixed data types, using kernel density
    estimation (KDE) and a direct resampling technique. First, a data set is simulated containing mixed data types.
    This data sets represents a general input data set. Then the dataframe KDE is applied on a processed dataframe
    resulting in kernel bandwiths per dimension. These bandwiths are used to resample a new data set using the
    existing input data set.

    For now, only the normal rule of thumb is implemented using the statsmodels implementation because the
    implementation of statsmodels using least squares or maximum likelihood cross validation is too slow for a data
    set of practical size. We are working on an implementation for least squares cross validation that is significant
    faster then the current implementation in statsmodels for categorical observables.

    In the resampling step it is chosen to use direct resampling, i.e., using an existing data point to define a kernel
    around it and sample a new data point from that kernel. Direct resampling is chosen because it is a
    straightforward technique.
    Another technique would be to define or describe (binned) the entire multidimensional distribution (the sum of
    the kernels of all original data points) and sample from that distribution. This is, however, not straightforward
    to do because such a distribution could take up a lot of memory. Maybe it is possible to define such a
    distribution sparsely.

    Data flow description:
    1. change column order (unordered categorical, ordered categorical, continuous) on df_to_resample -> data
    2. smooth_peaks() on data -> data_smoothed
    3. remove_nans() on data_smoothed -> data_no_nans
    4. select only continuous columns from data_no_nans -> data_continuous
        + 4b append_extremes() on data_continuous -> data_extremes (contains two data points extra, the extremes)
        + 4c transform_to_normal() on data_extremes -> data_normalized. Extremes are deleted from data_normalized.
    5. concatenation of data_no_nans (unordered categorical and ordered categorical) and data_normalized (only
       continuous) -> d
        + 5b KDEMultivariate() on d -> bw (bandwiths)
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

# todo:
# - add mirroring in resampling
# - use faster implementation of least squares cross validation
# - add binning and/or taylor expansion option for continuous columns
# - add business rules
# - save (sparse binned) PDF to resample (not direct resampling)

import numpy as np

from eskapade import ConfigObject, Chain
from eskapade import data_mimic
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel


logger = Logger()
logger.debug('Now parsing configuration file esk701_mimic_data')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk701_mimic_data'
settings['version'] = 0

np.random.seed(42)

ch = Chain('DataPrep')
ch.logger.log_level = LogLevel.DEBUG

sim_data = data_mimic.MixedVariablesSimulation(store_key='df',
                                               n_obs=100000,
                                               p_unordered=np.array([[0.2, 0.2, 0.3, 0.3], [0.3, 0.7]]),
                                               p_ordered=np.array([[0.1, 0.2, 0.7], [0.15, 0.4, 0.05, 0.3, 0.1]]),
                                               means_stds=np.array([[8, 8, 3], [2, 5, 2]]),
                                               heaping_values=[35.1],
                                               heaping_columns=['a'],
                                               heaping_sizes=[3000],
                                               nan_sizes=[2000, 2000],
                                               nan_columns=['b', 'f'])
sim_data.logger.log_level = LogLevel.DEBUG
ch.add(sim_data)

pre_data = data_mimic.KDEPreparation(read_key='df',
                                     data_store_key='data',
                                     data_smoothed_store_key='data_smoothed',
                                     data_no_nans_store_key='data_no_nans',
                                     data_normalized_store_key='data_normalized',
                                     maps_store_key='maps',
                                     qts_store_key='qts',
                                     new_column_order_store_key='new_column_order',
                                     ids_store_key='ids',
                                     unordered_categorical_columns=['d', 'e'],
                                     ordered_categorical_columns=['f', 'g'],
                                     continuous_columns=['a', 'b', 'c'],
                                     string_columns=['d', 'e'],
                                     count=1,
                                     extremes_fraction=0.15,
                                     smoothing_fraction=0.0002)
pre_data.logger.log_level = LogLevel.DEBUG
ch.add(pre_data)

ch = Chain('KDE')

kde = data_mimic.KernelDensityEstimation(data_no_nans_read_key='data_no_nans',
                                         data_normalized_read_key='data_normalized',
                                         store_key='bw')
kde.logger.log_level = LogLevel.DEBUG
ch.add(kde)

resampler = data_mimic.Resampler(data_normalized_read_key='data_normalized',
                                 data_read_key='data',
                                 bws_read_key='bw',
                                 qts_read_key='qts',
                                 new_column_order_read_key='new_column_order',
                                 maps_read_key='maps',
                                 ids_read_key='ids',
                                 df_resample_store_key='df_resample',
                                 resample_store_key='data_resample')
resampler.logger.log_level = LogLevel.DEBUG
ch.add(resampler)

# Usually, DoF = number of bins - number of model parameters. However, in this case, DoF is equal to 2 * the
# number of bins - number of model parameters. Two times the number of bins the reference (data_to_resample) has a
# DoF per bin as well.
bins = [np.array([-10, 1.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 1.5, 10]),
        np.array([-100, 0, 100]), np.array([-100, 0, 100]), np.array([-100, 0, 100])]
evaluater = data_mimic.ResampleEvaluation(data_read_key='data',
                                          resample_read_key='data_resample',
                                          bins=bins,
                                          n_bins=2**7,
                                          chi2_store_key='chi2',
                                          p_value_store_key='p_value',
                                          new_column_order_read_key='new_column_order',
                                          ks_store_key='kss',
                                          chis_store_key='chis',
                                          distance_store_key='distance')
evaluater.logger.log_level = LogLevel.DEBUG
ch.add(evaluater)

ch = Chain('report')

report = data_mimic.MimicReport(read_key='df',
                                resample_read_key='df_resample',
                                new_column_order_read_key='new_column_order',
                                chi2_read_key='chi2',
                                p_value_read_key='p_value',
                                maps_read_key='maps',
                                key_data_normalized='data_normalized',
                                distance_read_key='distance'
                                )
report.logger.log_level = LogLevel.DEBUG
ch.add(report)

logger.debug('Done parsing configuration file esk701_mimic_data')
