"""Project: Eskapade - A python-based package for data analysis.

Macro: esk201_readdata

Created: 2018/09/04

Description:
    Macro to that illustrates how to open files as pandas datasets.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
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
                                               means_stds=np.array([[8, 8, 3], [2, 5, 2]]))
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

resampler = data_mimic.Resampler(data_smoothed_read_key='data_smoothed',
                                 data_normalized_read_key='data_normalized',
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

# The number of DoF is equal to two times the number of bins because the reference (data_to_resample)
# has a DoF per bin as well, see the esk702 tutorial
bins = [np.array([-10, 1.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 1.5, 10]),
        np.array([-100, 0, 100]), np.array([-100, 0, 100]), np.array([-100, 0, 100])]
evaluater = data_mimic.ResampleEvaluation(data_read_key='data',
                                          resample_read_key='data_resample',
                                          bins=bins,
                                          n_bins=2**7,
                                          chi2_store_key='chi2', p_value_store_key='p_value')
evaluater.logger.log_level = LogLevel.DEBUG
ch.add(evaluater)

logger.debug('Done parsing configuration file esk701_mimic_data')
