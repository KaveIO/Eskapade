"""Project: Eskapade - A python-based package for data analysis.

Macro: esk702_mimic_data_only_unordered

Created: 2018/09/04

Description:
    This macro illustrates how to resample an existing data set, containing only unordered catagorical data,
    using kernel density estimation (KDE) and a direct resampling technique. First, a data set is simulated
    containing only unordered catagorical variables. This data sets represents a general input data set.
    Then the dataframe KDE is applied on a processed dataframe resulting in kernel bandwiths per dimension.
    These bandwiths are used to resample a new data set using the existing input data set.

    In the resampling step direct resampling is used, i.e., using an existing data point to define a kernel
    around it and sample a new data point from that kernel.

    Data flow description:
    1. KDEMultivariate() on ddata -> bw (bandwiths)
    2. kde_resample() on data_to_resample -> resample_data

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
logger.debug('Now parsing configuration file esk703_mimic_data')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk703_mimic_data'
settings['version'] = 0

np.random.seed(42)

ch = Chain('DataPrep')
ch.logger.log_level = LogLevel.DEBUG

sim_data = data_mimic.MixedVariablesSimulation(store_key='df',
                                               n_obs=100000,
                                               p_unordered=np.array([[0.2, 0.2, 0.3, 0.3], [0.3, 0.7]]))

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
                                     unordered_categorical_columns=['a', 'b'],
                                     string_columns=['a', 'b'],
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

ch=Chain('temp')

bins = [np.array([-10, 1.5, 10]), np.array([-10, 0.5, 10])]
evaluater = data_mimic.ResampleEvaluation(data_read_key='data',
                                          resample_read_key='data_resample',
                                          bins=bins,
                                          chi2_store_key='chi2',
                                          p_value_store_key='p_value',
                                          new_column_order_read_key='new_column_order',
                                          ks_store_key='kss',
                                          chis_store_key='chis',
                                          distance_store_key='distance',
                                          df_resample_read_key='df_resample')
evaluater.logger.log_level = LogLevel.DEBUG
ch.add(evaluater)

ch = Chain('report')

report = data_mimic.MimicReport(read_key='df',
                                resample_read_key='df_resample',
                                new_column_order_read_key='new_column_order',
                                unordered_categorical_columns=['a', 'b'],
                                ordered_categorical_columns=[],
                                continuous_columns=[],
                                string_columns=['a', 'b'],
                                business_rules_columns=[],
                                chi2_read_key='chi2',
                                p_value_read_key='p_value',
                                key_data_normalized='data_normalized',
                                distance_read_key='distance'
                                )
report.logger.log_level = LogLevel.DEBUG
ch.add(report)

logger.debug('Done parsing configuration file esk703_mimic_data')
