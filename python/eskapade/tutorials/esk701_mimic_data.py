"""Project: Eskapade - A python-based package for data analysis.

Macro: esk701_mimic_data

Created: 2018/09/04

Description:
    This macro illustrates how to resample an existing data set, containing mixed data types, using kernel density
    estimation (KDE) and a direct resampling technique. First, a data set is simulated containing mixed data types.
    This data sets represents a general input data set. The continuous columns are transformed to normal
    distribution using scikit learns quantile transformer. Then KDE is applied on a processed dataframe
    resulting in kernel bandwiths per dimension. These bandwiths are used to resample a new data set using the
    existing input data set.

    For now, only the normal rule of thumb is implemented using the statsmodels implementation because the
    implementation of statsmodels using least squares or maximum likelihood cross validation is too slow for a data
    set of practical size.

    In the resampling step it is chosen to use direct resampling, i.e., using an existing data point to define a kernel
    around it and sample a new data point from that kernel. Direct resampling is chosen because it is a
    straightforward technique.

    configuration settings:
    - Column names and data values can be (one way) hashed.
    - PCA can be applied on the continuous columns (after the transformation to a normal distribution) before the
      bandwith estimation. This PCA transformation preserves the original number of dimensions. Therefore,
      it only reduces the (linear) correlations between the continuous columns. This can be done to better meet the
      assumption for the normal rule of thumb (dimensions should be normally distribution and uncorrelated).
      WARNING: if one of the columns contains a NaN, the inverse pca transformation results in NaN's for all columns.

    Data flow description:
    1. change column order (unordered categorical, ordered categorical, continuous) on df_to_resample -> data
    2. smooth_peaks() on data -> data_smoothed
    3. remove_nans() on data_smoothed -> data_no_nans
    4. select only continuous columns from data_no_nans -> data_continuous
        + 4b append_extremes() on data_continuous -> data_extremes (contains two data points extra, the extremes)
        + 4c transform_to_normal() on data_extremes -> data_normalized. Extremes are deleted from data_normalized.
        + 4d pca (OPTIONAL) on data_normalized -> data_normalized_pca
    5. concatenation of data_no_nans (unordered categorical and ordered categorical) and data_normalized (only
       continuous) -> d
        + 5b KDEMultivariate() on d -> bw (bandwiths)
    6. insert_back_nans() on data_smoothed, data_normalized(_pca) and data -> data_to_resample.
       Data_smoothed is used to determine the original index of the nans for the continuous columns. Data_normalized
       is used to insert the non-nans for the continuous columns. We want to use data_normalized(_pca) because we
       want to resample in the transformed space because the bandwiths are determined in the transformed space. Data
       is used to insert to the nans and non-nans for the categorical column.
    7. kde_resample() on data_to_resample -> resample_normalized_unscaled
    8. Inverse transformations:
        + 8a inverse PCA transformation (OPTIONAL)
        + 8b scale_and_invert_normal_transformation() on resample_normalized_unscaled -> resample

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

# todo:
# - add binning and/or taylor expansion option for continuous columns

import numpy as np
import hashlib
import binascii
import os

from eskapade import ConfigObject, Chain
from eskapade import analysis
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

settings['hash_columns'] = 1
settings['hash_column_names'] = 1

settings['columns_to_hash'] = settings.get('columns_to_hash', ['d', 'g'])
settings['column_names_to_hash'] = settings.get('column_names_to_hash', ['a', 'b'])
settings['unordered_categorical_columns'] = settings.get('unordered_categorical_columns', ['d', 'e'])
settings['ordered_categorical_columns'] = settings.get('ordered_categorical_columns', ['f', 'g'])
settings['continuous_columns'] = settings.get('continuous_columns', ['a', 'b', 'c'])
settings['string_columns'] = settings.get('string_columns', ['d', 'e'])
settings['random_salt'] = settings.get('random_salt', os.urandom(256))
settings['business_rules_columns'] = settings.get('business_rules_columns', ['h'])
settings['business_rules_base_columns'] = settings.get('business_rules_base_columns', ['g'])
settings['pca'] = settings.get('pca', True)

np.random.seed(42)

ch = Chain('DataPrep')
ch.logger.log_level = LogLevel.DEBUG

sim_data = data_mimic.MixedVariablesSimulation(store_key='df',
                                               n_obs=10000,
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

# A 'business rule' column is added to the data for demoing purposes. Because the 'business rule' column has a high
# (100%) correlation with another column, this column is not used in the KDE and resample step.
def business_rule(x):
    return x + 1
add_business_rule = analysis.ApplyFuncToDf(read_key='df',
                                           apply_funcs=[{'colin': settings['business_rules_base_columns'][0],
                                                         'colout': settings['business_rules_columns'][0],
                                                         'func': business_rule}])
add_business_rule.logger.log_level = LogLevel.DEBUG
ch.add(add_business_rule)

#all data has been loaded, time to change column names
for column_name in settings['column_names_to_hash']:
    for setting in [settings['columns_to_hash'],
                    settings['unordered_categorical_columns'],
                    settings['ordered_categorical_columns'],
                    settings['continuous_columns'],
                    settings['string_columns']]:
        if column_name in setting:
            setting.remove(column_name)
            setting.append(str(binascii.hexlify(hashlib.pbkdf2_hmac('sha1', column_name.encode('utf-8'),
                                                                    settings['random_salt'], 1000, dklen=8))))


pre_data = data_mimic.KDEPreparation(read_key='df',
                                     data_store_key='data',
                                     data_smoothed_store_key='data_smoothed',
                                     data_no_nans_store_key='data_no_nans',
                                     data_normalized_store_key='data_normalized',
                                     data_normalized_pca_store_key='data_normalized_pca',
                                     maps_store_key='maps',
                                     qts_store_key='qts',
                                     pca_store_key='pca_model',
                                     new_column_order_store_key='new_column_order',
                                     ids_store_key='ids',
                                     do_pca=settings['pca'],
                                     unordered_categorical_columns=settings['unordered_categorical_columns'],
                                     ordered_categorical_columns=settings['ordered_categorical_columns'],
                                     continuous_columns=settings['continuous_columns'],
                                     string_columns=settings['string_columns'],
                                     count=1,
                                     extremes_fraction=0.15,
                                     smoothing_fraction=0.0002,
                                     columns_to_hash=settings['columns_to_hash'],
                                     column_names_to_hash=settings['column_names_to_hash'],
                                     random_salt=settings['random_salt'])

pre_data.logger.log_level = LogLevel.DEBUG
ch.add(pre_data)

ch = Chain('KDE')

kde = data_mimic.KernelDensityEstimation(data_no_nans_read_key='data_no_nans',
                                         data_normalized_read_key='data_normalized',
                                         data_normalized_pca_read_key='data_normalized_pca',
                                         do_pca=settings['pca'],
                                         store_key='bw')
kde.logger.log_level = LogLevel.DEBUG
ch.add(kde)

resampler = data_mimic.Resampler(data_normalized_read_key='data_normalized',
                                 data_normalized_pca_read_key='data_normalized_pca',
                                 data_read_key='data',
                                 bws_read_key='bw',
                                 qts_read_key='qts',
                                 new_column_order_read_key='new_column_order',
                                 maps_read_key='maps',
                                 ids_read_key='ids',
                                 pca_read_key='pca_model',
                                 do_pca=settings['pca'],
                                 df_resample_store_key='df_resample',
                                 resample_store_key='data_resample')
resampler.logger.log_level = LogLevel.DEBUG
ch.add(resampler)

# The 'business rule' column is added to the resampled data.
add_business_rule = analysis.ApplyFuncToDf(read_key='df_resample',
                                           apply_funcs=[{'colin': settings['business_rules_base_columns'][0],
                                                         'colout': settings['business_rules_columns'][0],
                                                         'func': business_rule}])
add_business_rule.logger.log_level = LogLevel.DEBUG
ch.add(add_business_rule)

bins = [np.array([-10, 1.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 1.5, 10]),
        np.array([-100, 0, 100]), np.array([-100, 0, 100]), np.array([-100, 0, 100])]
evaluater = data_mimic.ResampleEvaluation(data_read_key='data',
                                          resample_read_key='data_resample',
                                          bins=bins,
                                          chi2_store_key='chi2',
                                          p_value_store_key='p_value',
                                          new_column_order_read_key='new_column_order',
                                          ks_store_key='kss',
                                          chis_store_key='chis',
                                          distance_store_key='distance',
                                          df_resample_read_key='df_resample',
                                          corr_store_key='correlations')
evaluater.logger.log_level = LogLevel.DEBUG
ch.add(evaluater)

ch = Chain('report')

report = data_mimic.MimicReport(read_key='df',
                                resample_read_key='df_resample',
                                new_column_order_read_key='new_column_order',
                                unordered_categorical_columns=settings['unordered_categorical_columns'],
                                ordered_categorical_columns=settings['ordered_categorical_columns'],
                                continuous_columns=settings['continuous_columns'],
                                string_columns=settings['string_columns'],
                                business_rules_columns=settings['business_rules_columns'],
                                chi2_read_key='chi2',
                                p_value_read_key='p_value',
                                do_pca=settings['pca'],
                                key_data_normalized='data_normalized',
                                key_data_normalized_pca='data_normalized_pca',
                                distance_read_key='distance',
                                corr_read_key='correlations')
report.logger.log_level = LogLevel.DEBUG
ch.add(report)

logger.debug('Done parsing configuration file esk701_mimic_data')
