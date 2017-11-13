"""Project: Eskapade - A python-based package for data analysis.

Macro: esk410_testing_correlations_between_categories

Created: 2017/07/04

Description:
    This macro illustrates how to find correlations between categorical
    observables.

    Based on the hypothesis of no correlation expected frequencies of observations *
    are calculated. The measured frequencies are compared to expected frequencies. *
    From these the (significance of the) p-value of the hypothesis that the
    observables in the input dataset are not correlated is determined. The
    normalized residuals (pull values) for each bin in the dataset are also
    calculated. A detailed description of the method can be found in ABCDutils.h.
    A description of the method to calculate the expected frequencies can be found *
    in RooABCDHistPDF.cxx.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import analysis, root_analysis, visualization
from eskapade import process_manager
from eskapade import resources
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk410_testing_correlations_between_categories')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk410_testing_correlations_between_categories'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

input_files = [resources.fixture('mock_accounts.csv.gz')]

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = Chain('Data')

# --- 0. readdata keeps on opening the next file in the file list.
#     all kwargs are passed on to pandas file reader.
read_data = analysis.ReadToDf(name='dflooper', key='accounts', reader='csv')
read_data.path = input_files
ch.add(read_data)

# --- 1. add the record factorizer to convert categorical observables into integers
#     Here the columns dummy and loc of the input dataset are factorized
#     e.g. x = ['apple', 'tree', 'pear', 'apple', 'pear'] becomes the column:
#     x = [0, 1, 2, 0, 2]
#     By default, the mapping is stored in a dict under key: 'map_'+store_key+'_to_original'
fact = analysis.RecordFactorizer(name='rf1')
fact.columns = ['eyeColor', 'favoriteFruit']  # ['Obs_*']
fact.read_key = read_data.key
fact.inplace = True
# factorizer stores a dict with the mappings that have been applied to all observables
fact.sk_map_to_original = 'to_original'
# factorizer also stores a dict with the mappings back to the original observables
fact.sk_map_to_factorized = 'to_factorized'
ch.add(fact)

# --- 2. turn the dataframe into a roofit dataset (= roodataset)
df2rds = root_analysis.ConvertDataFrame2RooDataSet()
df2rds.read_key = read_data.key
df2rds.store_key = 'rds_' + read_data.key
df2rds.store_key_vars = 'rds_varset'
# the observables in this map are treated as categorical observables by roofit (roocategories)
df2rds.map_to_factorized = 'to_factorized'
df2rds.columns = fact.columns + ['age']  # + ['longitude', 'latitude']
# booleans and all remaining numpy category observables are converted to roocategories as well
# the mapping to restore all roocategories is stored in the datastore under this key
df2rds.sk_map_to_original = 'rds_to_original'
# store results in roofitmanager workspace?
# df2rds.into_ws = True
ch.add(df2rds)

# --- 3. run hypothesis tester
hypotest = root_analysis.UncorrelationHypothesisTester()
hypotest.map_to_original = df2rds.sk_map_to_original
hypotest.verbose_plots = True
hypotest.z_threshold = 3.0

# Choose one out of a, b, c
# a. run test for all combinations of columns
hypotest.columns = df2rds.columns
# b. run test for pairs of observables from two lists
# Make one by one combinations between x and y columns if inproduct = True, otherwise
# make all possible combination.
# hypotest.y_columns = ['Obs_A*'] # ['Obs_A1','Obs_A2', .. ,'Obs_An']
# hypotest.x_columns = ['Obs_B*'] # ['Obs_B1','Obs_B2', .. ,'Obs_Bn']
# hypotest.inproduct = True
# c. Specify exactly for what combinations to run the test
# hypotest.combinations = [['Obs_A1','Obs_B1','Obs_A2'], ['Obs_A2','Obs_B2']]

# read and write keys datastore
hypotest.read_key = df2rds.store_key
hypotest.read_key_vars = df2rds.store_key_vars
hypotest.pages_key = 'report_pages'
hypotest.hist_dict_key = 'histograms'
# key to store the results of the significance test in the datastore
hypotest.sk_significance_map = 'significance'
# key to store the results of the residuals test in the datastore
hypotest.sk_residuals_map = 'residuals'
# key to store the results of the residuals test in the datastore, in format which
# is makes further processing more easy
hypotest.sk_residuals_overview = 'residuals_overview'

# Advanced settings
# Specify what categories to ignore.
# hypotest.ignore_categories = ['None','Not_familair_with','NoFruit']
# hypotest.var_ignore_categories = ['obs1':'None','obs2':'Not_familiar_with','obs1:obs2':['None','pear']]
# Hypothesis tester is also applicable to continues variables once they are categorised;
# ie make bins. The number of bins can be set using the following options
# hypotest.default_number_of_bins = 5
# hypotest.var_default_number_of_bins = ['obs1':10,'obs2':5,'obs1:obs2':[3,3]]

hypotest.logger.log_level = LogLevel.DEBUG
ch.add(hypotest)

# --- 4. print contents of the datastore
overview = Chain('Overview')
hist_summary = visualization.DfSummary(name='HistogramSummary',
                                       read_key=hypotest.hist_dict_key,
                                       pages_key=hypotest.pages_key)
overview.add(hist_summary)

#########################################################################################

logger.debug('Done parsing configuration file esk410_testing_correlations_between_categories')
