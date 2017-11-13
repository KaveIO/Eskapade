"""Project: Eskapade - A python-based package for data analysis.

Macro: esk403_roodataset_convert

Created: 2017/03/28

Description:
    This macro illustrates how to convert a pandas dataframe to a roofit dataset
    (= roodataset), do something to it with roofit, and then convert the roodataset
    back again to a pandas dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import core_ops, analysis, root_analysis
from eskapade import process_manager
from eskapade import resources
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk403_roodataset_convert')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk403_roodataset_convert'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

input_files = [resources.fixture('mock_accounts.csv.gz')]

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = Chain('Data')

# --- 0. read the input data
#     all kwargs are passed on to pandas file reader.
read_data = analysis.ReadToDf(name='dflooper', key='accounts', reader='csv')
read_data.path = input_files
ch.add(read_data)

ch = Chain('Conversion1')

# --- 1. add the record factorizer
#     Here the columns dummy and loc of the input dataset are factorized
#     e.g. x = ['apple', 'tree', 'pear', 'apple', 'pear'] becomes the column:
#     x = [0, 1, 2, 0, 2]
#     By default, the mapping is stored in a dict under key: 'map_'+store_key+'_to_original'
fact = analysis.RecordFactorizer(name='rf1')
fact.columns = ['isActive', 'eyeColor', 'favoriteFruit', 'gender']
fact.read_key = 'accounts'
fact.store_key = 'accounts_fact'
# factorizer stores a dict with the mappings back to the original observables
fact.sk_map_to_original = 'to_original'
# factorizer also stores a dict with the mappings that have been applied to all observables
fact.sk_map_to_factorized = 'to_factorized'
fact.logger.log_level = LogLevel.DEBUG
ch.add(fact)

# --- 2. turn the dataframe into a roofit dataset (= roodataset)
df2rds = root_analysis.ConvertDataFrame2RooDataSet()
df2rds.read_key = fact.store_key
df2rds.store_key = 'rds_' + read_data.key
# the observables in this map are treated as categorical observables by roofit (roocategories)
df2rds.map_to_factorized = 'to_factorized'
df2rds.columns = ['gender', 'eyeColor', 'favoriteFruit', 'isActive']
# booleans and all remaining numpy category observables are converted to roocategories as well
# the mapping to restore all roocategories is stored in the datastore under this key
df2rds.sk_map_to_original = 'rds_to_original'
# store results in roofitmanager workspace?
# df2rds.into_ws = True
ch.add(df2rds)

pds = core_ops.PrintDs(name='pds1')
pds.keys = [fact.sk_map_to_factorized, df2rds.sk_map_to_original]
ch.add(pds)

# --- you should do something to the roodataset here,
#     possibly producting a new roodataset
action = Chain('Action')

# --- example to convert a roodatset back to a pandas df
ch = Chain('Conversion2')

# --- first, convert the roodataset back to a plain pandas dataframe
rds2df = root_analysis.ConvertRooDataSet2DataFrame()
rds2df.read_key = df2rds.store_key
rds2df.store_key = 'df_from_rds'
ch.add(rds2df)

# --- add second record factorizer, which now maps all roocategory columns
#     back to their original format.
#     For this it picks up the mapping in: 'rds_to_original'
refact = analysis.RecordFactorizer(name='rf2')
refact.read_key = rds2df.store_key
refact.store_key = 'df_refact'
refact.map_to_original = df2rds.sk_map_to_original
refact.logger.log_level = LogLevel.DEBUG
ch.add(refact)

pds = core_ops.PrintDs(name='pds2')
pds.keys = ['n_df_from_rds', 'df_refact']
ch.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk403_roodataset_convert')
