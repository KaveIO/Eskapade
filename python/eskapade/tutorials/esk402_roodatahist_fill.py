"""Project: Eskapade - A python-based package for data analysis.

Macro: esk402_roodatahist_fill

Created: 2017/03/28

Description:
    This macro illustrates how to fill a N-dimensional roodatahist from a
    pandas dataframe. (A roodatahist can be filled iteratively, while looping
    over multiple pandas dataframes.) The roodatahist can be used to create
    a roofit histogram-pdf (roohistpdf).

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

logger.debug('Now parsing configuration file esk402_roodatahist_fill')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk402_roodatahist_fill'
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
# readdata.itr_over_files = True
ch.add(read_data)

# --- 1. add the record factorizer to convert categorical observables into integers
#     Here the columns dummy and loc of the input dataset are factorized
#     e.g. x = ['apple', 'tree', 'pear', 'apple', 'pear'] becomes the column:
#     x = [0, 1, 2, 0, 2]
#     By default, the mapping is stored in a dict under key: 'map_'+store_key+'_to_original'
fact = analysis.RecordFactorizer(name='rf1')
fact.columns = ['isActive', 'eyeColor', 'favoriteFruit', 'gender']
fact.read_key = 'accounts'
fact.inplace = True
# factorizer stores a dict with the mappings that have been applied to all observables
fact.sk_map_to_original = 'to_original'
# factorizer also stores a dict with the mappings back to the original observables
fact.sk_map_to_factorized = 'to_factorized'
fact.logger.log_level = LogLevel.DEBUG
ch.add(fact)

# --- 2. Fill a roodatahist
df2rdh = root_analysis.RooDataHistFiller()
df2rdh.read_key = read_data.key
df2rdh.store_key = 'rdh_' + read_data.key
# the observables in this map are treated as categorical observables by roofit (roocategories)
df2rdh.map_to_factorized = 'to_factorized'
df2rdh.columns = ['transaction', 'latitude', 'longitude', 'age', 'eyeColor', 'favoriteFruit']
# df2rdh.into_ws = True
ch.add(df2rdh)

# --- print contents of the datastore
overview = Chain('Overview')
pds = core_ops.PrintDs()
pds.keys = ['n_rdh_accounts', 'n_accounts']
overview.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk402_roodatahist_fill')
