"""Project: Eskapade - A python-based package for data analysis.

Macro: esk208_record_factorizer

Created: 2017/03/13

Description:
    This macro performs the factorization of an input column of an input
    dataframe.  E.g. a columnn x with values 'apple', 'tree', 'pear',
    'apple', 'pear' is tranformed into columns x with values 0, 1, 2, 0, 2.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, resources, Chain
from eskapade import core_ops, analysis
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk208_record_factorizer')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk208_record_factorizer'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# --- Set path of data
data_path = resources.fixture('dummy.csv')

#########################################################################################
# --- now set up the chains and links based on configuration flags

factorize = Chain('Factorize')

# --- read dummy dataset
read_data = analysis.ReadToDf(key='test1', sep='|', reader='csv', path=data_path)
factorize.add(read_data)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer1')
pds.keys = ['test1']
factorize.add(pds)

# --- add the record factorizer
#     Here the columns dummy and loc of the input dataset are factorized
#     e.g. x = ['apple', 'tree', 'pear', 'apple', 'pear'] becomes the column:
#     x = [0, 1, 2, 0, 2]
#     By default, the mapping is stored in a dict under key: 'map_'+store_key+'_to_original'
fact = analysis.RecordFactorizer(name='rf1')
fact.columns = ['dummy', 'loc']
fact.read_key = 'test1'
fact.store_key = 'test1_fact'
fact.sk_map_to_original = 'to_original'
fact.logger.log_level = LogLevel.DEBUG
factorize.add(fact)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer2')
pds.keys = ['to_original', 'test1', 'test1_fact']
factorize.add(pds)

refactorize = Chain('ReFactorize')

# --- add second record factorizer, which now maps the columns
#     dummy and loc back to their original format
factorizer = analysis.RecordFactorizer(name='rf2')
factorizer.read_key = fact.store_key
factorizer.store_key = 'test1_refact'
factorizer.map_to_original = fact.sk_map_to_original
factorizer.logger.log_level = LogLevel.DEBUG
refactorize.add(factorizer)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer3')
pds.keys = ['test1', 'test1_fact', 'test1_refact']
refactorize.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk208_record_factorizer')
