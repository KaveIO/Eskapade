# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk208_record_factorizer                                              *
# * Created: 2017/03/13                                                            *
# * Description:                                                                   *
# *     This macro performs the factorization of an input column of an input
# *     dataframe.  E.g. a columnn x with values 'apple', 'tree', 'pear',
# *     'apple', 'pear' is tranformed into columns x with values 0, 1, 2, 0, 2.
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging

from eskapade import ConfigObject, resources
from eskapade import core_ops, analysis
from eskapade import process_manager


log = logging.getLogger('macro.esk208_record_factorizer')

log.debug('Now parsing configuration file esk208_record_factorizer')

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

ch1 = process_manager.add_chain('Factorize')

# --- read dummy dataset
readdata = analysis.ReadToDf(key='test1', sep='|', reader='csv', path=data_path)
ch1.add_link(readdata)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer1')
pds.keys = ['test1']
ch1.add_link(pds)

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
fact.set_log_level(logging.DEBUG)
ch1.add_link(fact)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer2')
pds.keys = ['to_original', 'test1', 'test1_fact']
ch1.add_link(pds)

ch2 = process_manager.add_chain('ReFactorize')

# --- add second record factorizer, which now maps the columns
#     dummy and loc back to their original format
refact = analysis.RecordFactorizer(name='rf2')
refact.read_key = fact.store_key
refact.store_key = 'test1_refact'
refact.map_to_original = fact.sk_map_to_original
refact.set_log_level(logging.DEBUG)
ch2.add_link(refact)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer3')
pds.keys = ['test1', 'test1_fact', 'test1_refact']
ch2.add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk208_record_factorizer')
