# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk208_record_factorizer                                              *
# * Created: 2017/03/13                                                            *
# * Description:                                                                   *
# *     This macro performs the factorization of an input column of an input dataframe.
# *     E.g. a columnn x with values 'apple', 'tree', 'pear', 'apple', 'pear'
# *     is tranformed into columns x with values 0, 1, 2, 0, 2, etc.
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk208_record_factorizer')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis
from eskapade.core import persistence

log.debug('Now parsing configuration file esk208_record_factorizer')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk208_record_factorizer'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# --- Set path of data
data_path = persistence.io_path('data', settings.io_conf(), 'dummy.csv')

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch1 = proc_mgr.add_chain('MyChain1')

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
#     The mapping is stored in a dict under key: 'map_'+store_key
fact = analysis.RecordFactorizer()
fact.columns = ['dummy', 'loc']
fact.read_key = 'test1'
fact.store_key = 'test1_fact'
ch1.add_link(fact)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer2')
pds.keys = ['test1', 'test1_fact', 'map_test1_fact']
ch1.add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk208_record_factorizer')
