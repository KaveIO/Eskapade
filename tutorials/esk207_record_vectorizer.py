# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk207_record_vectorizer                                              *
# * Created: 2017/03/13                                                            *
# * Description:                                                                   *
# *     This macro performs the vectorization of an input column of an input dataframe.
# *     E.g. a columnn x with values 1, 2 is tranformed into columns x_1 and x_2, 
# *     with values True or False assigned per record.
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk207_record_vectorizer')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis
from eskapade.core import persistence

log.debug('Now parsing configuration file esk207_record_vectorizer')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk207_record_vectorizer'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# --- Set path of data
data_path = persistence.io_path('data', settings.io_conf(), 'dummy.csv')

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch1 = proc_mgr.add_chain('MyChain1')

# --- read dummy dataset
readdata = analysis.ReadToDf(key ='test1', sep='|', reader='csv', path=data_path)
ch1.add_link(readdata)

# --- print contents of the datastore
pds = core_ops.PrintDs(name = 'printer1')
pds.keys = ['test1']
ch1.add_link(pds)

# --- add the record vectorizer
#     Here the columns x and y of the input dataset are vectorized
#     e.g. x=1 becomes the column: x_1 = True
vect = analysis.RecordVectorizer()
vect.columns = ['x','y']
vect.read_key = 'test1'
vect.store_key = 'vect_test'
vect.astype = int
ch1.add_link(vect)

# --- print contents of the datastore
pds = core_ops.PrintDs(name = 'printer2')
pds.keys = ['vect_test']
ch1.add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk207_record_vectorizer')

