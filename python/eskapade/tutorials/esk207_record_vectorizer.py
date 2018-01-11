"""Project: Eskapade - A python-based package for data analysis.

Macro: esk207_record_vectorizer

Created: 2017/03/13

Description:
    This macro performs the vectorization of an input column of an input dataframe.
    E.g. a columnn x with values 1, 2 is tranformed into columns x_1 and x_2,
    with values True or False assigned per record.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, resources, Chain
from eskapade import core_ops, analysis
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk207_record_vectorizer')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk207_record_vectorizer'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# --- Set path of data
data_path = resources.fixture('dummy.csv')

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch1 = Chain('MyChain1')

# --- read dummy dataset
read_data = analysis.ReadToDf(key='test1', sep='|', reader='csv', path=data_path)
ch1.add(read_data)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer1')
pds.keys = ['test1']
ch1.add(pds)

# --- add the record vectorizer
#     Here the columns x and y of the input dataset are vectorized
#     e.g. x=1 becomes the column: x_1 = True
vectorizer = analysis.RecordVectorizer()
vectorizer.columns = ['x', 'y']
vectorizer.read_key = 'test1'
vectorizer.store_key = 'vect_test'
vectorizer.astype = int
ch1.add(vectorizer)

# --- print contents of the datastore
pds = core_ops.PrintDs(name='printer2')
pds.keys = ['vect_test']
ch1.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk207_record_vectorizer')
