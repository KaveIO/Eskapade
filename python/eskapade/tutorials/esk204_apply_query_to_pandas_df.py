# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk204_apply_query_to_pandas_df                                       *
# * Created: 2017/02/23                                                            *
# * Description:                                                                   *
# *      Illustrates link that applies basic queries to pandas dataframe           *
# *      See for more information pandas documentation:
# *                                                                                *
# *      http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging

from numpy.random import randn
from pandas import DataFrame

from eskapade import ConfigObject, DataStore
from eskapade import core_ops, analysis
from eskapade import process_manager

log = logging.getLogger('macro.esk204_apply_query_to_pandas_df')

log.debug('Now parsing configuration file esk204_apply_query_to_pandas_df')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk204_apply_query_to_pandas_df'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# generate a dummy dataframe and add to datastore
# to this dataset selections are applied below, during link execution.

# NB: realize that, normally, such a dataframe is read or constructed on the fly
# during link execution.


df = DataFrame(randn(100, 3), columns=list('abc'))

ds = process_manager.service(DataStore)
ds['incoming_records'] = df

#########################################################################################
# --- Here we apply example selections to a dataframe picked up from the datastore. 

ch = process_manager.add_chain('DataPrep')

# querySet = seletions that are applies to incoming_records
# after selections, only keep column in selectColumns ('a', 'c')
link = analysis.ApplySelectionToDf(readKey='incoming_records',
                                   storeKey='outgoing_records',
                                   querySet=['a>0', 'c<b'],
                                   selectColumns=['a', 'c'])
# Any other kwargs given to ApplySelectionToDf are passed on the the
# pandas query() function.
link.set_log_level(logging.DEBUG)
ch.add_link(link)

link = core_ops.DsObjectDeleter()
link.deletionKeys = ['incoming_records']
ch.add_link(link)

link = core_ops.PrintDs()
link.keys = ['n_outgoing_records', 'outgoing_records']
ch.add_link(link)

#########################################################################################

log.debug('Done parsing configuration file esk204_apply_query_to_pandas_df')
