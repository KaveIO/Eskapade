"""Project: Eskapade - A python-based package for data analysis.

Macro: esk204_apply_query_to_pandas_df

Created: 2017/02/23

Description:
    Illustrates link that applies basic queries to pandas dataframe
    See for more information pandas documentation:

    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from numpy.random import randn
from pandas import DataFrame

from eskapade import ConfigObject, DataStore, Chain
from eskapade import core_ops, analysis
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk204_apply_query_to_pandas_df')

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

data_prep = Chain('DataPrep')

# query_set = seletions that are applies to incoming_records
# after selections, only keep column in select_columns ('a', 'c')
link = analysis.ApplySelectionToDf(read_key='incoming_records',
                                   store_key='outgoing_records',
                                   query_set=['a>0', 'c<b'],
                                   select_columns=['a', 'c'])
# Any other kwargs given to ApplySelectionToDf are passed on the the
# pandas query() function.
link.logger.log_level = LogLevel.DEBUG
data_prep.add(link)

link = core_ops.DsObjectDeleter()
link.deletion_keys = ['incoming_records']
data_prep.add(link)

link = core_ops.PrintDs()
link.keys = ['n_outgoing_records', 'outgoing_records']
data_prep.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk204_apply_query_to_pandas_df')
