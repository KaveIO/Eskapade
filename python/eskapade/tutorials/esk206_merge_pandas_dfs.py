"""Project: Eskapade - A python-based package for data analysis.

Macro: esk206_merge_pandas_dfs

Created: 2017/02/23

Description:
    Illustrate link that calls basic merge() of pandas dataframes
    For more information see pandas documentation:

    http://pandas.pydata.org/pandas-docs/stable/merging.html

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from pandas import DataFrame

from eskapade import ConfigObject, DataStore, Chain
from eskapade import core_ops, analysis
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk206_merge_pandas_dfs')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk206_merge_pandas_dfs'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# generate dummy dataframes and add to datastore
# these dataframes are concatenated below, during link execution.

# NB: realize that, normally, such dataframes are read or generated
# on the fly during link execution.

ds = process_manager.service(DataStore)

ds['left'] = DataFrame({'key': ['K0', 'K1', 'K2', 'K3'],
                        'A': ['A0', 'A1', 'A2', 'A3'],
                        'B': ['B0', 'B1', 'B2', 'B3']})

ds['right'] = DataFrame({'key': ['K0', 'K1', 'K2', 'K3'],
                         'C': ['C0', 'C1', 'C2', 'C3'],
                         'D': ['D0', 'D1', 'D2', 'D3']})

#########################################################################################
# --- below we merge the two dataframes found in the datastore

data_prep = Chain('DataPrep')

# inner-join the two dataframes with each other on 'key' during link execution
link = analysis.DfMerger(input_collection1='left',
                         input_collection2='right',
                         output_collection='outgoing',
                         how='inner',
                         on='key')
# Any other kwargs given to DfMerger are passed on the the
# pandas merge() function.
link.logger.log_level = LogLevel.DEBUG
data_prep.add(link)

link = core_ops.DsObjectDeleter()
link.deletion_keys = ['left', 'right']
data_prep.add(link)

link = core_ops.PrintDs()
link.keys = ['n_outgoing', 'outgoing']
data_prep.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk206_merge_pandas_dfs')
