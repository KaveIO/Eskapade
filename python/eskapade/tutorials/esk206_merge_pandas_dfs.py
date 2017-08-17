# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk206_merge_pandas_dfs                                               *
# * Created: 2017/02/23                                                            *
# * Description:                                                                   *
# *      Illustrate link that calls basic merge() of pandas dataframes                      *
# *      For more information see pandas documentation:
# *                                                                                *
# *      http://pandas.pydata.org/pandas-docs/stable/merging.html
# *                                                                                *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging

from pandas import DataFrame

from eskapade import process_manager as proc_mgr
from eskapade import ConfigObject, DataStore
from eskapade import core_ops, analysis

log = logging.getLogger('macro.esk206_merge_pandas_dfs')

log.debug('Now parsing configuration file esk206_merge_pandas_dfs')

#########################################################################################
# --- minimal analysis information

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk206_merge_pandas_dfs'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# generate dummy dataframes and add to datastore
# these dataframes are concatenated below, during link execution.

# NB: realize that, normally, such dataframes are read or generated
# on the fly during link execution.

ds = proc_mgr.service(DataStore)

ds['left'] = DataFrame({'key': ['K0', 'K1', 'K2', 'K3'],
                        'A': ['A0', 'A1', 'A2', 'A3'],
                        'B': ['B0', 'B1', 'B2', 'B3']})

ds['right'] = DataFrame({'key': ['K0', 'K1', 'K2', 'K3'],
                         'C': ['C0', 'C1', 'C2', 'C3'],
                         'D': ['D0', 'D1', 'D2', 'D3']})

#########################################################################################
# --- below we merge the two dataframes found in the datastore

ch = proc_mgr.add_chain('DataPrep')

# inner-join the two dataframes with each other on 'key' during link execution
link = analysis.DfMerger(input_collection1 ='left',
                         input_collection2 = 'right',
                         output_collection = 'outgoing',
                         how = 'inner',
                         on = 'key')
# Any other kwargs given to DfMerger are passed on the the
# pandas merge() function.
link.set_log_level(logging.DEBUG)
ch.add_link(link)

link = core_ops.DsObjectDeleter()
link.deletionKeys = ['left','right']
ch.add_link(link)

link = core_ops.PrintDs()
link.keys = ['n_outgoing','outgoing']
ch.add_link(link)

#########################################################################################

log.debug('Done parsing configuration file esk206_merge_pandas_dfs')
