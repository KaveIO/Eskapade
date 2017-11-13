"""Project: Eskapade - A python-based package for data analysis.

Macro: esk103_printdatastore

Created: 2017/02/15

Description:
    Macro to illustrate the use of the Printdatastore link.
    Prindatastore prints an overview of the contents in the
    datastore at the state of running

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import core_ops, process_manager, ConfigObject, DataStore, Chain
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk103_printdatastore')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk103_printdatastore'
settings['version'] = 0

#########################################################################################
# --- for this macro, fill the datastore with some dummy information

process_manager.service(DataStore)['hello'] = 'world'
process_manager.service(DataStore)['d'] = {'a': 1, 'b': 2, 'c': 3}

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = Chain('Overview')

# printdatastore prints an overview of the contents in the datastore
# at the state of executing the link.
# The overview consists of list of keys in the datastore and and the object types.
link = core_ops.PrintDs()
# keys are the items for which the contents of the actual item is printed.
# if the key is not known ('foo'), then it is skipped.
link.keys = ['foo', 'hello', 'd']
ch.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk103_printdatastore')
