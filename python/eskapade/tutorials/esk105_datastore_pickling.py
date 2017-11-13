"""Project: Eskapade - A python-based package for data analysis.

Macro: esk105_datastore_pickling

Created: 2017/02/20

Description:
    Macro serves as input to other three esk105 example macros.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import core_ops
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk105_datastore_pickling.')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk105_datastore_pickling'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

The setup consists of three simple chains that add progressively more information to the datastore.
In the examples the datastore gets persisted after the execution of each chain, and can be picked
up again as input for the next chain.

- The pickled datastore(s) can be found in the data directory:
{data_path}

- The pickled configuration object(s) and backed-up configuration file can be found in:
{conf_path}
"""
logger.info(msg, data_path=settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/',
            conf_path=settings['resultsDir'] + '/' + settings['analysisName'] + '/config/v0/')

# dummy information used in this macro, added to each chain below.
f = {'hello': 'world', 'v': [3, 1, 4, 1, 5], 'n_favorite': 7}
g = {'a': 1, 'b': 2, 'c': 3}
h = [2, 7]

#########################################################################################
# --- now set up the chains and links based on configuration flags

#########
# chain 1
ch = Chain('chain1')

# the link ToDsDict adds objects to the datastore at link execution.
link = core_ops.ToDsDict(name='intods_1')
link.store_key = 'f'
link.obj = f
ch.add(link)

# print contents of datastore
link = core_ops.PrintDs()
ch.add(link)

#########
# chain 2
ch = Chain('chain2')

# the link AssertInDs checks the presence
# of certain objects in the datastore
link = core_ops.AssertInDs()
link.keySet = ['f']
ch.add(link)

# the link ToDsDict adds objects to the datastore at link execution.
link = core_ops.ToDsDict(name='intods_2')
link.store_key = 'g'
link.obj = g
ch.add(link)

link = core_ops.PrintDs()
ch.add(link)

#########
# chain 3
ch = Chain('chain3')

# the link AssertInDs checks the presence
# of certain objects in the datastore
link = core_ops.AssertInDs()
link.keySet = ['f', 'g']
ch.add(link)

# the link ToDsDict adds objects to the datastore at link execution.
link = core_ops.ToDsDict(name='intods_3')
link.store_key = 'h'
link.obj = h
ch.add(link)

link = core_ops.PrintDs()
ch.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk105_datastore_pickling.')
