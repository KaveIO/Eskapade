"""Project: Eskapade - A python-based package for data analysis.

Macro: esk109_debugging_tips

Created: 2017/02/26

Description:
    Macro to illustrate basic debugging features of Eskapade.
    The macro shows how to start interactive ipython sessions while
    running through the chains, and also how to break out of a chain.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import DataStore
from eskapade import core_ops
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk109_debugging_tips')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk109_debugging_tips'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

To end the eskapade_run session with an interactive ipython shell,
from the cmd line use the this flag: -i
"""
logger.info(msg)

# testing false used for running integration tests. do not remove.
settings['TESTING'] = settings.get('TESTING', False)

#########################################################################################
# --- now set up the chains and links based on configuration flags

ds = process_manager.service(DataStore)
ds['hello'] = 'world'
ds['d'] = {'a': 1, 'b': 2, 'c': 3}

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = Chain('Overview')

# 1. printdatastore prints an overview of the contents in the datastore
# at the state of executing the link.
# The overview consists of list of keys in the datastore and and the object types.
link = core_ops.PrintDs(name='printer1')
# keys are the items for which the contents of the actual item is printed.
link.keys = ['hello', 'd']
ch.add(link)

# 2. This link will start an interactive ipython session.
# from this session, one can access the datastore and the configobject with:
# >>> ds
# or
# >>> settings
# Try to add something to the datastore in this session!
# >>> ds['foo'] = 'bar'
if not settings['TESTING']:
    link = core_ops.IPythonEmbed()
    ch.add(link)

# 3. let's see what has been added to the datastore ...
link = core_ops.PrintDs(name='printer2')
# keys are the items for which the contents of the actual item is printed.
link.keys = ['foo', 'hello', 'd']
ch.add(link)

# 4. This link sends out a break signal!
# eskapade execution or any remaining links and chains is skipped.
link = core_ops.Break()
# keys are the items for which the contents of the actual item is printed.
ch.add(link)
link.keys = ['foo', 'hello', 'd']

# 5. this link should not be reached because of the Break!
ch = Chain('End')
link = core_ops.PrintDs(name='printer3')
ch.add(link)

# 6. eskapade_run with cmd line option -i to end the eskapade session with an interactive ipython shell

#########################################################################################

logger.debug('Done parsing configuration file esk109_debugging_tips')
