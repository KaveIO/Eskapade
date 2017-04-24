# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk105_datastore_pickling                                                         
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro serves as input to other three esk105 example macros.
# *      
# *      
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk105_datastore_pickling')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops

log.debug('Now parsing configuration file esk105_datastore_pickling')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk105_datastore_pickling'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

The setup consists of three simple chains that add progressively more information to the datastore.
In the examples the datastore gets persisted after the execution of each chain, and can be picked 
up again as input for the next chain.

- The pickled datastore(s) can be found in the data directory:
%s

- The pickled configuration object(s) and backed-up configuration file can be found in:
%s
""" % (settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/',
       settings['resultsDir'] + '/' + settings['analysisName'] + '/config/v0/')
log.info(msg)

# dummy information used in this macro, added to each chain below.
f = { 'hello': 'world', 'v': [3,1,4,1,5], 'n_favorite': 7 }
g = { 'a' : 1, 'b' : 2, 'c' : 3 }
h = [ 2, 7 ]

#########################################################################################
# --- now set up the chains and links based on configuration flags

proc_mgr = ProcessManager()

#########
# chain 1
ch = proc_mgr.add_chain('chain1')

# the link ToDsDict adds objects to the datastore at link execution.
link = core_ops.ToDsDict(name='intods_1')
link.store_key = 'f'
link.obj = f
ch.add_link(link)

# print contents of datastore
link = core_ops.PrintDs()
ch.add_link(link)


#########
# chain 2
ch = proc_mgr.add_chain('chain2')

# the link AssertInDs checks the presence
# of certain objects in the datastore
link = core_ops.AssertInDs()
link.keySet = ['f']
ch.add_link(link)

# the link ToDsDict adds objects to the datastore at link execution.
link = core_ops.ToDsDict(name='intods_2')
link.store_key = 'g'
link.obj = g
ch.add_link(link)

link = core_ops.PrintDs()
ch.add_link(link)


#########
# chain 3
ch = proc_mgr.add_chain('chain3')

# the link AssertInDs checks the presence
# of certain objects in the datastore
link = core_ops.AssertInDs()
link.keySet = ['f','g']
ch.add_link(link)

# the link ToDsDict adds objects to the datastore at link execution.
link = core_ops.ToDsDict(name='intods_3')
link.store_key = 'h'
link.obj = h
ch.add_link(link)

link = core_ops.PrintDs()
ch.add_link(link)


#########################################################################################

log.debug('Done parsing configuration file esk105_datastore_pickling')

