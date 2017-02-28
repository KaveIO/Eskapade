# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk104_datastore_operations                                                         
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro to illustrate how to control the contents of the datastore
# *      
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk104_basic_datastore_operations')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis

log.debug('Now parsing configuration file esk104_basic_datastore_operations')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk104_basic_datastore_operations'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# some dummy information to use in this macro
f = { 'hello': 'world', 'v': [3,1,4,1,5], 'n_favorite': 7 }
g = { 'a' : 1, 'b' : 2, 'c' : 3, 'd' : 4, 'e' : 'favorite' }

#########################################################################################
# --- now set up the chains and links based on configuration flags

# demo consists of 5 simple parts:
#
# 1. putting items in the datastore, and displaying the contents.
# 2. asserting the presence of items in the datastore, and then deleting individual items from the datastore.
# 3. deleting all items from the datastore. 
# 4. deleting all but certain items from the datastore.
# 5. moving, copying, or removing objects from the datastore

proc_mgr = ProcessManager()

#########
# chain 1:
# - putting items in the datastore.
# - displaying the contents of items in the datastore.

ch = proc_mgr.add_chain('chain1')

# the link ToDsDict adds objects to the datastore
# by default this happens at the execution of the link.
# (optionally, this can be done at initialization.)
# Here it is used as a dummy data generator.

link = core_ops.ToDsDict(name='intods_1')
link.obj = f
# copydict = true: all items in dict f are added to the datastore
link.copydict = True
ch.add_link(link)

# print contents of datastore
link = core_ops.PrintDs()
link.keys = ['n_favorite','hello']
ch.add_link(link)


#########
# chain 2
# - asserting the presence of items in the datastore.
# - deleting individual items from the datastore.

ch = proc_mgr.add_chain('chain2')

# the link AssertInDs checks the presence
# of certain objects in the datastore
link = core_ops.AssertInDs()
link.keySet = ['hello', 'n_favorite']
ch.add_link(link)

# the link DsObjectDeleter removes objects from the datastore
# here, keep only 'v'
link = core_ops.DsObjectDeleter()
link.keepOnly = ['v']
ch.add_link(link)

link = core_ops.PrintDs()
link.keys = ['v','hello']
ch.add_link(link)


#########
# chain 3
# - deleting all items from the datastore.

ch = proc_mgr.add_chain('chain3')

# default, delete everything from the datastore
link = core_ops.DsObjectDeleter()
ch.add_link(link)

link = core_ops.ToDsDict(name='intods_3')
link.obj = g
link.copydict = True
ch.add_link(link)

link = core_ops.PrintDs()
ch.add_link(link)


#########
# chain 4
# - deleting all but certain items from the datastore.

ch = proc_mgr.add_chain('chain4')

link = core_ops.AssertInDs()
link.keySet = ['a', 'b']
ch.add_link(link)

# here, delete only c and d
link = core_ops.DsObjectDeleter()
link.deletionKeys = ['d', 'e']
ch.add_link(link)

link = core_ops.PrintDs()
ch.add_link(link)



#########
# chain 5
# - moving, copying, or removing objects from the datastore

ch = proc_mgr.add_chain('chain5')

link = core_ops.AssertInDs()
link.keySet = ['a', 'b', 'c']
ch.add_link(link)

# the link DsToDs can move, copy, or remove objects from the datastore
# default is move.

# in this example, move key 'c' to 'd'
link = core_ops.DsToDs(name ='ds_move')
link.readKey = 'c'
link.storeKey = 'd'
ch.add_link(link)

link = core_ops.PrintDs(name ='pds1')
link.keys = ['d']
ch.add_link(link)

# in this example, copy key 'd' to 'e'
link = core_ops.DsToDs(name ='ds_copy')
link.readKey = 'd'
link.storeKey = 'e'
link.copy = True
ch.add_link(link)

link = core_ops.PrintDs(name ='pds2')
link.keys = ['d','e']
ch.add_link(link)

# in this example, remove item 'd' 
link = core_ops.DsToDs(name ='ds_remove')
link.readKey = 'd'
link.remove = True
ch.add_link(link)

link = core_ops.PrintDs(name ='pds3')
link.keys = ['e']
ch.add_link(link)

link = core_ops.DsObjectDeleter()
link.keepOnly = ['a']
ch.add_link(link)

# empty ...
link = core_ops.PrintDs()
link.keys = ['a']
ch.add_link(link)

#########################################################################################

log.debug('Done parsing configuration file esk104_basic_datastore_operations')
