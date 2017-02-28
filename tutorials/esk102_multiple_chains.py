# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk101_multiple_chains                                                *
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro to illustrate the use of multiple chains                            *
# *      
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk102_multiple_chains')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis

log.debug('Now parsing configuration file esk102_multiple_chains')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk102_multiple_chains'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['do_chain0'] = True 
settings['do_chain1'] = True 
settings['do_chain2'] = True

#########################################################################################
# --- now set up the chains and links based on configuration flags

# Three simple chains are set up.

proc_mgr = ProcessManager()

if settings['do_chain0']:
   ch = proc_mgr.add_chain('Chain0')
   link = core_ops.HelloWorld(name='hello0')
   link.hello = 'Town'
   ch.add_link(link)

# adding more chains is as easy as calling add_chain and passing a new name.

if settings['do_chain1']:
   ch = proc_mgr.add_chain('Chain1')
   link = core_ops.HelloWorld(name='hello1')
   link.hello = 'World'
   ch.add_link(link)

if settings['do_chain2']:
   ch = proc_mgr.add_chain('Chain2')
   link = core_ops.HelloWorld(name='hello2')
   link.hello = 'Universe'
   ch.add_link(link)

#########################################################################################

log.debug('Done parsing configuration file esk102_multiple_chains')

