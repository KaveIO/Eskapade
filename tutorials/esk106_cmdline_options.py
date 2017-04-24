# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk106_cmdline_options                                                         
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro to illustrate the use of flags set from the command line.
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
log = logging.getLogger('macro.esk106_cmdline_options')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops

log.debug('Now parsing configuration file esk106_cmdline_options')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk106_cmdline_options'
settings['version'] = 0

msg = r"""

The two flags below control whether chains are turned on or off. (default=on)
from the cmd line, control these with: 

-c do_chain0=False -c do_chain1=False

Try it; No Hello Worlds will be printed.
"""
log.info(msg)

#########################################################################################
# --- now set up the chains and links based on configuration flags

proc_mgr = ProcessManager()

if settings.get('do_chain0', True):
   ch = proc_mgr.add_chain('Chain0')
   link = core_ops.HelloWorld(name='hello0')
   link.hello = 'Town'
   ch.add_link(link)

if settings.get('do_chain1', True):
   ch = proc_mgr.add_chain('Chain1')
   link = core_ops.HelloWorld(name='hello1')
   link.hello = 'Universe'
   ch.add_link(link)

#########################################################################################

log.debug('Done parsing configuration file esk106_cmdline_options')

