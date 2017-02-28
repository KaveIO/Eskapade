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
from eskapade import core_ops, analysis

log.debug('Now parsing configuration file esk106_cmdline_options')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk106_cmdline_options'
settings['version'] = 0

#########################################################################################
# --- process commands passed on from the command line (-c).
#     any variable declared on the cmd line will be instantiated. 
if 'cmd' in settings: 
    try:
        exec(settings['cmd'])
        log.info('Python cmd executed: %s' % settings['cmd'])
    except: pass
    
# --- Utility func, used below, to check if variable has been defined
#     already, either on cmd line or in settings object.
#     If already exists, return corresponding value.
#     Else, create in settings and set to 'default'.
def checkVar(varName, dic1=vars(), dic2=ProcessManager().service(ConfigObject), default=False):
    varValue = default
    if varName in dic1:   # check existence from cmd line
        varValue = dic1[varName]
    elif varName in dic2: # check in ConfigObject
        varValue = dic2[varName]
    return varValue

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

The two flags below control whether chains are turned on or off. (default=on)
from the cmd line, control these with: 

-c 'do_chain0=False; do_chain1=False' 

Try it. No hello world statements are printed.
"""
log.info(msg)

# the two flags below control whether chains are turned on or off. (default=on)
# by using checkVar(), we can see whether the two flags have already been defined,
# either on the cmd line or already in the settings dict.
# if so, they are not overwritten.

# from the cmd line, control these with: 
# -c 'do_chain0=False; do_chain1=False' 

settings['do_chain0'] = checkVar('do_chain0', default=True)
settings['do_chain1'] = checkVar('do_chain1', default=True)

#########################################################################################
# --- now set up the chains and links based on configuration flags

proc_mgr = ProcessManager()

if settings['do_chain0']:
   ch = proc_mgr.add_chain('Chain0')
   link = core_ops.HelloWorld(name='hello0')
   link.hello = 'Town'
   ch.add_link(link)

if settings['do_chain1']:
   ch = proc_mgr.add_chain('Chain1')
   link = core_ops.HelloWorld(name='hello1')
   link.hello = 'Universe'
   ch.add_link(link)

#########################################################################################

log.debug('Done parsing configuration file esk106_cmdline_options')

