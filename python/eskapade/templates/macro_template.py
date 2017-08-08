# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : MACROTEMPLATE                                                         *
# * Created: DATE                                                                  *
# * Description:                                                                   *
# *      Macro to (please fill in short description here) 
# *      
# *                                                                                *
# * Authors:                                                                       *
# *      Your name(s) here                                                         *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.MACROTEMPLATE')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis

log.debug('Now parsing configuration file MACROTEMPLATE')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'MACROTEMPLATE'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# settings['do_hello'] = True
# ...

#########################################################################################
# --- now set up the chains and links based on configuration flags

# if settings['do_hello']:
#    ch = proc_mgr.add_chain('Hello')
#    link = core_ops.HelloWorld(name='HelloWorld')
#    link.set_log_level(logging.DEBUG)
#    link.repeat = 2
#    ch.add_link(link)




#########################################################################################

log.debug('Done parsing configuration file MACROTEMPLATE')

