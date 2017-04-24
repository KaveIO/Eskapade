# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk101_helloworld                                                     *
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro to say hello to the world with Eskapade!                            *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk101_helloworld')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops

log.debug('Now parsing configuration file esk101_helloworld')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk101_helloworld'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

#     E.g. define flags turn on or off certain chains with links.
#     by default all set to false, unless already configured in
#     configobject or vars()

settings['do_hello'] = True
settings['n_repeat'] = 2

#########################################################################################
# --- now set up the chains and links based on configuration flags

proc_mgr = ProcessManager()

if settings['do_hello']:
    ch = proc_mgr.add_chain('Hello')
    link = core_ops.HelloWorld(name='HelloWorld')
    link.set_log_level(logging.DEBUG)
    link.repeat = settings['n_repeat']
    ch.add_link(link)


#########################################################################################

log.debug('Done parsing configuration file esk101_helloworld')

