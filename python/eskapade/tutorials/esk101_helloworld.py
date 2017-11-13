"""Project: Eskapade - A python-based package for data analysis.

Macro: esk101_helloworld

Created: 2017/02/20

Description:
    Macro to say hello to the world with Eskapade!

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import core_ops
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk101_helloworld')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
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

if settings['do_hello']:
    hello = Chain(name='Hello')
    link = core_ops.HelloWorld(name='HelloWorld')
    link.logger.log_level = LogLevel.DEBUG
    link.repeat = settings['n_repeat']
    hello.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk101_helloworld')
