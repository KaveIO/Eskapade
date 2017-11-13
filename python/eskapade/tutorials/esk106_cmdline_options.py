"""Project: Eskapade - A python-based package for data analysis.

Macro: esk106_cmdline_options

Created: 2017/02/20

Description:
    Macro to illustrate the use of flags set from the command line.

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

logger.debug('Now parsing configuration file esk106_cmdline_options')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk106_cmdline_options'
settings['version'] = 0

msg = r"""

The two flags below control whether chains are turned on or off. (default=on)
from the cmd line, control these with:

-c do_chain0=False -c do_chain1=False

Try it; No Hello Worlds will be printed.
"""
logger.info(msg)

#########################################################################################
# --- now set up the chains and links based on configuration flags

if settings.get('do_chain0', True):
    ch = Chain('Chain0')
    link = core_ops.HelloWorld(name='hello0')
    link.hello = 'Town'
    ch.add(link)

if settings.get('do_chain1', True):
    ch = Chain('Chain1')
    link = core_ops.HelloWorld(name='hello1')
    link.hello = 'Universe'
    ch.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk106_cmdline_options')
