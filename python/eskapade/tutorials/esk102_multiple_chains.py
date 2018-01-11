"""Project: Eskapade - A python-based package for data analysis.

Macro: esk102_multiple_chains

Created: 2017/02/20

Description:
    Macro to illustrate the use of multiple chains

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

logger.debug('Now parsing configuration file esk102_multiple_chains')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
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

if settings['do_chain0']:
    ch = Chain('Chain0')
    link = core_ops.HelloWorld(name='hello0')
    link.hello = 'Town'
    ch.add(link)

# adding more chains is as easy as creating multiple chain instances with unique names.

if settings['do_chain1']:
    ch = Chain('Chain1')
    link = core_ops.HelloWorld(name='hello1')
    link.hello = 'World'
    ch.add(link)

if settings['do_chain2']:
    ch = Chain('Chain2')
    link = core_ops.HelloWorld(name='hello2')
    link.hello = 'Universe'
    ch.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk102_multiple_chains')
