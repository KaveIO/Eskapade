"""Project: Eskapade - A python-based package for data analysis.

Macro: esk107_chain_looper

Created: 2017/02/17

Description:
    Macro to that illustrates how to repeat the execution of a chain.
    Follow-up example in macro: esk209_readdata_itr.py

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

logger.debug('Now parsing configuration file esk107_chain_looper')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk107_chain_looper'
settings['version'] = 0

#########################################################################################
# --- Analysis configuration flags.
#     E.g. use these flags turn on or off certain chains with links.
#     by default all set to false, unless already configured in
#     configobject or vars()

# turn on/off the example
settings['do_example'] = True

#########################################################################################
# --- now set up the chains and links, based on configuration flags


# --- example loops over the first chain 10 times.

if settings['do_example']:
    # --- a loop is set up in the chain MyChain.
    #     we iterate over the chain until the link RepeatChain is done.
    #     then move on to the next chain (Overview)
    ch = Chain('MyChain')

    link = core_ops.HelloWorld(name='HelloWorld')
    link.logger.log_level = LogLevel.DEBUG
    ch.add(link)

    # --- this link sends out a signal to repeat the execution of the chain.
    #     It serves as the 'continue' statement of the loop.
    #     go back to start of the chain until counter reaches 10.
    repeater = core_ops.RepeatChain()
    # repeat max of 10 times
    repeater.maxcount = 10
    repeater.logger.log_level = LogLevel.DEBUG
    ch.add(repeater)

# --- print contents of the datastore.
#    which in this case is empty.
overview = Chain('Overview')
pds = core_ops.PrintDs(name='End')
overview.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk107_chain_looper')
