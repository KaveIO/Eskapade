"""Project: Eskapade - A python-based package for data analysis.

Macro: esk105_datastore_pickling

Created: 2017/02/20

Description:
    Macro illustrates how to start running at any particular chain by
    picking up the datastore and configuration objects from the
    previous chain.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject
from eskapade import process_manager, resources
from eskapade.logger import Logger

logger = Logger()

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# turning on this flag, the process manager starts off at chain3,
# and does so by reading in the datastore written out after chain 2.

settings = process_manager.service(ConfigObject)
settings['beginWithChain'] = 'chain3'

msg = r"""

--> Make sure to run this macro after running macro
    esk105_B_store_each_chain.py

By default, the process manager starts the execution of the first
configured chain and end with the last one.  If the run-process services
are written out after the execution of each chain (--store-all), it is
possible to execute one particular chain.  This is done by picking up
the services written out by the previous chain.  This is a nice feature
for debugging and developing purposes.

From the command line use the options:

-b BEGIN_WITH_CHAIN
-e END_WITH_CHAIN
-s SINGLE_CHAIN
"""
logger.info(msg)

#########################################################################################
# --- now parse the follow-up macro

# the flag doNotStoreResults is picked up when parsing the following macro
process_manager.execute_macro(resources.tutorial('esk105_datastore_pickling.py'))
