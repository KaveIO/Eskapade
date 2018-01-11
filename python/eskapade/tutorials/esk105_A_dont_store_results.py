"""Project: Eskapade - A python-based package for data analysis.

Macro: esk105_datastore_pickling

Created: 2017/02/20

Description:
    Macro to illustrate how to _not_ persist the datastore
    and configuration object.

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

# turning on this flag, the datastore and configuration are not written out to disk
# at the end of the program.

settings = process_manager.service(ConfigObject)
settings['doNotStoreResults'] = True

msg = r"""

The global flag settings['doNotStoreResults'] (default = False) controls
the non-persistence of the run-process services after execution of the
chains.  By default, services are stored after the last chain.

From the command line, this flag is set with option --store-none.
"""
logger.info(msg)

#########################################################################################
# --- now parse the follow-up macro

# the flag doNotStoreResults is picked up when parsing the following macro
process_manager.execute_macro(resources.tutorial('esk105_datastore_pickling.py'))
