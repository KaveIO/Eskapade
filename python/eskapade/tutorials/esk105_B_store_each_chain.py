# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk105_datastore_pickling                                                         
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro illustrates how to persist the datastore 
# *      and configuration object after each chain.
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

from eskapade import process_manager as proc_mgr
from eskapade import ConfigObject

log = logging.getLogger('macro.esk105_B_store_each_chain')

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# turning on this flag, the datastore and configuration are not written out to disk
# at the end of the program.

settings = proc_mgr.service(ConfigObject)
settings['storeResultsEachChain'] = True

msg = r"""

The global flag settings['storeResultsEachChain'] (default = False)
controls persistence of the run-process services after the execution of
a chain.  By default, these objects are only stored after the last
chain.

From the command line, this flag is set with option --store-all.
Alternatively, the option --store-one="chain_name" can be used to store
the results of a single chain.  The with the option --store-none, no
results are stored.
"""
log.info(msg)

#########################################################################################
# --- now parse the follow-up macro


# the flag doNotStoreResults is picked up when parsing the following macro
macro = settings['macrosDir'] + '/' + 'esk105_datastore_pickling.py'
proc_mgr.execute_macro(macro)
