# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk105_datastore_pickling                                                         
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro to illustrate how to _not_ persist the datastore 
# *      and configuration object.
# *      
# *      
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ConfigObject, ProcessManager

import logging
log = logging.getLogger('macro.esk105_A_dont_store_results')

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# turning on this flag, the datastore and configuration are not written out to disk
# at the end of the program.

settings = ProcessManager().service(ConfigObject)
settings['doNotStoreResults'] = True

msg = r"""

The global flag ProcessManager().service(ConfigObject)['doNotStoreResults']=True controls that the 
datastore & configobject are not written out at the end of the program.
By default, these are only writting out after the last chain. (default=False) 

From the cmd line, this flag is set with option: -n
"""
log.info(msg)

#########################################################################################
# --- now parse the follow-up macro

proc_mgr = ProcessManager()

# the flag doNotStoreResults is picked up when parsing the following macro
macro = settings['macrosDir'] + '/' + 'esk105_datastore_pickling.py'
proc_mgr.execute_macro( macro )
