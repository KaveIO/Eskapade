# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk202_writedata                                                         
# * Created: 2017/02/20                                                            *
# * Description:                                                                   *
# *      Macro to illustrate writing pandas dataframes to file.
# *      
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk202_writedata')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis
from eskapade.core import persistence

log.debug('Now parsing configuration file esk202_writedata')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk202_writedata'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['do_readdata']  = True
settings['do_writedata'] = True

#########################################################################################
# --- Set path of data
data_path = persistence.io_path('data', settings.io_conf(), 'dummy.csv')

#########################################################################################
# --- now set up the chains and links based on configuration flags

proc_mgr = ProcessManager()

# --- readdata with default settings reads all three input files simultaneously.
#     all extra key word arguments are passed on to pandas reader.
if settings['do_readdata']:
    ch = proc_mgr.add_chain('ReadData')

    # --- readdata keeps on opening the next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    readdata = analysis.ReadToDf(name ='reader', key ='test', sep='|', reader='csv', path=[data_path] * 3)
    ch.add_link(readdata)

if settings['do_writedata']:
    ch = proc_mgr.add_chain('WriteData')

    # --- writedata needs a specified output format ('writer' argument).
    #     if this is not set, try to determine this from the extension from the filename.
    #     'key' is picked up from the datastore. 'path' is the output filename.
    #     all other kwargs are passed on to pandas file writer.
    writedata = analysis.WriteFromDf(name ='writer', key ='test', path='tmp3.csv', writer='csv')
    ch.add_link(writedata)


#########################################################################################

log.debug('Done parsing configuration file esk202_writedata')
