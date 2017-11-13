"""Project: Eskapade - A python-based package for data analysis.

Macro: esk202_writedata

Created: 2017/02/20

Description:
    Macro to illustrate writing pandas dataframes to file.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain, resources
from eskapade import analysis
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk202_writedata')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk202_writedata'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['do_readdata'] = True
settings['do_writedata'] = True

#########################################################################################
# --- Set path of data
data_path = resources.fixture('dummy.csv')

#########################################################################################
# --- now set up the chains and links based on configuration flags

# --- readdata with default settings reads all three input files simultaneously.
#     all extra key word arguments are passed on to pandas reader.
if settings['do_readdata']:
    read = Chain('ReadData')
    # --- readdata keeps on opening the next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    read_data = analysis.ReadToDf(name='reader', key='test', sep='|', reader='csv', path=[data_path] * 3)
    read.add(read_data)

if settings['do_writedata']:
    write = Chain('WriteData')
    # --- writedata needs a specified output format ('writer' argument).
    #     if this is not set, try to determine this from the extension from the filename.
    #     'key' is picked up from the datastore. 'path' is the output filename.
    #     all other kwargs are passed on to pandas file writer.
    write_data = analysis.WriteFromDf(name='writer', key='test', path='tmp3.csv', writer='csv')
    write.add(write_data)

#########################################################################################

logger.debug('Done parsing configuration file esk202_writedata')
