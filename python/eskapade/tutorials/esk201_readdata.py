"""Project: Eskapade - A python-based package for data analysis.

Macro: esk201_readdata

Created: 2017/02/17

Description:
    Macro to that illustrates how to open files as pandas datasets.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain, resources
from eskapade import core_ops, analysis
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk201_readdata')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk201_readdata'
settings['version'] = 0

#########################################################################################
# --- Analysis configuration flags.
#     E.g. use these flags turn on or off certain chains with links.
#     by default all set to false, unless already configured in
#     configobject or vars()

# turn on/off the 2 examples
settings['do_example1'] = True
settings['do_example2'] = True

#########################################################################################
# --- Set path of data
data_path = resources.fixture('dummy.csv')

#########################################################################################
# --- now set up the chains and links, based on configuration flags

# --- example 1: readdata with one input file
if settings['do_example1']:
    ch1 = Chain('MyChain1')
    read_data = analysis.ReadToDf(key='test1', sep='|', reader='csv', path=data_path)
    ch1.add(read_data)

    # --- do something useful with the test dataset here ...

# --- example 2: readdata with default settings reads all three input files simultaneously.
#                all extra key word arguments are passed on to pandas reader.
if settings['do_example2']:
    ch2 = Chain('MyChain2')

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    read_data = analysis.ReadToDf(name='reader2', key='test2', sep='|', reader='csv', usecols=['x', 'y'])
    read_data.path = [data_path] * 3
    ch2.add(read_data)

# --- print contents of the datastore
overview = Chain('Overview')
pds = core_ops.PrintDs(name='End')
pds.keys = ['n_test1', 'n_test2']
overview.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk201_readdata')
