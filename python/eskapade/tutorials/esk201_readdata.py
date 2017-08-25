# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk201_readdata                                                       *
# * Created: 2017/02/17                                                            *
# * Description:                                                                   *
# *      Macro to that illustrates how to open files as pandas datasets.           *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team.                                                       *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging

from eskapade import ConfigObject, resources
from eskapade import core_ops, analysis
from eskapade import process_manager


log = logging.getLogger('macro.esk201_readdata')

log.debug('Now parsing configuration file esk201_readdata')

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
    ch1 = process_manager.add_chain('MyChain1')

    readdata = analysis.ReadToDf(key='test1', sep='|', reader='csv', path=data_path)
    ch1.add_link(readdata)

    # --- do something useful with the test dataset here ...

# --- example 2: readdata with default settings reads all three input files simultaneously.
#                all extra key word arguments are passed on to pandas reader.
if settings['do_example2']:
    ch2 = process_manager.add_chain('MyChain2')

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    readdata = analysis.ReadToDf(name='reader2', key='test2', sep='|', reader='csv', usecols=['x', 'y'])
    readdata.path = [data_path] * 3
    ch2.add_link(readdata)

# --- print contents of the datastore
process_manager.add_chain('Overview')
pds = core_ops.PrintDs(name='End')
pds.keys = ['n_test1', 'n_test2']
process_manager.get_chain('Overview').add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk201_readdata')
