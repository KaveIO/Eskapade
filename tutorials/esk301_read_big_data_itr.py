# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk301_read_big_data_itr                                              *
# * Created: 2017/02/17                                                            *
# * Description:                                                                   *
# *      Macro to that illustrates how to loop over multiple (possibly large!)     *
# *      datasets in chunks.                                                       *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team.                                                       *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk301_read_big_data_itr')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis
from eskapade.core import persistence

log.debug('Now parsing configuration file esk301_read_big_data_itr')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk301_read_big_data_itr'
settings['version'] = 0

#########################################################################################

# when chunking through an input file, pick up only N lines in each iteration.
chunksize = 5

#########################################################################################
# --- Set path of data
data_path = persistence.io_path('data', settings.io_conf(), 'dummy.csv')

#########################################################################################
# --- now set up the chains and links, based on configuration flags

proc_mgr = ProcessManager()

# --- example 1: readdata loops over the input files, but no file chunking.

if settings.get('do_example1', True):
    ch = proc_mgr.add_chain('MyChain1')

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    readdata = analysis.ReadToDf(name='dflooper1', key='test1', sep='|', reader='csv', usecols=['x', 'y'])
    readdata.path = [data_path] * 3
    readdata.itr_over_files = True
    ch.add_link(readdata)

    # --- this serves as the break statement from this loop.
    #     if dataset test is empty, which can happen as the very last dataset by readdata,
    #     then skip the rest of this chain.
    skipper = core_ops.SkipChainIfEmpty()
    skipper.collectionSet = ['test1']
    skipper.checkAtInitialize = False
    skipper.checkAtExecute = True
    ch.add_link(skipper)

    # --- do something useful with the test dataset here ...
    #     e.g. apply selections, or collect into histograms.

    # --- this serves as the continue statement of the loop. go back to start of the chain.
    #     repeater listens to readdata is there are any more datasets coming. if so, continue the loop.
    repeater = core_ops.RepeatChain()
    # repeat until readdata says halt.
    repeater.listenTo = 'chainRepeatRequestBy_' + readdata.name
    # repeat max of 10 times
    #repeater.maxcount = 10
    ch.add_link(repeater)


# --- example 2: readdata loops over the input files, with file chunking.

if settings.get('do_example2', True):
    ch = proc_mgr.add_chain('MyChain2')

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next 4 lines of the open or next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    readdata = analysis.ReadToDf(name='dflooper2', key='test2', sep='|', reader='csv', usecols=['x', 'y'],
                                 chunksize=chunksize)
    readdata.path = [data_path] * 3
    ch.add_link(readdata)

    # --- this serves as the break statement from this loop.
    #     if dataset test is empty, which can happen as the very last dataset by readdata,
    #     then skip the rest of this chain.
    skipper = core_ops.SkipChainIfEmpty()
    skipper.collectionSet = ['test2']
    skipper.checkAtInitialize = False
    skipper.checkAtExecute = True
    ch.add_link(skipper)

    # --- do something useful with the test dataset here ...
    #     e.g. apply selections, or collect into histograms.

    # querySet = seletions that are applies to incoming_records
    # after selections, only keep column in selectColumns ('a', 'c')
    link = analysis.ApplySelectionToDf(readKey='test2', storeKey='reduced_data', querySet=['x>1'])
    # Any other kwargs given to ApplySelectionToDf are passed on the the
    # pandas query() function.
    ch.add_link(link)

    # --- As an example, will merge reduced datasets back into a single, merged dataframe.
    concat = analysis.DfConcatenator()
    concat.readKeys = ['merged', 'reduced_data']
    concat.storeKey = 'merged'
    concat.ignore_missing_input = True  # in first iteration input 'merged' is missing.
    ch.add_link(concat)

    # --- this serves as the continue statement of the loop. go back to start of the chain.
    repeater = core_ops.RepeatChain()
    # repeat until readdata says halt.
    repeater.listenTo = 'chainRepeatRequestBy_' + readdata.name
    # repeat max of 10 times
    #repeater.maxcount = 10
    ch.add_link(repeater)


# --- print contents of the datastore
proc_mgr.add_chain('Overview')
pds = core_ops.PrintDs(name='End')
pds.keys = ['n_test1', 'n_sum_test1', 'n_test2', 'n_sum_test2', 'test2', 'n_merged']
proc_mgr.get_chain('Overview').add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk301_read_big_data_itr')
