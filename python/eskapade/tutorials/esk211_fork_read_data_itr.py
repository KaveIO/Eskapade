"""Project: Eskapade - A python-based package for data analysis.

Macro: esk211_fork_read_data_itr

Created: 2017/02/17

Description:
    Macro to that illustrates how to loop over multiple (possibly large!)
    datasets in chunks.
    Similar to esk209, but here the reading of dataset is forked into different processes.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
import pandas as pd

from eskapade import analysis, core_ops, process_manager, resources, ConfigObject, Chain
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk211_fork_read_data_itr')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk211_fork_read_data_itr'
settings['version'] = 0

# no need to set this normally, but illustrates how to throttle the number of concurrent processes.
# default is set to number of available cpu cores.
process_manager.num_cpu = 4

#########################################################################################

# when chunking through an input file, pick up only N lines in each iteration.
chunk_size = 5

#########################################################################################
# --- Set path of data
data_path = resources.fixture('dummy.csv')

#########################################################################################
# --- now set up the chains and links, based on configuration flags

# --- example 2: readdata loops over the input files, with file chunking.

if settings.get('do_example2', True):
    ch = Chain('MyChain2')
    ch.n_fork = 10

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next 4 lines of the open or next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    read_data = analysis.ReadToDf(name='dflooper2', key='test2', sep='|', reader='csv', usecols=['x', 'y'],
                                  chunksize=chunk_size)
    read_data.path = [data_path] * 3
    ch.add(read_data)

    # --- do something useful with the test dataset here ...
    #     e.g. apply selections, or collect into histograms.

    # query_set = seletions that are applies to incoming_records
    # after selections, only keep column in select_columns ('a', 'c')
    link = analysis.ApplySelectionToDf(read_key='test2', store_key='reduced_data', query_set=['x>1'])
    # Any other kwargs given to ApplySelectionToDf are passed on the the
    # pandas query() function.
    ch.add(link)

    dc = core_ops.ForkDataCollector()
    dc.keys = [{'key_ds': link.store_key, 'func': pd.concat}]
    ch.add(dc)

    # --- this serves as the continue statement of the loop. go back to start of the chain.
    repeater = core_ops.RepeatChain()
    # repeat until readdata says halt.
    repeater.listen_to = 'chainRepeatRequestBy_' + read_data.name
    ch.add(repeater)

# --- print contents of the datastore
overview = Chain('Overview')

pds = core_ops.PrintDs(name='End')
pds.keys = ['reduced_data']
overview.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk211_fork_read_data_itr')


if __name__ == "__main__":
    import escore
    escore.eskapade_run()
