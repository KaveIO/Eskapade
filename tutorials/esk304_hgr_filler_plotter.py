# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk304_hgr_filler_plotter                                                  *
# * Created: 2017/02/17                                                            *
# * Description:                                                                   *
# *      Macro that illustrates how to loop over multiple (possibly large!)        *
# *      datasets in chunks, in each loop fill a (common) histogram, and plot the  * 
# *      final histogram.
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team.                                                       *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging, os
log = logging.getLogger('macro.esk304_hgr_filler_plotter')

import tempfile
import numpy as np
from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis, visualization

log.debug('Now parsing configuration file esk304_hgr_filler_plotter.py')

#########################################################################################
# --- minimal analysis information
settings = ProcessManager().service(ConfigObject)
settings['analysisName'] = 'esk304_hgr_filler_plotter'
settings['version'] = 0

#########################################################################################

msg = r"""

The plots and latex files produced by link hist_summary can be found in dir:
%s
""" % (settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/report/')
log.info(msg)

# --- Analysis configuration flags.
#     E.g. use these flags turn on or off certain chains with links.
#     by default all set to false, unless already configured in
#     configobject or vars()

settings['do_loop'] = True 
settings['do_plotting'] = True

chunksize = 400

#########################################################################################
# --- create dummy example dataset, which is read in below

input_files = [os.environ['ESKAPADE'] + '/data/esk304_data_mock_accounts.csv.gz'] * 1

def to_date(x):
    import pandas as pd
    try:
        ts = pd.Timestamp(x.split()[0])
        return ts
    except:
        pass
    return x

#########################################################################################
# --- now set up the chains and links, based on configuration flags

procMgr = ProcessManager()

# --- example 2: readdata loops over the input files, with file chunking.

if settings['do_loop']:
    ch = procMgr.add_chain('Data')

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next 400 lines of the open or next file in the file list.
    #     all kwargs are passed on to pandas file reader. 
    readdata = analysis.ReadToDf(name = 'dflooper', key = 'rc', reader='csv')
    readdata.chunksize = chunksize
    readdata.path = input_files
    ch.add_link(readdata)

    # add conversion functions to "Data" chain
    # here, convert column 'registered', an integer, to an actual timestamp.
    conv_funcs = [{'func': to_date, 'colin': 'registered', 'colout': 'date'}]
    transform = analysis.ApplyFuncToDf(name='Transform', read_key=readdata.key, 
                                       apply_funcs=conv_funcs)
    ch.add_link(transform)

    # --- As an example, will fill histogram iteratively over the file loop
    hf = analysis.HistogrammarFiller()
    hf.read_key = 'rc'
    hf.store_key = 'hist'
    hf.set_log_level(logging.DEBUG)
    # colums that are picked up to do value_counting on in the input dataset
    # note: can also be 2-dim: ['isActive','age']
    # in this example, the rest are one-dimensional histograms
    hf.columns = ['date','isActive','age','eyeColor','gender','company','latitude','longitude',['isActive','age'],['latitude','longitude']] 
    # binning is apply to all input columns that are numeric or timestamps.
    # default binning is: bin_width = 1, bin_offset = 0
    # for timestamps, default binning is: { 'bin_width': np.timedelta64(30,'D'),
    #                                       'bin_offset': np.datetime64('2010-01-04') } }
    hf.bin_specs = { 'longitude': { 'bin_width' : 5, 'bin_offset' : 0  }, \
                     'latitude': { 'bin_width' : 5, 'bin_offset' : 0 } }
    ch.add_link(hf) 

    # --- this serves as the continue statement of the loop. go back to start of the chain.
    repeater = core_ops.RepeatChain()
    # repeat until readdata says halt.
    repeater.listenTo = 'chainRepeatRequestBy_'+readdata.name
    ch.add_link(repeater)

    link = core_ops.DsObjectDeleter()
    link.keepOnly = ['hist','n_sum_rc','rc']
    ch.add_link(link)

    
# --- print contents of the datastore
if settings['do_plotting']:
    ch = procMgr.add_chain('Overview')
    pds = core_ops.PrintDs(name='End')
    pds.keys = ['n_sum_rc']
    ch.add_link(pds)
    
    # --- make a nice summary report of the created histograms
    hist_summary = visualization.HistogrammarSummary(name='HistogramSummary', \
                                                     read_key=hf.store_key)
    ch.add_link(hist_summary)

#########################################################################################

log.debug('Done parsing configuration file esk304_hgr_filler_plotter.py')
