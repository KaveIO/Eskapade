"""Project: Eskapade - A python-based package for data analysis.

Macro: esk302_histogram_filler_plotter

Created: 2017/02/17

Description:
    Macro that illustrates how to loop over multiple (possibly large!)
    datasets in chunks, in each loop fill a (common) histogram, and plot the
    final histogram.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import analysis, core_ops, process_manager, resources, visualization, ConfigObject, Chain
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk302_histogram_filler_plotter.')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk302_histogram_filler_plotter'
settings['version'] = 0

#########################################################################################

msg = r"""

The plots and latex files produced by link hist_summary can be found in dir:
{path}
"""
logger.info(msg, path=settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/report/')

# --- Analysis configuration flags.
#     E.g. use these flags turn on or off certain chains with links.
#     by default all set to false, unless already configured in
#     configobject or vars()

settings['do_loop'] = True

chunk_size = 400

#########################################################################################
# --- create dummy example dataset, which is read in below

input_files = [resources.fixture('mock_accounts.csv.gz'),
               resources.fixture('mock_accounts.csv.gz')]


def to_date(x):
    """Convert to timestamp."""
    import pandas as pd
    try:
        ts = pd.Timestamp(x.split()[0])
        x = ts
    except Exception:
        logger.warning('Date conversion failed!')
    return x


#########################################################################################
# --- now set up the chains and links, based on configuration flags

# --- example 2: readdata loops over the input files, with file chunking.

if settings['do_loop']:
    ch = Chain('Data')

    # --- a loop is set up in the chain MyChain.
    #     we iterate over (chunks of) the next file in the list until the iterator is done.
    #     then move on to the next chain (Overview)

    # --- readdata keeps on opening the next 400 lines of the open or next file in the file list.
    #     all kwargs are passed on to pandas file reader.
    read_data = analysis.ReadToDf(name='dflooper', key='rc', reader='csv')
    read_data.chunksize = chunk_size
    read_data.path = input_files
    ch.add(read_data)

    # add conversion functions to "Data" chain
    # here, convert column 'registered', an integer, to an actual timestamp.
    conv_funcs = [{'func': to_date, 'colin': 'registered', 'colout': 'date'}]
    transform = analysis.ApplyFuncToDf(name='Transform', read_key=read_data.key,
                                       apply_funcs=conv_funcs)
    ch.add(transform)

    # --- As an example, will fill histogram iteratively over the file loop
    vc = analysis.ValueCounter()
    vc.read_key = 'rc'
    vc.store_key_hists = 'hist'
    vc.logger.log_level = LogLevel.DEBUG
    # colums that are picked up to do value_counting on in the input dataset
    # note: can also be 2-dim: ['isActive','age']
    # in this example, the rest are one-dimensional histograms
    vc.columns = ['date', 'isActive', 'age', 'eyeColor', 'gender', 'company', 'latitude', 'longitude',
                  ['isActive', 'age']]
    # binning is apply to all input columns that are numeric or timestamps.
    # default binning is: bin_width = 1, bin_offset = 0
    # for timestamps, default binning is:
    # { 'bin_width': np.timedelta64(30,'D'), 'bin_offset': np.datetime64('2010-01-04') } }
    vc.bin_specs = {'longitude': {'bin_width': 5, 'bin_offset': 0},
                    'latitude': {'bin_width': 5, 'bin_offset': 0}}
    # as we are running in a loop, store the resulting histograms in the finalize() of the link,
    # after having looped through all (small) datasets.
    vc.store_at_finalize = True
    ch.add(vc)

    # --- this serves as the continue statement of the loop. go back to start of the chain.
    repeater = core_ops.RepeatChain()
    # repeat until readdata says halt.
    repeater.listen_to = 'chainRepeatRequestBy_' + read_data.name
    ch.add(repeater)

    link = core_ops.DsObjectDeleter()
    link.keep_only = ['hist', 'n_sum_rc']
    ch.add(link)

# --- print contents of the datastore
overview = Chain('Overview')
pds = core_ops.PrintDs(name='End')
pds.keys = ['n_sum_rc']
overview.add(pds)

# --- make a nice summary report of the created histograms
hist_summary = visualization.DfSummary(name='HistogramSummary',
                                       read_key=vc.store_key_hists)
overview.add(hist_summary)

#########################################################################################

logger.debug('Done parsing configuration file esk302_histogram_filler_plotter.')
