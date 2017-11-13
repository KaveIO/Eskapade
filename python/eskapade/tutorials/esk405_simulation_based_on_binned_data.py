"""Project: Eskapade - A python-based package for data analysis.

Macro: esk405_simulation_based_on_binned_data

Created: 2017/04/06

Description:
    Imagine the situation where you wish to simulate an existing dataset, where
    you want the simulated dataset to have the same features and characteristics
    as the input dataset, including all known correlations between observables,
    possibly non-linear.
    The input data can have both categorical and continuous (float) observables.

    This macro shows how this simulation can be done with roofit, by building a
    (potentially large) n-dimensional roofit histogram of all requested
    input observables with the RooDataHistFiller link.

    Be careful not to blow up the total number of bins, which grows exponentially
    with the number of input observables. We can control this by setting the number of
    bins per continuous observable, or by setting the maximum total number of bins
    allowed in the histogram, which scales down the number of allowed bins in each
    continuous observable. Realize that, the more bins one has, the more input data
    is needed to will all bins with decent statistics.

    This macro has two settings, controlled with settings['high_num_dims'].
    When false, the roodatahist contains 3 observables, of which two continous and
    1 categorical. When true, the roodatahist is 6 dimensional, with 4 continous
    observables and 2 categorical ones. The latter example is slower, but works fine!

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import core_ops, analysis, root_analysis
from eskapade import process_manager
from eskapade import resources
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk405_simulation_based_on_binned_data')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk405_simulation_based_on_binned_data'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['high_num_dims'] = False

input_files = [resources.fixture('mock_accounts.csv.gz')]

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = Chain('Data')

# --- 0. read input data
read_data = analysis.ReadToDf(name='dflooper', key='accounts', reader='csv')
read_data.path = input_files
ch.add(read_data)

# --- 1. add the record factorizer
#     Here the columns dummy and loc of the input dataset are factorized
#     e.g. x = ['apple', 'tree', 'pear', 'apple', 'pear'] becomes the column:
#     x = [0, 1, 2, 0, 2]
#     By default, the mapping is stored in a dict under key: 'map_'+store_key+'_to_original'
fact = analysis.RecordFactorizer(name='rf1')
fact.columns = ['isActive', 'eyeColor', 'favoriteFruit', 'gender']
fact.read_key = 'accounts'
fact.inplace = True
fact.sk_map_to_original = 'to_original'
fact.sk_map_to_factorized = 'to_factorized'
fact.logger.log_level = LogLevel.DEBUG
ch.add(fact)

# --- 2. Fill a roodatahist with the contents of the dataframe
df2rdh = root_analysis.RooDataHistFiller()
df2rdh.read_key = read_data.key
df2rdh.store_key = 'rdh_' + read_data.key
df2rdh.store_key_vars = 'rdh_vars'
df2rdh.store_key_cats = 'rdh_cats'
df2rdh.map_to_factorized = 'to_factorized'
if settings['high_num_dims']:
    df2rdh.columns = ['transaction', 'latitude', 'longitude', 'age', 'eyeColor', 'favoriteFruit']
else:
    df2rdh.columns = ['longitude', 'age', 'eyeColor']
# be careful not to blow up the total number of bins.
# do this by setting the maximum total number of bins allowed.
df2rdh.n_max_total_bins = 1e6
# a histogram-based pdf is created out of the roodatahist object
# we use this pdf below to simulate a new dataset with the same properties as the original
df2rdh.create_hist_pdf = 'hpdf_Ndim'
# all output is stored in the workspace, not datastore
df2rdh.into_ws = True
ch.add(df2rdh)

# --- Print overview
pws = root_analysis.PrintWs()
ch.add(pws)

pds = core_ops.PrintDs()
ch.add(pds)

# --- 3. resimulate the data with the created hist-pdf, and plot these data and the pdf
ch = Chain('WsOps')
wsu = root_analysis.WsUtils()
wsu.add_simulate(pdf='hpdf_Ndim', obs='rdh_vars', num=10000, key='simdata')
wsu.add_plot(obs='age', data='simdata', pdf='hpdf_Ndim', output_file='test.pdf',
             pdf_kwargs={'ProjWData': ('rdh_cats', 'simdata')})
ch.add(wsu)

#########################################################################################

logger.debug('Done parsing configuration file esk405_simulation_based_on_binned_data')
