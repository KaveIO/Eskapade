"""Project: Eskapade - A python-based package for data analysis.

Macro: esk306_concatenate_reports

Created: 2017/03/28

Description:
    This macro illustrates how to concatenate the reports of several
    visualization links into one big report.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, resources, Chain
from eskapade import analysis, visualization
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk306_concatenate_reports.')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk306_concatenate_reports'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

input_files = resources.fixture('correlated_data.sv.gz')

#########################################################################################
# --- now set up the chains and links based on configuration flags

data = Chain('Data')

# --- 0. readdata keeps on opening the next file in the file list.
#     all kwargs are passed on to pandas file reader.
read_data = analysis.ReadToDf(name='dflooper', key='accounts', reader='csv', sep=' ')
read_data.path = input_files
data.add(read_data)

# --- 1. add data-frame summary link to "Summary" chain
summarizer = visualization.DfSummary(name='Create_stats_overview',
                                     read_key=read_data.key, pages_key='report_pages')
data.add(summarizer)

# --- 2. Fill 2d histogrammar histograms
hf = analysis.HistogrammarFiller()
hf.read_key = 'accounts'
hf.store_key = 'hist'
hf.logger.log_level = LogLevel.DEBUG
hf.columns = [
    ['x1', 'x2'],
    ['x1', 'x3'],
    ['x1', 'x4'],
    ['x1', 'x5'],
    ['x2', 'x3'],
    ['x2', 'x4'],
    ['x2', 'x5'],
    ['x3', 'x4'],
    ['x3', 'x4'],
    ['x4', 'x5']]
hf._unit_bin_specs = {'bin_width': 0.2, 'bin_offset': 0.0}
data.add(hf)

hs = visualization.DfSummary(name='HistogramSummary1', read_key=hf.store_key, pages_key='report_pages')
data.add(hs)

# --- 3. make visualizations of correlations
corr_link = visualization.CorrelationSummary(name='correlation_summary',
                                             read_key=read_data.key, pages_key='report_pages')
data.add(corr_link)

#########################################################################################

logger.debug('Done parsing configuration file esk306_concatenate_reports.')
