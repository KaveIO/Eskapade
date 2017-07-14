# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk306_concatenate_reports                                             *
# * Created: 2017/03/28                                                            *
# *                                                                                *
# * Description:
# *
# * This macro illustrates how to concatenate the reports of several
# * visualization links into one big report.
# *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Licence:
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk306_concatenate_reports')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis, root_analysis, visualization

log.debug('Now parsing configuration file esk306_concatenate_reports')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk306_concatenate_reports'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

input_files = [os.environ['ESKAPADE'] + '/data/correlated_data.sv.gz']

#########################################################################################
# --- now set up the chains and links based on configuration flags

ch = proc_mgr.add_chain('Data')

# --- 0. readdata keeps on opening the next file in the file list.
#     all kwargs are passed on to pandas file reader.
readdata = analysis.ReadToDf(name='dflooper', key='accounts', reader='csv', sep=' ')
readdata.path = input_files
ch.add_link(readdata)

# --- 1. add data-frame summary link to "Summary" chain
summarizer = visualization.DfSummary(name='Create_stats_overview',
                                     read_key=readdata.key, pages_key='report_pages')
ch.add_link(summarizer)

# --- 2. Fill 2d histogrammar histograms
hf = analysis.HistogrammarFiller()
hf.read_key = 'accounts'
hf.store_key = 'hist'
hf.set_log_level(logging.DEBUG)
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
ch.add_link(hf)

hs = visualization.DfSummary(name='HistogramSummary1', read_key=hf.store_key, pages_key='report_pages')
ch.add_link(hs)

# --- 3. make visualizations of correlations
corr_link = visualization.CorrelationSummary(name='correlation_summary',
                                             read_key=readdata.key, pages_key='report_pages')
ch.add_link(corr_link)

#########################################################################################

log.debug('Done parsing configuration file esk306_concatenate_reports')
