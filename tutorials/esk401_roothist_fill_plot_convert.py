# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk401_roothist_fill_plot_convert                                     *
# * Created: 2017/03/28                                                            *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *
# * Description:
# *
# * This macro illustrates how to fill 1-3 dimensional root histograms from a
# * pandas dataframe. In turn, these histogram are: 2) plotted,
# * 3) converted to a roofit histogram (roodatahist), and 4) converted to a
# * roofit dataset (roodataset).
# *                                         :                                      *
# * Licence:
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk401_roothist_fill_plot_convert')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis, visualization, root_analysis

log.debug('Now parsing configuration file esk401_roothist_fill_plot_convert')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk401_roothist_fill_plot_convert'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['read_data'] = True
settings['make_plot'] = True
settings['convert_to_rdh'] = True
settings['convert_to_rds'] = True

input_files = [os.environ['ESKAPADE'] + '/data/correlated_data.sv.gz']

#########################################################################################

msg = r"""

The plots and latex files produced by link hist_summary can be found in dir:
%s
""" % (settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/report/')
log.info(msg)

#########################################################################################
# --- now set up the chains and links based on configuration flags

if settings['read_data']:
    ch = proc_mgr.add_chain('Data')

    # --- 0. read input data
    readdata = analysis.ReadToDf(name='reader', key='correlated_data', reader='csv', sep=' ')
    readdata.path = input_files
    ch.add_link(readdata)

    # --- 1. Fill root histograms
    #        For now, RootHistFiller only accepts numeric observables
    hf = root_analysis.RootHistFiller()
    # columns is a list of single observables or sub-lists in case of multi-dimensional histograms
    hf.columns = ['x1', 'x2', 'x3', 'x4', 'x5', ['x1', 'x2'], ['x2', 'x3'], ['x4', 'x5']]
    hf.read_key = 'correlated_data'
    hf.store_key = 'hist'
    hf.var_min_value = {'x2': -5, 'x3': -5, 'x4': -5, 'x5': -5}
    hf.var_max_value = {'x2': 5, 'x3': 5, 'x4': 5, 'x5': 5}
    ch.add_link(hf)

if settings['make_plot']:
    ch = proc_mgr.add_chain('Plotting')

    # --- 2. make a nice summary report of the created histograms
    hs = visualization.DfSummary(name='HistogramSummary', read_key=hf.store_key)
    ch.add_link(hs)

if settings['convert_to_rdh']:
    ch = proc_mgr.add_chain('Convert1')

    # --- 3. convert a root histogram to a RooDataHist object
    h2rdh = root_analysis.ConvertRootHist2RooDataHist()
    h2rdh.read_key = 'x1'
    h2rdh.hist_dict_key = 'hist'
    h2rdh.create_hist_pdf = 'hpdf'
    #h2rds.into_ws = True
    ch.add_link(h2rdh)

if settings['convert_to_rds']:
    ch = proc_mgr.add_chain('Convert2')

    # --- 4. convert a histogram to a RooDataSet object
    h2rds = root_analysis.ConvertRootHist2RooDataSet()
    h2rds.read_key = 'x2:x3'
    h2rds.hist_dict_key = 'hist'
    #h2rds.into_ws = True
    ch.add_link(h2rds)


# --- summary
ch = proc_mgr.add_chain('Summary')

pds = core_ops.PrintDs()
pds.keys = ['hist', 'n_rdh_x1', 'n_rds_x2_vs_x3']
ch.add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk401_roothist_fill_plot_convert')
