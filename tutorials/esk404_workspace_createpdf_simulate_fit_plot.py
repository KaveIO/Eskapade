# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk404_workspace_createpdf_simulate_fit_plot                          *
# * Created: 2017/03/27                                                            *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *
# * Description:
# *
# * Macro illustrates how do basic statistical data analysis with roofit, 
# * by making use of the rooworkspace functionality.
# *
# * The example shows how to define a pdf, simulate data, fit this data,
# * and then plot the fit result.
# * 
# * The generated data is converted to a dataframe and the contents is plotted
# * with a default plotter link.
# *
# * Licence:
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk404_workspace_createpdf_simulate_fit_plot')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, visualization, root_analysis

log.debug('Now parsing configuration file esk404_workspace_createpdf_simulate_fit_plot')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk404_workspace_createpdf_simulate_fit_plot'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['generate_fit_plot'] = True
settings['summary'] = True

#########################################################################################
# --- now set up the chains and links based on configuration flags

# make sure Eskapade RooFit library is loaded
root_analysis.roofit_utils.load_libesroofit()

if settings['generate_fit_plot']:
    # --- generate pdf, simulate, fit, and plot
    ch = proc_mgr.add_chain('WsOps')

    # --- 1. define a model by passing strings to the rooworkspace factory
    #     for details on rooworkspace factory see:
    #     https://root.cern.ch/root/html/tutorials/roofit/rf511_wsfactory_basic.C.html
    wsu = root_analysis.WsUtils(name = 'modeller')
    wsu.factory = ["Gaussian::sig1(x[-10,10],mean[5,0,10],0.5)",
                   "Gaussian::sig2(x,mean,1)",
                   "Chebychev::bkg(x,{a0[0.5,0.,1],a1[-0.2,-1,1]})",
                   "SUM::sig(sig1frac[0.8,0.,1.]*sig1,sig2)",
                   "SUM::model(bkgfrac[0.5,0.,1.]*bkg,sig)"]
    ch.add_link(wsu)

    # --- 2. simulation: 1000 records of observable 'x' with pdf 'model'.
    #        in case pdf covers several observables use comma-separated string obs='x,y,z'
    #        the simulated data is stored in the datastore under key 'simdata'
    wsu = root_analysis.WsUtils(name = 'simulater')
    wsu.add_simulate(pdf='model', obs='x', num=1000, key='simdata')
    ch.add_link(wsu)

    # --- 3. fit: perform fit of pdf 'model' to dataset 'simdata'.
    #        store the fit result object in the datastore under key 'fit_result'
    wsu = root_analysis.WsUtils(name = 'fitter')
    wsu.add_fit(pdf='model', data='simdata', key='fit_result')
    ch.add_link(wsu)

    # --- 4. plot the fit result:
    #        a. plot the observable 'x' of the the dataset 'simdata' and plot the
    #           fitted uncertainy band of the pdf 'model' on top of this.
    #           The plot is stored under the key 'simdata_plot'.
    #        b. plot the fitted pdf 'model' without uncertainty band on top of the same frame 'simdata_plot'.
    #           store the resulting plot in the file 'fit_of_simdata.pdf'
    wsu = root_analysis.WsUtils(name = 'plotter')
    wsu.add_plot(obs='x', data='simdata', pdf='model', pdf_kwargs={'VisualizeError': 'fit_result', 'MoveToBack': ()}, key='simdata_plot')
    wsu.add_plot(obs='x', pdf='model', output_file='fit_of_simdata.pdf', key='simdata_plot')
    ch.add_link(wsu)

    # --- 5. convert simulated data to dataframe
    ch = proc_mgr.add_chain('Conversion2')

    rds2df = root_analysis.ConvertRooDataSet2DataFrame()
    rds2df.read_key = 'simdata'
    rds2df.store_key = 'df_simdata'
    rds2df.remove_original = True
    ch.add_link(rds2df)

if settings['summary']:
    proc_mgr.add_chain('Summary')

    # print contents of the workspace
    pws = root_analysis.PrintWs()
    ch.add_link(pws)

    # print contents of datastore
    pds = core_ops.PrintDs(name='pds2')
    #pds.keys = ['accounts']
    ch.add_link(pds)

    # --- make a summary document of simulated dataframe
    summarizer = visualization.DfSummary(name='Create_stats_overview',
                                         read_key=rds2df.store_key)
    proc_mgr.get_chain('Summary').add_link(summarizer)

#########################################################################################

log.debug('Done parsing configuration file esk404_workspace_createpdf_simulate_fit_plot')
