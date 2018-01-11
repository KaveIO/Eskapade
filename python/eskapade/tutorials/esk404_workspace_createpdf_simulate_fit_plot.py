"""Project: Eskapade - A python-based package for data analysis.

Macro: esk404_workspace_createpdf_simulate_fit_plot

Created: 2017/03/27

Description:
    Macro illustrates how do basic statistical data analysis with roofit,
    by making use of the rooworkspace functionality.

    For a brief lesson on RooFit, see here:
    https://root.cern.ch/roofit-20-minutes

    The example shows how to define a pdf, simulate data, fit this data,
    and then plot the fit result.

    The generated data is converted to a dataframe and the contents is plotted
    with a default plotter link.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import core_ops, visualization, root_analysis
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger('macro.esk404_workspace_createpdf_simulate_fit_plot')

logger.debug('Now parsing configuration file esk404_workspace_createpdf_simulate_fit_plot')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk404_workspace_createpdf_simulate_fit_plot'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['generate_fit_plot'] = True
settings['summary'] = True

#########################################################################################
# --- now set up the chains and links based on configuration flags

if settings['generate_fit_plot']:
    # --- generate pdf, simulate, fit, and plot
    ch = Chain('WsOps')

    # --- 1. define a model by passing strings to the rooworkspace factory
    #     For the workspace factory syntax, see:
    #     https://root.cern.ch/doc/master/RooFactoryWSTool_8cxx_source.html#l00722
    #     For rooworkspace factory examples see:
    #     https://root.cern.ch/root/html/tutorials/roofit/rf511_wsfactory_basic.C.html
    #     https://root.cern.ch/root/html/tutorials/roofit/rf512_wsfactory_oper.C.html
    #     https://root.cern.ch/root/html/tutorials/roofit/rf513_wsfactory_tools.C.html
    wsu = root_analysis.WsUtils(name='modeller')
    wsu.factory = ["Gaussian::sig1(x[-10,10],mean[5,0,10],0.5)",
                   "Gaussian::sig2(x,mean,1)",
                   "Chebychev::bkg(x,{a0[0.5,0.,1],a1[-0.2,-1,1]})",
                   "SUM::sig(sig1frac[0.8,0.,1.]*sig1,sig2)",
                   "SUM::model(bkgfrac[0.5,0.,1.]*bkg,sig)"]
    ch.add(wsu)

    # --- 2. simulation: 1000 records of observable 'x' with pdf 'model'.
    #        in case pdf covers several observables use comma-separated string obs='x,y,z'
    #        the simulated data is stored in the datastore under key 'simdata'
    wsu = root_analysis.WsUtils(name='simulater')
    wsu.add_simulate(pdf='model', obs='x', num=1000, key='simdata')
    ch.add(wsu)

    # --- 3. fit: perform fit of pdf 'model' to dataset 'simdata'.
    #        store the fit result object in the datastore under key 'fit_result'
    wsu = root_analysis.WsUtils(name='fitter')
    wsu.add_fit(pdf='model', data='simdata', key='fit_result')
    ch.add(wsu)

    # --- 4. plot the fit result:
    #        a. plot the observable 'x' of the the dataset 'simdata' and plot the
    #           fitted uncertainy band of the pdf 'model' on top of this.
    #           The plot is stored under the key 'simdata_plot'.
    #        b. plot the fitted pdf 'model' without uncertainty band on top of the same frame 'simdata_plot'.
    #           store the resulting plot in the file 'fit_of_simdata.pdf'
    wsu = root_analysis.WsUtils(name='plotter')
    wsu.add_plot(obs='x', data='simdata', pdf='model', pdf_kwargs={'VisualizeError': 'fit_result', 'MoveToBack': ()},
                 key='simdata_plot')
    wsu.add_plot(obs='x', pdf='model', output_file='fit_of_simdata.pdf', key='simdata_plot')
    ch.add(wsu)

    # --- 5. convert simulated data to dataframe
    ch = Chain('Conversion2')

    rds2df = root_analysis.ConvertRooDataSet2DataFrame()
    rds2df.read_key = 'simdata'
    rds2df.store_key = 'df_simdata'
    rds2df.remove_original = True
    ch.add(rds2df)

if settings['summary']:
    ch = Chain('Summary')

    # print contents of the workspace
    pws = root_analysis.PrintWs()
    ch.add(pws)

    # print contents of datastore
    pds = core_ops.PrintDs(name='pds2')
    # pds.keys = ['accounts']
    ch.add(pds)

    # --- make a summary document of simulated dataframe
    summarizer = visualization.DfSummary(name='Create_stats_overview',
                                         read_key=rds2df.store_key)
    ch.add(summarizer)

#########################################################################################

logger.debug('Done parsing configuration file esk404_workspace_createpdf_simulate_fit_plot')
