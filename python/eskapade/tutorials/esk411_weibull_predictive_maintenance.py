"""Project: Eskapade - A python-based package for data analysis.

Macro: esk411_weibull_predictive_maintenance

Created: 2017/03/27

Description:
    Macro illustrates how to fit several Weibull distributions to a falling
    time difference distribution, indicating times between maintenance.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Licence:
    Redistribution and use in source and binary forms, with or without
    modification, are permitted according to the terms listed in the file
    LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import process_manager
from eskapade import root_analysis
from eskapade.core import persistence
from eskapade.logger import Logger
from eskapade.root_analysis import roofit_utils

# make sure Eskapade RooFit library is loaded
roofit_utils.load_libesroofit()

import ROOT
from ROOT import RooFit

logger = Logger()

logger.debug('Now parsing configuration file esk411_weibull_predictive_maintenance')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk411_weibull_predictive_maintenance'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

The plots and latex report produced by link WsUtils can be found in dir:
{path}
"""
logger.info(msg, path=persistence.io_path('results_data', 'report'))

settings['generate'] = True
# settings['read_data'] = not settings['generate']
settings['model'] = True
settings['process'] = True
settings['fit_plot'] = True
settings['summary'] = True

fitpdf = 'sum3pdf'
n_percentile_bins = 300

#########################################################################################
# --- now set up the chains and links based on configuration flags

if settings['model']:
    # --- generate pdf
    ch = Chain('Model')

    # --- 1. define a model
    wsu = root_analysis.WsUtils(name='modeller')
    factory = ["RooWeibull::wb1(t[0,110000000],a1[0.93,0,2],b1[2.2e-4,0,1e-3])",
               "RooWeibull::wb2(t,a2[0.61,0,2],b2[1.1e-5,0,1e-3])",
               "RooWeibull::wb3(t,a3[0.43,0,2],b3[4.7e-7,0,1e-3])",
               "RooWeibull::wb4(t,a4[0.43,0,2],b4[2.2e-7,0,1e-3])",
               "SUM::sum2pdf(N1[580000,0,2e6]*wb1,N2[895000,0,2e6]*wb2)",
               "SUM::sum3pdf(N1[580000,0,2e6]*wb1,N2[895000,0,2e6]*wb2,N3[150500,0,2e6]*wb3)",
               "SUM::sum4pdf(N1[580000,0,2e6]*wb1,N2[895000,0,2e6]*wb2,N3[150500,0,2e6]*wb3,N4[1e5,0,2e6]*wb4)"]
    wsu.factory += factory
    ch.add(wsu)

if settings['generate']:
    # --- generate pdf
    ch = Chain('Generate')

    wsu = root_analysis.WsUtils(name='generator')
    wsu.add_simulate(pdf=fitpdf, obs='t', num=1625500, key='rds')
    ch.add(wsu)

# # --- example of how to import a roodataset from a root file
# if settings['read_data']:
#     ch = Chain('Data')
#     read_data = root_analysis.ReadFromRootFile()
#     read_data.path = '/opt/eskapade/data/tsv_renamed_data.root'
#     read_data.keys = ['rds']
#     ch.add(read_data)

if settings['process']:
    ch = Chain('ProcessData')

    # --- a. rebinning configuration, for (faster) binned fit
    pb = root_analysis.RooFitPercentileBinning()
    pb.read_key = 'rds'
    pb.var_number_of_bins = {'t': n_percentile_bins}
    pb.binning_name = 'percentile'
    ch.add(pb)

    # --- b. create rebinned roodatahist for fitting below
    conv = root_analysis.ConvertRooDataSet2RooDataHist()
    conv.read_key = 'rds'
    conv.store_key = 'binnedData'
    conv.columns = ['t']
    conv.binning_name = 'percentile'
    ch.add(conv)

if settings['fit_plot']:
    ch = Chain('FitAndPlot')

    # --- 1. fit: perform fit of pdf 'fitpdf' to dataset 'binnedData'.
    #        store the fit result object in the datastore under key 'fit_result'
    wsu = root_analysis.WsUtils(name='fitter', pages_key='weibull_fit_report')
    wsu.add_fit(pdf=fitpdf, data='binnedData', key='fit_result')
    ch.add(wsu)

    # --- 2. plot the fit result
    wsu = root_analysis.WsUtils(name='plotter1', pages_key='weibull_fit_report')
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot1', bins=100)
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot1', pdf_args=(
        RooFit.Components('wb1'), RooFit.LineColor(ROOT.kRed), RooFit.LineStyle(ROOT.kDashed)))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot1', pdf_args=(
        RooFit.Components('wb3'), RooFit.LineColor(ROOT.kGreen), RooFit.LineStyle(ROOT.kDashed)))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot1',
                 pdf_args=(RooFit.Components('wb2'), RooFit.LineColor(ROOT.kAzure), RooFit.LineStyle(ROOT.kDashed)),
                 output_file='fit_of_time_difference_full_range.pdf', logy=True, miny=1)
    ch.add(wsu)

    wsu = root_analysis.WsUtils(name='plotter2', pages_key='weibull_fit_report')
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot2', bins=100, plot_range=(0, 3e6))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot2', pdf_args=(
        RooFit.Components('wb1'), RooFit.LineColor(ROOT.kRed), RooFit.LineStyle(ROOT.kDashed)))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot2', pdf_args=(
        RooFit.Components('wb3'), RooFit.LineColor(ROOT.kGreen), RooFit.LineStyle(ROOT.kDashed)))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot2',
                 pdf_args=(RooFit.Components('wb2'), RooFit.LineColor(ROOT.kAzure), RooFit.LineStyle(ROOT.kDashed)),
                 output_file='fit_of_time_difference_medium_range.pdf', logy=True, miny=10)
    ch.add(wsu)

    wsu = root_analysis.WsUtils(name='plotter3', pages_key='weibull_fit_report')
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot3', bins=100, plot_range=(0, 20000))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot3', pdf_args=(
        RooFit.Components('wb1'), RooFit.LineColor(ROOT.kRed), RooFit.LineStyle(ROOT.kDashed)))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot3', pdf_args=(
        RooFit.Components('wb3'), RooFit.LineColor(ROOT.kGreen), RooFit.LineStyle(ROOT.kDashed)))
    wsu.add_plot(obs='t', data='rds', pdf=fitpdf, key='plot3',
                 pdf_args=(RooFit.Components('wb2'), RooFit.LineColor(ROOT.kAzure), RooFit.LineStyle(ROOT.kDashed)),
                 output_file='fit_of_time_difference_short_range.pdf', logy=True, miny=100)
    ch.add(wsu)

if settings['summary']:
    ch = Chain('Summary')

    # print contents of the workspace
    pws = root_analysis.PrintWs()
    ch.add(pws)

#########################################################################################


logger.debug('Done parsing configuration file esk411_weibull_predictive_maintenance')
