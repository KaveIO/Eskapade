"""Project: Eskapade - A python-based package for data analysis.

Macro: tutorial_5

Created: 2017/07/12

Description:
    This tutorial macro illustrates how to define a new probability density
    function (pdf) in RooFit, how to compile it, and how to use it in
    Eskapade to simulate a dataset, fit it, and plot the results.

    For a brief lesson on RooFit, see here:
    https://root.cern.ch/roofit-20-minutes

    This tutorial shows how to build, compile and load a new pdf model.

    Many good RooFit tutorials exist. See $ROOTSYS/tutorials/roofit/
    of your local ROOT installation.
    This tutorial is partially based in RooFit tutorial:
    $ROOTSYS/tutorials/roofit/rf104_classfactory.C

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import sys

import ROOT

from eskapade import process_manager, ConfigObject, root_analysis, Chain
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file tutorial_5.')

###############################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'tutorial_5'
settings['version'] = 0

###############################################################################
# - First create, compile and load your pdf model. We can either create it
#   on the fly or load if it has already been created.
pdf_name = 'MyPdf'
pdf_lib_base = pdf_name + '_cxx'
pdf_lib_ext = '.so'
pdf_lib_name = pdf_lib_base + pdf_lib_ext

if ROOT.gSystem.Load(pdf_lib_name) != 0:
    logger.info('Building and compiling RooFit pdf {name}.', name=pdf_name)
    # building a roofit pdf class called MyPdfV
    ROOT.RooClassFactory.makePdf(pdf_name, "x,A,B", "", "A*fabs(x)+pow(x-B,2)", True, False,
                                 "x:(A/2)*(pow(x.max(rangeName),2)+pow(x.min(rangeName),2))"
                                 "+(1./3)*(pow(x.max(rangeName)-B,3)-pow(x.min(rangeName)-B,3))")
    # compiling this class and loading it into ROOT on the fly.
    ROOT.gROOT.ProcessLineSync(".x {}.cxx+".format(pdf_name))

# --- check existence of class MyPdf in ROOT
logger.info('Now checking existence of ROOT class {name}.', name=pdf_name)
cl = ROOT.TClass.GetClass(pdf_name)
if not cl:
    logger.fatal('Could not find ROOT class {name}. Did you build and compile it correctly?', name=pdf_name)
    sys.exit(1)
else:
    logger.info('Successfully found ROOT class {name}.', name=pdf_name)

###############################################################################

msg = r"""
The plots and latex files produced by this tutorial can be found in dir:
{path}
"""
logger.info(msg, path=settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/report/')

###############################################################################
# --- now set up the chains and links based on configuration flags

# --- generate pdf, simulate, fit, and plot
ch = Chain('WsOps')

# --- 1. define a model by passing strings to the rooworkspace factory
#     For the workspace factory syntax, see:
#     https://root.cern.ch/doc/master/RooFactoryWSTool_8cxx_source.html#l00722
#     For rooworkspace factory examples see:
#     https://root.cern.ch/root/html/tutorials/roofit/rf511_wsfactory_basic.C.html
#     https://root.cern.ch/root/html/tutorials/roofit/rf512_wsfactory_oper.C.html
#     https://root.cern.ch/root/html/tutorials/roofit/rf513_wsfactory_tools.C.html
#     Here we use the pdf class we just created (MyPdfV3), with observable y and parameter A and B,
#     with ranges (-10,10), (0,100) and (-10,10) respectively. The starting values of A and B are
#     10 and 2 respectively.
wsu = root_analysis.WsUtils(name='modeller')
wsu.factory = ['{pdf}::testpdf(y[-10,10],A[10,0,100],B[2,-10,10])'.format(pdf=pdf_name)]
ch.add(wsu)

# --- 2. simulation: 400 records of observable 'y' with pdf 'testpdf' (of type MyPdfV3).
#        the simulated data is stored in the datastore under key 'simdata'
wsu = root_analysis.WsUtils(name='simulater')
wsu.add_simulate(pdf='testpdf', obs='y', num=400, key='simdata')
ch.add(wsu)

# --- 3. fit: perform fit of pdf 'testpdf' to dataset 'simdata'.
#        store the fit result object in the datastore under key 'fit_result'
#        The fit knows from the input dataset that the observable is y, and that
#        the fit parameters are A and B.
wsu = root_analysis.WsUtils(name='fitter')
wsu.pages_key = 'report_pages'
wsu.add_fit(pdf='testpdf', data='simdata', key='fit_result')
ch.add(wsu)

# --- 4. plot the fit result:
#        a. plot the observable 'y' of the the dataset 'simdata' and plot the
#           fitted uncertainty band of the pdf 'model' on top of this.
#           The plot is stored under the key 'simdata_plot'.
#        b. plot the fitted pdf 'model' without uncertainty band on top of the same frame 'simdata_plot'.
#           store the resulting plot in the file 'fit_of_simdata.pdf'
wsu = root_analysis.WsUtils(name='plotter')
wsu.pages_key = 'report_pages'
wsu.add_plot(obs='y', data='simdata', pdf='testpdf', pdf_kwargs={'VisualizeError': 'fit_result', 'MoveToBack': ()},
             key='simdata_plot')
wsu.add_plot(obs='y', pdf='testpdf', output_file='fit_of_simdata.pdf', key='simdata_plot')
ch.add(wsu)

###############################################################################
logger.debug('Done parsing configuration file tutorial_5.')
