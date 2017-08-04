# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : tutorial_3                                                         *
# * Created: 2017/07/12                                                                  *
# * Description:                                                                   *
# *      This tutorial macro illustrates how to define a new probability density
# *      function (pdf) in RooFit, how to compile it, and how to use it in Eskapade to
# *      simulate a dataset, fit it, and plot the results.
# *
# *      This tutorial shows how to build, compile and load a new pdf model.
# *
# *      Many good RooFit tutorials exist. See $ROOTSYS/tutorials/roofit/
# *      of your local ROOT installation.
# *      This tutorial is partially based in RooFit tutorial:
# *      $ROOTSYS/tutorials/roofit/rf104_classfactory.C
# *
# * Authors:                                                                       *
# *      KPMG The Netherlands, Big Data & Advanced Analytics team
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging, sys
log = logging.getLogger('macro.tutorial_3')

import ROOT

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis, root_analysis, visualization

log.debug('Now parsing configuration file tutorial_3')

###############################################################################
# --- first create, compile and load your pdf model

msg = r"""
<start of instructions>

TUTORIAL 3: ROOFIT

Move to the directory:

% cd $ESKAPADE/cxx/roofit/src/

Start an interactive python session and type:

>>> import ROOT
>>> ROOT.RooClassFactory.makePdf("MyPdfV2","x,A,B","","A*fabs(x)+pow(x-B,2)")

This command creates a RooFit skeleton probability density function class named MyPdfV2,
with the variable x,a,b and the given formula expression.

Also type:

>>> ROOT.RooClassFactory.makePdf("MyPdfV3","x,A,B","","A*fabs(x)+pow(x-B,2)",True,False, \
    "x:(A/2)*(pow(x.max(rangeName),2)+pow(x.min(rangeName),2))+(1./3)*(pow(x.max(rangeName)-B,3)-pow(x.min(rangeName)-B,3))")

This creates the RooFit p.d.f. class MyPdfV3, with the variable x,a,b and the given formula expression,
and the given expression for analytical integral over x.

Exit python (Ctrl-D) and type:

% ls -l MyPdf*

You will see two cxx files and two header files. Open the file MyPdfV2.cxx.
You should see an evaluate() method in terms of x,a and b with the formula expression we provided.

Now open the file MyPdfV3.cxx. This also contains the method analyticalIntegral() with the expresssion
for the analytical integral over x that we provided.

If no analytical integral has been provided, as in MyPdfV2, RooFit will try to try to compute the integral
itself. (Of course this is a costly operation.) If you wish, since we know the analytical integral for MyPdfV2,
go ahead and edit MyPdfV2.cxx to add the expression of the analytical integral to the class.

As another example of a simple pdf class, take a look at the expressions in the file:
$ESKAPADE/cxx/roofit/src/RooWeibull.cxx

Now move the header files to their correct location:

% mv MyPdfV*.h $ESKAPADE/cxx/roofit/include/

To make sure that these classes get picked up in Eskapade roofit libary, open the file:

$ESKAPADE/cxx/roofit/dict/LinkDef.h

and add the lines:

#pragma link C++ class MyPdfV2+;
#pragma link C++ class MyPdfV3+;

Finally, let's compile the c++ code of these classes:

% cd $ESKAPADE
% make install

You should see the compiler churning away, processing several existing classes but also MyPdfV2 and MyPdfV3.

We are now able to open the Eskapade roofit library, so we can use these classes in python:

>>> from eskapade.root_analysis import roofit_utils
>>> roofit_utils.load_libesroofit()

In fact, this code is used right below.

<end of instructions>
"""
log.info(msg)

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'tutorial_3'
settings['version'] = 0

#########################################################################################

# --- 0. make sure Eskapade RooFit library is loaded

# --- NOT USED BY DEFAULT: Example of how to compile and load a roofit class on the fly
if 'onthefly' in settings and settings['onthefly']:
    pdf_name = 'MyPdfV3'
    log.info('Building and compiling RooFit pdf %s' % pdf_name)
    # building a roofit pdf class called MyPdfV3
    ROOT.RooClassFactory.makePdf(pdf_name, "x,A,B","","A*fabs(x)+pow(x-B,2)",True,False, \
                                 "x:(A/2)*(pow(x.max(rangeName),2)+pow(x.min(rangeName),2))+(1./3)*(pow(x.max(rangeName)-B,3)-pow(x.min(rangeName)-B,3))")
    # compiling this class and loading it into ROOT on the fly.
    ROOT.gROOT.ProcessLineSync(".x %s.cxx+" % pdf_name)

# --- load and compile the Eskapade roofit library
from eskapade.root_analysis import roofit_utils
roofit_utils.load_libesroofit()

# --- check existence of class MyPdfV3 in ROOT
pdf_name = 'MyPdfV3'
log.info('Now checking existence of ROOT class %s' % pdf_name)
cl = ROOT.TClass.GetClass(pdf_name)
if not cl:
    log.critical('Could not find ROOT class %s. Did you build and compile it correctly?' % pdf_name)
    sys.exit(1)
else:
    log.info('Successfully found ROOT class %s' % pdf_name)

#########################################################################################

msg = r"""
The plots and latex files produced by this tutorial can be found in dir:
%s
""" % (settings['resultsDir'] + '/' + settings['analysisName'] + '/data/v0/report/')
log.info(msg)

#########################################################################################
# --- now set up the chains and links based on configuration flags

# --- generate pdf, simulate, fit, and plot
ch = proc_mgr.add_chain('WsOps')

# --- 1. define a model by passing strings to the rooworkspace factory
#     for details on rooworkspace factory see:
#     https://root.cern.ch/root/html/tutorials/roofit/rf511_wsfactory_basic.C.html
#     Here we use the pdf class we just created (MyPdfV3), with observable y and parameter A and B,
#     with ranges (-10,10), (0,100) and (-10,10) respectively. The starting values of A and B are
#     10 and 2 respectively.
wsu = root_analysis.WsUtils(name = 'modeller')
wsu.factory = ["MyPdfV3::testpdf(y[-10,10],A[10,0,100],B[2,-10,10])"]
ch.add_link(wsu)

# --- 2. simulation: 400 records of observable 'y' with pdf 'testpdf' (of type MyPdfV3).
#        the simulated data is stored in the datastore under key 'simdata'
wsu = root_analysis.WsUtils(name = 'simulater')
wsu.add_simulate(pdf='testpdf', obs='y', num=400, key='simdata')
ch.add_link(wsu)

# --- 3. fit: perform fit of pdf 'testpdf' to dataset 'simdata'.
#        store the fit result object in the datastore under key 'fit_result'
#        The fit knows from the input dataset that the observable is y, and that
#        the fit parameters are A and B.
wsu = root_analysis.WsUtils(name = 'fitter')
wsu.pages_key='report_pages'
wsu.add_fit(pdf='testpdf', data='simdata', key='fit_result')
ch.add_link(wsu)

# --- 4. plot the fit result:
#        a. plot the observable 'y' of the the dataset 'simdata' and plot the
#           fitted uncertainy band of the pdf 'model' on top of this.
#           The plot is stored under the key 'simdata_plot'.
#        b. plot the fitted pdf 'model' without uncertainty band on top of the same frame 'simdata_plot'.
#           store the resulting plot in the file 'fit_of_simdata.pdf'
wsu = root_analysis.WsUtils(name = 'plotter')
wsu.pages_key='report_pages'
wsu.add_plot(obs='y', data='simdata', pdf='testpdf', pdf_kwargs={'VisualizeError': 'fit_result', 'MoveToBack': ()}, key='simdata_plot')
wsu.add_plot(obs='y', pdf='testpdf', output_file='fit_of_simdata.pdf', key='simdata_plot')
ch.add_link(wsu)

#########################################################################################

log.debug('Done parsing configuration file tutorial_3')

