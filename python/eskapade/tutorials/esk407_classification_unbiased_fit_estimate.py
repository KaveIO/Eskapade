"""Project: Eskapade - A python-based package for data analysis.

Macro: esk407_classification_unbiased_fit_estimate

Created: 2017/04/12

Description:
    This macro illustrates how to get an unbiased estimate of the number of
    high risk clients, by doing a template fit to data.

    Assume a classifier has been trained and optimized to separate high-risk from
    low risk clients. But the high- to low-risk ratio in data is very low and unknown,
    so the false-positive rate is non-negligible.

    We can use templates of the score of the ML classifier of the high- and low-risk
    testing samples to (at least) get an unbiased estimate of the total number of
    high-risk clients. This is done by fitting the (unbiased) testing templates
    to the score distribution in the actual dataset. The shapes differentiate
    the number of high- and low-risk clients.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain
from eskapade import core_ops, root_analysis
from eskapade import process_manager
from eskapade.logger import Logger
from eskapade.root_analysis import roofit_utils

# make sure Eskapade RooFit library is loaded
roofit_utils.load_libesroofit()

import ROOT
from ROOT import RooFit

logger = Logger()

logger.debug('Now parsing configuration file esk407_classification_unbiased_fit_estimate')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk407_classification_unbiased_fit_estimate'
settings['version'] = 0

#########################################################################################
# --- now set up the chains and links based on configuration flags

# --- generate pdf, simulate, fit, and plot
ch = Chain('WsOps')

# 1. simulate output score of machine learning classifier
wsu = root_analysis.WsUtils(name='DataSimulator')
wsu.factory = ["expr::trans('@0-@1',{score[0,1],1})",
               "RooExponential::high_risk(trans,10)",
               "RooPolynomial::low_risk(score,{-0.4,-0.4})",
               "SUM::model(low_risk_frac[0.95,0.,1.]*low_risk,high_risk)"]
wsu.add_simulate(pdf='high_risk', obs='score', num=1000, key='unbiased_high_risk_testdata', into_ws=True)
wsu.add_simulate(pdf='low_risk', obs='score', num=1000, key='unbiased_low_risk_testdata', into_ws=True)
wsu.add_simulate(pdf='model', obs='score', num=1000, key='data', into_ws=True)
wsu.add_plot(obs='score', data='data', pdf='model', key='simplot')
wsu.add_plot(obs='score', pdf='model',
             pdf_args=(RooFit.Components('low_risk'), RooFit.LineColor(ROOT.kRed), RooFit.LineStyle(ROOT.kDashed)),
             output_file='data_with_generator_model.pdf', key='simplot')
ch.add(wsu)

# 2a. turn data into roofit histograms
wsu = root_analysis.WsUtils(name='HistMaker')


def make_histograms(w):
    """Make histogram."""
    # Need to be imported here as well, otherwise throws: name 'ROOT' is not defined.
    import ROOT  # noqa
    from eskapade.root_analysis.decorators.roofit import ws_put
    w.var('score').setBins(40)
    high_risk_hist = ROOT.RooDataHist('high_risk_hist', 'high_risk_hist', ROOT.RooArgSet(w.var('score')),
                                      w.data('unbiased_high_risk_testdata'))
    low_risk_hist = ROOT.RooDataHist('low_risk_hist', 'low_risk_hist', ROOT.RooArgSet(w.var('score')),
                                     w.data('unbiased_low_risk_testdata'))
    ws_put(w, high_risk_hist)
    ws_put(w, low_risk_hist)


wsu.apply = [make_histograms]
ch.add(wsu)

# 2b. classification algorithms often return rather spiky scoring or probability distributions
#     meaning: non-continuous distributions.
#     Sometimes the validation samples has records that give a score that does not fit in one of the
#     peaks of the testing data.
#     Here we fix the testing histograms  (default 0.01 per bin)
#     this makes sure that we can use these histograms as fit templates to fit validation records that fall
#     inside those bins.
wsu = root_analysis.WsUtils(name='TemplateFixer')


def nonzero_templates(w):
    """Fix histogram to make sure that all bins have a non-zero value."""
    def nonzero_hist(rdh, minimum_value=0.01):
        """Fix non-zero bins."""
        if rdh.numEntries() == 0:
            return
        rdh.get(0)
        for i in range(rdh.numEntries()):
            rdh.get(i)
            weight = rdh.weight()
            if weight > 0:
                continue
            rdh.set(minimum_value, 0)

    high_risk_hist = w.data('high_risk_hist')
    low_risk_hist = w.data('low_risk_hist')
    nonzero_hist(high_risk_hist)
    nonzero_hist(low_risk_hist)


wsu.apply = [nonzero_templates]
ch.add(wsu)

# 3. create pdfs out of roofit histograms
wsu = root_analysis.WsUtils(name='TemplateMaker')
wsu.factory = ["RooHistPdf::high_risk_pdf({score}, high_risk_hist)",
               "RooHistPdf::low_risk_pdf({score}, low_risk_hist)",
               "SUM::hist_model(N_low_risk[1000,0,2000]*low_risk_pdf, N_high_risk[0,0,1000]*high_risk_pdf)"]
wsu.add_plot(obs='score', data='unbiased_high_risk_testdata', pdf='high_risk_pdf',
             output_file='high_risk_data_template.pdf')
wsu.add_plot(obs='score', data='unbiased_low_risk_testdata', pdf='low_risk_pdf',
             output_file='low_risk_data_template.pdf')
ch.add(wsu)

# 4. fit combined pdf to actual dataset and show results
wsu = root_analysis.WsUtils(name='TemplateFitter')
wsu.add_fit(pdf='hist_model', data='data', key='fit_result')
wsu.add_plot(obs='score', data='data', pdf='hist_model',
             pdf_args=(RooFit.MoveToBack(),),
             pdf_kwargs={'VisualizeError': 'fit_result'},
             key='data_plot')
wsu.add_plot(obs='score', pdf='hist_model', key='data_plot')
wsu.add_plot(obs='score', pdf='hist_model',
             pdf_args=(RooFit.Components('low_risk_pdf'), RooFit.LineColor(ROOT.kRed), RooFit.LineStyle(ROOT.kDashed)),
             output_file='template_fit_to_data.pdf', key='data_plot')
ch.add(wsu)

# 5. Print overview
pws = root_analysis.PrintWs()
ch.add(pws)

pds = core_ops.PrintDs()
ch.add(pds)

#########################################################################################

logger.debug('Done parsing configuration file esk407_classification_unbiased_fit_estimate')
