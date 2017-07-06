# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk408_classification_error_propagation_after_fit                     *
# * Created: 2017/04/12                                                            *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team                                                        *
# * 
# * Description:
# *
# * This macro continues on the idea in esk407_classification_unbiased_fit_estimate.
# * It illustrates how to assign statistically motivated probabilities to
# * high risk clients, by doing a template fit to data, and - based on this -
# * calculating the probability and uncertainty on this for each client.
# *
# * Assume a classifier has been trained and optimized to separate high-risk from
# * low risk clients. But the high- to low-risk ratio in data is very low and unknown,
# * so the false-positive rate is non-negligible. 
# *
# * We can use templates of the score of the ML classifier of the high- and low-risk
# * testing samples to (at least) score the probability that someone is a high risk
# * client, in light of the fact that most clients with a high classifier score will
# * in fact be false-positive low risk clients.
# *
# * In addition to the probability, the algorithm assigns as statistical uncertainty
# * to each probability.
# * The total sum of these probabilities equals the number of estimated high-risk
# * clients, as also obtained in example esk407.
# *
# * Licence:
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk408_classification_error_propagation_after_fit')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, visualization, root_analysis

import ROOT
from ROOT import RooFit

log.debug('Now parsing configuration file esk408_classification_error_propagation_after_fit')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk408_classification_error_propagation_after_fit'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

#########################################################################################
# --- now set up the chains and links based on configuration flags

# make sure Eskapade RooFit library is loaded
root_analysis.roofit_utils.load_libesroofit()

# --- generate pdf, simulate, fit, and plot
ch = proc_mgr.add_chain('WsOps')

# 1. simulate output score of machine learning classifier
wsu = root_analysis.WsUtils(name = 'DataSimulator')
wsu.factory = ["RooGaussian::high_risk(score[0,1],1,0.15)",
               "RooPolynomial::low_risk(score,{-0.3,-0.3})",
               "SUM::model(frac[0.1,0.,1.]*high_risk,low_risk)"]
wsu.add_simulate(pdf='model', obs='score', num=500, key='data', into_ws=True)
wsu.add_fit(pdf='model', data='data', key='fit_result', into_ws=True)
wsu.add_plot(obs='score', data='data', pdf='model', key='simplot')
wsu.add_plot(obs='score', pdf='model', \
             pdf_args=(RooFit.Components('low_risk'), RooFit.LineColor(ROOT.kRed), \
                       RooFit.LineStyle(ROOT.kDashed)),
             output_file='data_with_generator_model.pdf', key='simplot')
ch.add_link(wsu)

ch = proc_mgr.add_chain('SignalPValue')

# 2. plot signal probability
wsu = root_analysis.WsUtils(name = 'SignalProbability')
wsu.factory = ["expr::high_risk_pvalue('@0*@1/@2',{frac,high_risk,model})"]
wsu.add_plot(obs='score', func='high_risk_pvalue',
             func_args=(RooFit.MoveToBack(),),
             func_kwargs={'VisualizeError': ('fit_result')},
             key='ratio_plot')
wsu.add_plot(obs='score', func='high_risk_pvalue', output_file='high_risk_probability.pdf', key='ratio_plot')
ch.add_link(wsu)

# 3. calculate p-values and uncertainties thereon
ape = root_analysis.AddPropagatedErrorToRooDataSet()
ape.from_ws = True
ape.data = 'data'
ape.function = 'high_risk_pvalue'
ape.fit_result = 'fit_result'
ape.function_error_name = 'high_risk_perror'
ch.add_link(ape)

ch = proc_mgr.add_chain('Summary')

# 4. convert back to df and plot
rds2df = root_analysis.ConvertRooDataSet2DataFrame()
rds2df.read_key = 'data'
rds2df.from_ws = True
rds2df.store_key = 'df_pvalues'
ch.add_link(rds2df)

# 5. make summary plots
summarizer = visualization.DfSummary(name='Create_stats_overview',
                                     read_key=rds2df.store_key)
ch.add_link(summarizer)

# 6. Print overview
pws = root_analysis.PrintWs()
ch.add_link(pws)

pds = core_ops.PrintDs()
ch.add_link(pds)

#########################################################################################

log.debug('Done parsing configuration file esk408_classification_error_propagation_after_fit')
