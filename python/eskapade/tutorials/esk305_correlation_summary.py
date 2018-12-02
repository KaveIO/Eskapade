"""Project: Eskapade - A python-based package for data analysis.

Macro: esk305_correlation_summary

Created: 2017/04/04

Description:
    Macro to demonstrate generating correlation heatmaps


Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, resources, Chain
from eskapade import analysis, visualization
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk305_correlation_summary.')

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk305_correlation_summary'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

settings['input_path'] = resources.fixture('correlated_data.sv.gz')
settings['reader'] = 'csv'
settings['separator'] = ' '
settings['correlations'] = ['pearson', 'kendall', 'spearman', 'correlation_ratio', 'phik']

#########################################################################################
# --- now set up the chains and links based on configuration flags

# create chains
data = Chain('Data')

# load data
reader = analysis.ReadToDf(name='reader',
                           path=settings['input_path'],
                           sep=settings['separator'],
                           key='input_data',
                           reader=settings['reader'])

data.add(reader)

# make visualizations of correlations
summary = Chain('Summary')

corr_link = visualization.CorrelationSummary(name='correlation_summary',
                                             read_key='input_data',
                                             store_key='correlations',
                                             methods=settings['correlations'])

summary.add(corr_link)

#########################################################################################

logger.debug('Done parsing configuration file esk305_correlation_summary.')
