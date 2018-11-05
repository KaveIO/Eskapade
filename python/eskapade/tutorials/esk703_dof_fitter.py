"""Project: Eskapade - A python-based package for data analysis.

Macro: esk201_readdata

Created: 2018/09/04

Description:
    TODO!!!!!

    TODO: write good summary with explanation of choices made

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
import numpy as np

from eskapade import ConfigObject, Chain
from eskapade import data_mimic
from eskapade import process_manager
from eskapade.logger import Logger, LogLevel


logger = Logger()
logger.debug('Now parsing configuration file esk702_dof_fitter')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk702_dof_fitter'
settings['version'] = 0

np.random.seed(42)

ch = Chain('DoFFitter')
ch.logger.log_level = LogLevel.DEBUG

# Usually, DoF = number of bins - number of model parameters. However, in this case, DoF is equal to 2 * the
# number of bins - number of model parameters. Two times the number of bins the reference (data_to_resample) has a
# DoF per bin as well.
# In the case of kernel density estimation, the number of model parameters is not known. Nonetheless, Dof can be
# estimated by fitting a chiˆ2 distribution to a sample of chiˆ2's with DoF as the fitting parameter.
bins = [np.array([-10, 1.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 0.5, 10]), np.array([-10, 1.5, 10]),
        np.array([-100, 0, 100]), np.array([-100, 0, 100]), np.array([-100, 0, 100])]
dof_fitter = data_mimic.DoFFitter(n_obs=100000,
                                  p_unordered=np.array([[0.2, 0.2, 0.3, 0.3], [0.3, 0.7]]),
                                  p_ordered=np.array([[0.1, 0.2, 0.7], [0.15, 0.4, 0.05, 0.3, 0.1]]),
                                  means_stds=np.array([[8, 8, 3], [2, 5, 2]]),
                                  bins=bins,
                                  n_chi2_samples=100,
                                  dof_store_key='dof')
dof_fitter.logger.log_level = LogLevel.DEBUG
ch.add(dof_fitter)

logger.debug('Done parsing configuration file esk702_dof_fitter')
