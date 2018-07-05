"""Project: Eskapade - A python-based package for data analysis.

Macro: esk201_readdata

Created: 2017/02/17

Description:
    Macro to that illustrates how to open files as pandas datasets.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject, Chain, resources
from eskapade import core_ops, analysis
from eskapade import process_manager
from eskapade.logger import Logger

logger = Logger()

logger.debug('Now parsing configuration file esk701_mimic_data')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk701_mimic_data'
settings['version'] = 0


logger.debug('Done parsing configuration file esk701_mimic_data')
