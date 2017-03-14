# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Macro  : esk110_code_profiling                                                 *
# * Created: 2017/02/26                                                            *
# * Description:                                                                   *
# *      Macro to demo how to run eskapade with code profiling turned on 
# *      
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import logging
log = logging.getLogger('macro.esk110_code_profiling')

from eskapade import ConfigObject, ProcessManager
from eskapade import core_ops, analysis

log.debug('Now parsing configuration file esk110_code_profiling')

#########################################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk110_code_profiling'
settings['version'] = 0

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

msg = r"""

Your can profile the speed of your analysis functions by running with the cmd line option: -P
You will need to select the order in which the profile output is shown on screen. 
For example: -P cumulative

After running, this prints out a long list of all functions called, including the time it 
took to run each of of them, sorted in the way you have specified.
"""
log.info(msg)

# turn on code profiling in the ConfigObject
# turn off this line if you want to experiment with the profiling from the cmd line!
settings['doCodeProfiling'] = 'cumulative'

#########################################################################################
# --- now set up the chains and links based on configuration flags

# Just look at the code profiling output on the screen!
# The output is sorted by cumulative processing time.

# For more documentation on code profiling, see:
# https://docs.python.org/2/library/profile.html

#########################################################################################

log.debug('Done parsing configuration file esk110_code_profiling')

