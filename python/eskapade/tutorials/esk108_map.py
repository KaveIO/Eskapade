"""Project: Eskapade - A python-based package for data analysis.

Macro: esk108_map

Created: 2017/02/20

Description:
    Macro to illustrate how input lines can be read in,
    processed, and reprinted. E.g. for use in map reduce application.
    Use in combination with: esk108_reduce

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import ConfigObject
from eskapade import process_manager, resources

#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# turning on this flag, the datastore and configuration are not written out to disk
# at the end of the program.

settings = process_manager.service(ConfigObject)
settings['do_map'] = True
settings['doNotStoreResults'] = True

#########################################################################################
# --- now parse the follow-up macro

# the flag doNotStoreResults is picked up when parsing the following macro
process_manager.execute_macro(resources.tutorial('esk108_eventlooper.py'))
