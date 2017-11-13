"""Project: Eskapade - A python-based package for data analysis.

Macro: esk203_apply_func_to_pandas_df

Created: 2017/02/23

Description:
    Illustrates link that calls basic apply() to columns of a pandas dataframes
    See for more information pandas documentation:

    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.apply.html

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""
from numpy.random import randn
from pandas import DataFrame

from eskapade import analysis, core_ops, process_manager, ConfigObject, DataStore, Chain
from eskapade.logger import Logger, LogLevel

logger = Logger()

logger.debug('Now parsing configuration file esk203_apply_func_to_pandas_df')

#########################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk203_apply_func_to_pandas_df'
settings['version'] = 0


#########################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

# functions to be applied below

def square(x):
    """Square value."""
    return x * x


def sqrt_abs(x):
    """Take square root of absolute value."""
    from math import sqrt
    return sqrt(abs(x))


conv_funcs = [{'func': square, 'colin': 'x', 'colout': 'xx'},
              {'func': sqrt_abs, 'colin': 'y', 'colout': 'yy'}]

# generate a dummy dataframe and add to datastore
# to this dataset selections are applied below, during link execution.

# NB: realize that, normally, such a dataframe is read or constructed on the fly
# during link execution.

df = DataFrame(randn(20, 2), columns=list('xy'))

ds = process_manager.service(DataStore)
ds['incoming_data'] = df

#########################################################################################
# --- now set up the chains and links based on configuration flags

data_prep = Chain('DataPrep')

# query_set = seletions that are applies to incoming_records
# after selections, only keep column in select_columns ('a', 'c')
# add conversion functions to "Data" chain
link = analysis.ApplyFuncToDf(name='Transform',
                              read_key='incoming_data',
                              store_key='transformed_data',
                              apply_funcs=conv_funcs)
# Any other kwargs given to ApplyFuncToDf are passed on the the
# pandas query() function.
link.logger.log_level = LogLevel.DEBUG
data_prep.add(link)

link = core_ops.DsObjectDeleter()
link.deletion_keys = ['incoming_data']
data_prep.add(link)

link = core_ops.PrintDs()
link.keys = ['transformed_data']
data_prep.add(link)

#########################################################################################

logger.debug('Done parsing configuration file esk203_apply_func_to_pandas_df')
