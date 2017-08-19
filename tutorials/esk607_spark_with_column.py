# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Macro  : esk607_spark_with_column                                            *
# * Created: 2017/06/14                                                          *
# * Description:                                                                 *
# *     Tutorial macro for adding a new column to a Spark dataframe by applying  *
# *     a Spark built-in or user-defined function to a selection of columns      *
# *     in a Spark dataframe.                                                    *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************


import logging
log = logging.getLogger('macro.esk607_spark_with_column')

from pyspark.sql import types
from pyspark.sql import functions

from eskapade import ConfigObject, ProcessManager
from eskapade.core import persistence
from analytics_engine.spark_analysis import SparkManager
from analytics_engine import spark_analysis

log.debug('Now parsing configuration file esk607_spark_with_column')


##########################################################################
# Minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk607_spark_with_column'
settings['version'] = 0


##########################################################################
# Start Spark session

spark = proc_mgr.service(SparkManager).create_session(eskapade_settings=settings)


##########################################################################
# CSV and dataframe settings

# NB: local file may not be accessible to worker node in cluster mode
file_path = ['file:' + persistence.io_path('data', settings.io_conf(), 'dummy1.csv')]


##########################################################################
# Now set up the chains and links based on configuration flags

proc_mgr.add_chain('Read')

# create read link for each data file
read_link = spark_analysis.SparkDfReader(name='ReadFile',
                                         store_key='spark_df',
                                         read_methods=['csv'])

# set CSV read arguments
read_link.read_meth_args['csv'] = (file_path,)
read_link.read_meth_kwargs['csv'] = dict(sep='|', header=True, inferSchema=True)

# add link to chain
proc_mgr.get_chain('Read').add_link(read_link)

# create link to create new column
col_link = spark_analysis.SparkWithColumn(name='UdfPower', read_key=read_link.store_key, store_key='new_spark_df')

# example using a user-defined function and the 'include'-option
col_link.col_select = ['x', 'y']
col_link.col_usage = 'include'
col_link.func = functions.udf(lambda a, b: float(a)**float(b), returnType=types.DoubleType())   # Power of two columns
col_link.new_column = 'pow_xy1'

# add link to chain
proc_mgr.add_chain('AddColumn').add_link(col_link)

# create link to create new column
col_link = spark_analysis.SparkWithColumn(name='BuiltPower', read_key=col_link.store_key, store_key=col_link.store_key)

# example using a built-in Spark-function and the 'exclude'-option
col_link.col_select = ['dummy', 'date', 'loc', 'pow_xy1']
col_link.col_usage = 'exclude'
col_link.func = functions.pow    # Power of two columns
col_link.new_column = 'pow_xy2'

# add link to chain
proc_mgr.get_chain('AddColumn').add_link(col_link)


##########################################################################

log.debug('Done parsing configuration file esk607_spark_with_column')
