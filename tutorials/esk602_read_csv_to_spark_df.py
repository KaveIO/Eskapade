# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Macro  : esk602_read_csv_to_spark_df                                         *
# * Created: 2017/05/31                                                          *
# * Description:                                                                 *
# *     Tutorial macro for reading CSV files into a Spark data frame             *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import logging
log = logging.getLogger('macro.esk602_read_csv_to_spark_df')

from eskapade import ConfigObject, ProcessManager
from eskapade.core import persistence
from analytics_engine.spark_analysis import SparkManager
from analytics_engine import spark_analysis

log.debug('Now parsing configuration file esk602_read_csv_to_spark_df')


##########################################################################
# --- minimal analysis information

proc_mgr = ProcessManager()

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk602_read_csv_to_spark_df'
settings['version'] = 0


##########################################################################
# --- start Spark session

spark = proc_mgr.service(SparkManager).create_session(eskapade_settings=settings)


##########################################################################
# --- CSV and data-frame settings

# NB: local file may not be accessible to worker node in cluster mode
file_paths = ['file:' + persistence.io_path('data', settings.io_conf(), 'dummy1.csv'),
              'file:' + persistence.io_path('data', settings.io_conf(), 'dummy2.csv')]
separator = '|'
has_header = True
infer_schema = True
num_partitions = 5
columns = ['date', 'loc', 'x', 'y']


##########################################################################
# --- now set up the chains and links based on configuration flags

# create read link
read_link = spark_analysis.SparkDfReader(name='Reader',
                                         store_key='spark_df',
                                         read_methods=['csv'])

# set CSV read arguments
read_link.read_meth_args['csv'] = (file_paths,)
read_link.read_meth_kwargs['csv'] = dict(sep=separator,
                                         header=has_header,
                                         inferSchema=infer_schema)

if columns:
    # add select function
    read_link.read_methods.append('select')
    read_link.read_meth_args['select'] = tuple(columns)

if num_partitions:
    # add repartition function
    read_link.read_methods.append('repartition')
    read_link.read_meth_args['repartition'] = (num_partitions,)

# add link to chain
proc_mgr.add_chain('Read').add_link(read_link)


##########################################################################

log.debug('Done parsing configuration file esk602_read_csv_to_spark_df')
