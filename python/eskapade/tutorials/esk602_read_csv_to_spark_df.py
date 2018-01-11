"""Project: Eskapade - A python-based package for data analysis.

Macro: esk602_read_csv_to_spark_df

Created: 2017/05/31

Description:
    Tutorial macro for reading CSV files into a Spark data frame

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, resources, spark_analysis, Chain
from eskapade.logger import Logger
from eskapade.spark_analysis import SparkManager

logger = Logger()

logger.debug('Now parsing configuration file esk602_read_csv_to_spark_df.')

##########################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk602_read_csv_to_spark_df'
settings['version'] = 0

##########################################################################
# --- start Spark session

spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)

##########################################################################
# --- CSV and data-frame settings

# NB: local file may not be accessible to worker node in cluster mode
file_paths = ['file:' + resources.fixture('dummy1.csv'),
              'file:' + resources.fixture('dummy2.csv')]

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
read = Chain('Read')
read.add(read_link)

##########################################################################

logger.debug('Done parsing configuration file esk602_read_csv_to_spark_df.')
