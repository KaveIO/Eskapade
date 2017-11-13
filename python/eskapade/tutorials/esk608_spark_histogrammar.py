"""Project: Eskapade - A python-based package for data analysis.

Macro: esk608_spark_histogrammar

Created: 2017/05/31

Description:
    Tutorial macro for making histograms of a Spark dataframe

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, visualization, resources, spark_analysis, Chain
from eskapade.logger import Logger, LogLevel
from eskapade.spark_analysis import SparkManager

logger = Logger()

logger.debug('Now parsing configuration file esk608_spark_histogrammar')

##########################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk608_spark_histogrammar'
settings['version'] = 0

##########################################################################
# --- start Spark session

spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)

##########################################################################
# --- CSV and data-frame settings

file_paths = ['file:' + resources.fixture('dummy.csv')]
separator = '|'
has_header = True
infer_schema = True
num_partitions = 4
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

output = Chain('Output')

# fill spark histograms
hf = spark_analysis.SparkHistogrammarFiller()
hf.read_key = read_link.store_key
hf.store_key = 'hist'
hf.logger.log_level = LogLevel.DEBUG
# colums that are picked up to do value_counting on in the input dataset
# note: can also be 2-dim: ['x','y']
# in this example, the rest are one-dimensional histograms
hf.columns = ['x', 'y', 'loc', ['x', 'y'], 'date']
output.add(hf)

# make a nice summary report of the created histograms
hist_summary = visualization.DfSummary(name='HistogramSummary',
                                       read_key=hf.store_key)
output.add(hist_summary)

###########################################################################
# --- the end

logger.debug('Done parsing configuration file esk608_spark_histogrammar')
