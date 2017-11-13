"""Project: Eskapade - A python-based package for data analysis.

Macro: esk605_create_spark_df

Created: 2017/06/08

Description:
    Tutorial macro for creating Spark data frames from
    different types of input data

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from collections import OrderedDict as odict

import pandas as pd

from eskapade import process_manager, ConfigObject, DataStore, spark_analysis, Chain
from eskapade.logger import Logger
from eskapade.spark_analysis import SparkManager

logger = Logger()

logger.debug('Now parsing configuration file esk605_create_spark_df.')

##########################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk605_create_spark_df'
settings['version'] = 0

##########################################################################
# --- start Spark session

spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)

##########################################################################
# --- input data

ds = process_manager.service(DataStore)
schema = odict([('index', int), ('foo', str), ('bar', float)])
ds['rows'] = [(it, 'foo{:d}'.format(it), (it + 1) / 2.) for it in range(100)]  # list of tuples
ds['rdd'] = spark.sparkContext.parallelize(ds['rows'])  # RDD
ds['df'] = spark.createDataFrame(ds['rdd'], schema=list(schema.keys()))  # Spark data frame
ds['pd'] = pd.DataFrame(ds['rows'], columns=schema.keys())  # Pandas data frame


##########################################################################
# --- now set up the chains and links based on configuration flags

# define function to set number of partitions
def set_num_parts(df, max_num_parts):
    """Set number of partitions.

    :param max_num_parts maximum number of partitions
    """
    if df.rdd.getNumPartitions() > max_num_parts:
        df = df.repartition(max_num_parts)
    return df


# create chain and data-frame-creator links
chain = Chain('Create')
for ds_key, lnk_schema in zip(('rows', 'rdd', 'df', 'pd'), (list(schema.keys()), schema, schema, None)):
    # create data-frame-creator link
    lnk = spark_analysis.SparkDfCreator(name='df_creator_{}'.format(ds_key),
                                        read_key=ds_key,
                                        store_key='{}_df'.format(ds_key),
                                        schema=lnk_schema,
                                        process_methods=['filter', set_num_parts, 'cache'])

    # set post-process-method arguments
    lnk.process_meth_args['filter'] = ('index > 19',)  # select rows with index > 19
    lnk.process_meth_kwargs[set_num_parts] = dict(max_num_parts=2)  # set maximum number of partitions to 2

    # add link to chain
    chain.add(lnk)

##########################################################################

logger.debug('Done parsing configuration file esk605_create_spark_df.')
