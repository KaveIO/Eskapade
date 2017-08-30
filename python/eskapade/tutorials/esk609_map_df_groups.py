# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Macro  : esk609_map_df_groups                                                *
# * Created: 2017/06/20                                                          *
# * Description:                                                                 *
# *     Tutorial macro for applying map functions on groups of rows              *
# *     in Spark data frames                                                     *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import logging

from eskapade import process_manager, ConfigObject, DataStore, spark_analysis
from eskapade.spark_analysis import SparkManager

log = logging.getLogger('macro.esk609_map_df_groups')

log.debug('Now parsing configuration file esk609_map_df_groups')

##########################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk609_map_df_groups'
settings['version'] = 0

##########################################################################
# --- start Spark session

spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)


##########################################################################
# --- helper (map) function

def add_sum_bar(gr):
    """Add sum of "bar" variable to rows in group"""

    rows = list(gr)
    sum_bar = sum(r['bar'] for r in rows)
    return [r + (sum_bar,) for r in rows]


##########################################################################
# --- input data

ds = process_manager.service(DataStore)
rows = [(it, 'foo{:d}'.format(it), (it + 1) / 2.) for it in range(100)]
ds['df'] = spark.createDataFrame(rows, schema=['index', 'foo', 'bar'])

##########################################################################
# --- now set up the chains and links based on configuration flags

# create chain
chain = process_manager.add_chain('Map')

# create a link to convert the data frame into an RDD
conv_lnk = spark_analysis.SparkDfConverter(name='DfConverter',
                                           read_key='df',
                                           store_key='rdd',
                                           output_format='rdd',
                                           preserve_col_names=True)
chain.add_link(conv_lnk)

# create a link to calculate the sum of "bar" for each group of ten rows
map_lnk = spark_analysis.RddGroupMapper(name='Mapper',
                                        read_key='rdd',
                                        store_key='map_rdd',
                                        group_map=sum,
                                        input_map=lambda r: (r['index'] // 10, r['bar']),
                                        flatten_output_groups=False)
chain.add_link(map_lnk)

# create a link to add a column with the sum of "bar" for each group of ten rows
flmap_lnk = spark_analysis.RddGroupMapper(name='FlatMapper',
                                          read_key='rdd',
                                          store_key='flat_map_rdd',
                                          group_map=add_sum_bar,
                                          input_map=lambda r: (r['index'] // 10, r),
                                          result_map=lambda g: g[1],
                                          flatten_output_groups=True)
chain.add_link(flmap_lnk)

##########################################################################

log.debug('Done parsing configuration file esk609_map_df_groups')