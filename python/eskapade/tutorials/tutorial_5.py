# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Macro  : Tutorial_5                                                          *
# * Created: 2017/06/14                                                          *
# * Description:                                                                 *
# *     Macro illustrates basic setup of chains and links with Apache Spark,     *
# *     by showing: how to open and run over a dataset,                          *
# *     apply transformations to it, and plot the results.                       *
# *                                                                              *
# * Authors:                                                                     *
# *     KPMG Big Data team.                                                      *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import logging

from analytics_engine.spark_analysis import SparkManager

from eskapade import process_manager as proc_mgr, ConfigObject, resources

log = logging.getLogger('macro.Tutorial_5')

#########################################################################################

msg = r"""

Be sure to download the input dataset:

$ wget -P $ESKAPADE/data/ https://statweb.stanford.edu/~tibs/ElemStatLearn/datasets/LAozone.data
"""
log.info(msg)

#########################################################################################
# --- minimal analysis information

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'Tutorial_5'

#########################################################################################
# --- setup Spark

proc_mgr.service(SparkManager).get_or_create_session()

#########################################################################################
# --- analysis values, settings, helper functions, configuration flags.

DATA_FILE_PATH = resources.fixture('LAozone.data')
VAR_LABELS = dict(doy='Day of year', date='Date', vis='Visibility', vis_km='Visibility')
VAR_UNITS = dict(vis='mi', vis_km='km')


def comp_date(day):
    """Get date/time from day of year"""

    import pandas as pd
    return pd.Timestamp('1976-01-01') + pd.Timedelta('{:d}D'.format(day - 1))


def mi_to_km(dist):
    """Convert miles to kilometres"""

    return dist * 1.60934


#########################################################################################
# --- now set up the chains and links based on configuration flags

# create first chain
proc_mgr.add_chain('Data')

## add data-frame reader to "Data" chain
# reader = spark_analysis.SparkDfReader()
# proc_mgr.get_chain('Data').add_link(reader)

## add conversion functions to "Data" chain
# transform = spark_analysis.SparkWithColumn()
# proc_mgr.get_chain('Data').add_link(transform)

# create second chain
proc_mgr.add_chain('Summary')

## fill spark histograms
# histo = spark_analysis.SparkHistogrammarFiller()
# proc_mgr.get_chain('Summary').add_link(histo)

## add data-frame summary link to "Summary" chain
# summarizer = visualization.DfSummary(name='Create_stats_overview', read_key=histo.store_key,
#                                     var_labels=VAR_LABELS, var_units=VAR_UNITS)
# proc_mgr.get_chain('Summary').add_link(summarizer)


#########################################################################################
# --- exercises
#
# 1.
# Adapt the reader link such that it reads in the LAozone.data CSV with the SparkReader.
# Hint: see the example macro tutorials/esk602_read_csv_to_spark_df.py.

# 2.
# Adapt the transform link such that it creates two additional columns:
# - 'date' which uses 'doy' as input and the 'comp_data' function
# - 'vis_km' which uses 'vis' as input and the 'mi_to_km' function
# The functions need to be of type pyspark.sql.functions.udf
# Hint: for each output column, a separate transform link is needed.

# 3.
# Add the SparkHistogrammarFiller link that creates histograms of the columns
# 'vis', 'vis_km', 'doy', and 'data'.
# Hint: uncomment the DfSummary link to produce output in the ./results directory.

# 4.
# Create a new link 'SparkDfPrinter' in the 'Summary' chain that
# takes a Spark dataframe from the DataStore and shows 42 rows.
# Hint: use make_link.sh python/eskapade/spark_analysis SparkDfPrinter.

# The answers can be found in the Tutorial Apache Spark section of the documentation.
