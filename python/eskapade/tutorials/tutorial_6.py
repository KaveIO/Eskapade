"""Project: Eskapade - A python-based package for data analysis.

Macro: Tutorial_6

Created: 2017/06/14

Description:
    Macro illustrates basic setup of chains and links with Apache Spark,
    by showing: how to open and run over a dataset,
    apply transformations to it, and plot the results.

Authors:
    KPMG Advanced Analytics & Big Data team.

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, Chain, spark_analysis
from eskapade.logger import Logger

logger = Logger()

#########################################################################################

msg = r"""

Be sure to download the input dataset:

$ wget https://s3-eu-west-1.amazonaws.com/kpmg-eskapade-share/data/LAozone.data
"""
logger.info(msg)

#########################################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'tutorial_6'

#########################################################################################
# --- setup Spark

process_manager.service(spark_analysis.SparkManager).create_session(include_eskapade_modules=True)

#########################################################################################
# --- analysis values, settings, helper functions, configuration flags.

VAR_LABELS = dict(doy='Day of year', date='Date', vis='Visibility', vis_km='Visibility')
VAR_UNITS = dict(vis='mi', vis_km='km')


def comp_date(day):
    """Get date/time from day of year."""
    import pandas as pd
    return pd.Timestamp('1976-01-01') + pd.Timedelta('{:d}D'.format(day - 1))


def mi_to_km(dist):
    """Convert miles to kilometres."""
    return dist * 1.60934


#########################################################################################
# --- now set up the chains and links based on configuration flags

# create first chain
data = Chain('Data')

# # add data-frame reader to "Data" chain
# reader = spark_analysis.SparkDfReader()
# data.add(reader)

# # add conversion functions to "Data" chain
# transform = spark_analysis.SparkWithColumn()
# data.add(transform)

# create second chain
summary = Chain('Summary')

# # fill spark histograms
# histo = spark_analysis.SparkHistogrammarFiller()
# summary.add(histo)

# # add data-frame summary link to "Summary" chain
# summarizer = visualization.DfSummary(name='Create_stats_overview', read_key=histo.store_key,
#                                      var_labels=VAR_LABELS, var_units=VAR_UNITS)
# summary.add(summarizer)


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
# Hint: use eskapade_generate_link --dir python/eskapade/spark_analysis SparkDfPrinter.

# The answers can be found in the Tutorial Apache Spark section of the documentation.
