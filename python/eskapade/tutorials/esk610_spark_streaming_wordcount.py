# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Macro  : esk610_spark_streaming                                              *
# * Created: 2017/05/31                                                          *
# * Description:                                                                 *
# *     Tutorial macro running Spark Streaming word count example in Eskapade,   *
# *     derived from:                                                            *
# *                                                                              *
# *     https://spark.apache.org/docs/latest/streaming-programming-guide.html    *
# *                                                                              *
# *     Counts words in UTF8 encoded, '\n' delimited text received from a        *
# *     stream every second. The stream can be from either files or network.     *
# *                                                                              *
# *     For example:                                                             *
# *                                                                              *
# *     i)  to run locally using tcp stream, use netcat and type random words:   *
# *           `$ nc -lk 9999`                                                    *
# *         and then run the example (in a second terminal):                     *
# *           `$ run_eskapade -c stream_type='tcp' \                             *
# *                  tutorials/esk610_spark_streaming_wordcount.py`              *
# *                                                                              *
# *         NB: hostname and port can be adapted in this macro                   *
# *                                                                              *
# *     ii) to run locally using file stream, create dummy files in /tmp:        *
# *          `$ for ((i=0; i<=100; i++)); do \                                   *
# *                 echo "Hello world" > /tmp/dummy_$(printf %05d ${i}); \       *
# *                 sleep 0.1; \                                                 *
# *             done`                                                            *
# *        and then run the example (in a second terminal):                      *
# *          `$ run_eskapade -c stream_type='file' \                             *
# *                 tutorials/esk610_spark_streaming_wordcount.py`               *
# *                                                                              *
# *         NB: only new files in /tmp are processed, do not forget to delete    *
# *                                                                              *
# *      In both cases, the output stream is stored in flat files in             *
# *      $ESKAPADE/results/esk610_spark_streaming/data/v0/dstream/               *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import logging

from pyspark.streaming import StreamingContext

from eskapade import process_manager as proc_mgr, ConfigObject, DataStore, spark_analysis
from eskapade.spark_analysis import SparkManager

log = logging.getLogger('macro.esk610_spark_streaming')
log.debug('Now parsing configuration file esk610_spark_streaming')

##########################################################################
# --- minimal analysis information

settings = proc_mgr.service(ConfigObject)
settings['analysisName'] = 'esk610_spark_streaming'
settings['version'] = 0


# check command line


def checkVar(varName, local_vars=vars(), settings=settings, default=False):
    varValue = default
    if varName in local_vars:
        varValue = settings[varName] = local_vars[varName]
    elif varName in settings:
        varValue = settings[varName]
    return varValue


stream_type = checkVar('stream_type')

##########################################################################
# --- start Spark session

# create Spark Context
sm = proc_mgr.service(SparkManager)
sc = sm.create_session(eskapade_settings=settings).sparkContext

# create Spark Streaming context
ssc = StreamingContext(sc, 1.0)
sm.spark_streaming_context = ssc

# define data stream
ds = proc_mgr.service(DataStore)

if not stream_type or stream_type == 'file':
    ds['dstream'] = ssc.textFileStream('/tmp/')
elif stream_type == 'tcp':
    ds['dstream'] = ssc.socketTextStream('localhost', 9999)
else:
    log.error('unsupported stream_type specified: {}'.format(stream_type))

##########################################################################
# --- now set up the chains and links based on configuration flags

proc_mgr.add_chain('SparkStreaming')

# the word count example
wordcount_link = spark_analysis.SparkStreamingWordCount(
    name='SparkStreamingWordCount', read_key='dstream', store_key='wordcounts')
proc_mgr.get_chain('SparkStreaming').add_link(wordcount_link)

# store output
writer_link = spark_analysis.SparkStreamingWriter(
    name='SparkStreamingWriter', read_key=wordcount_link.store_key, path=persistence.io_dir(
        'results_data', settings.io_conf()) + '/dstream/wordcount', suffix='txt', repartition=1)
proc_mgr.get_chain('SparkStreaming').add_link(writer_link)

# start/stop of Spark Streaming
control_link = spark_analysis.SparkStreamingController(name='SparkStreamingController', timeout=10)
proc_mgr.get_chain('SparkStreaming').add_link(control_link)

##########################################################################

log.debug('Done parsing configuration file esk610_spark_streaming')
