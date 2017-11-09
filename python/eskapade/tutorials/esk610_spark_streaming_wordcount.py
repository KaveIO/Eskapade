r"""Project: Eskapade - A python-based package for data analysis.

Macro: esk610_spark_streaming

Created: 2017/05/31

Description:
    Tutorial macro running Spark Streaming word count example in Eskapade,
    derived from:

    https://spark.apache.org/docs/latest/streaming-programming-guide.html

    Counts words in UTF8 encoded, '\n' delimited text received from a
    stream every second. The stream can be from either files or network.

    For example:

    i)  to run locally using tcp stream, use netcat and type random words:
         `$ nc -lk 9999`
        and then run the example (in a second terminal):
         `$ eskapade_run -c stream_type='tcp' \
                python/eskapade/tutorials/esk610_spark_streaming_wordcount.py`

        NB: hostname and port can be adapted in this macro

    ii) to run locally using file stream, create dummy files in /tmp:
         `$ for i in $(seq -f \"%05g\" 0 100); \
               do echo "Hello world" > /tmp/eskapade_stream_test/dummy_$i;
                   sleep 0.1; \
           done`
        and then run the example (in a second terminal):
         `$ eskapade_run -c stream_type='file' \
                python/eskapade/tutorials/esk610_spark_streaming_wordcount.py`

        NB: only new files in /tmp/eskapade_stream_test are processed,
            do not forget to delete this directory

    In both cases, the output stream is stored in flat files in
    ./results/esk610_spark_streaming/data/v0/dstream/

    Do not forget to clean the results directory when testing.

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from pyspark.streaming import StreamingContext

from eskapade import process_manager, ConfigObject, DataStore, spark_analysis, Chain
from eskapade.core import persistence
from eskapade.logger import Logger
from eskapade.spark_analysis import SparkManager

logger = Logger()
logger.debug('Now parsing configuration file esk610_spark_streaming.')

##########################################################################
# --- minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk610_spark_streaming'
settings['version'] = 0


# check command line
def check_var(var_name, local_vars=vars(), settings=settings, default=False):
    """Set setting and return it."""
    var_value = default
    if var_name in local_vars:
        var_value = settings[var_name] = local_vars[var_name]
    elif var_name in settings:
        var_value = settings[var_name]
    return var_value


stream_type = check_var('stream_type')

##########################################################################
# --- start Spark session

# create Spark Context
sm = process_manager.service(SparkManager)
sc = sm.create_session(eskapade_settings=settings).sparkContext

# create Spark Streaming context
ssc = StreamingContext(sc, 1.0)
sm.spark_streaming_context = ssc

# define data stream
ds = process_manager.service(DataStore)

if not stream_type or stream_type == 'file':
    ds['dstream'] = ssc.textFileStream('/tmp/eskapade_stream_test/')
elif stream_type == 'tcp':
    ds['dstream'] = ssc.socketTextStream('localhost', 9999)
else:
    logger.error('unsupported stream_type specified: {type}.', type=stream_type)

##########################################################################
# --- now set up the chains and links based on configuration flags

spark_streaming = Chain('SparkStreaming')

# the word count example
wordcount_link = spark_analysis.SparkStreamingWordCount(
    name='SparkStreamingWordCount', read_key='dstream', store_key='wordcounts')
spark_streaming.add(wordcount_link)

# store output
writer_link = spark_analysis.SparkStreamingWriter(
    name='SparkStreamingWriter',
    read_key=wordcount_link.store_key,
    output_path='file:' + persistence.io_path('results_data', '/dstream/wordcount'),
    suffix='txt',
    repartition=1)

spark_streaming.add(writer_link)

# start/stop of Spark Streaming
control_link = spark_analysis.SparkStreamingController(name='SparkStreamingController', timeout=10)
spark_streaming.add(control_link)

##########################################################################

logger.debug('Done parsing configuration file esk610_spark_streaming.')
