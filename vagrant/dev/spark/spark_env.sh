## Begin Spark
#

export SPARK_HOME="SPARK_HOME_VAR"
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYTHONPATH="${SPARK_HOME}/python:$(ls ${SPARK_HOME}/python/lib/py4j-*-src.zip):${PYTHONPATH}"
export SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"
export PYSPARK_SUBMIT_ARGS="--master local[2] --executor-cores 2 --executor-memory 4g pyspark-shell"
#
## End Spark
