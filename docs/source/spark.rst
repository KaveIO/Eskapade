============
Apache Spark
============

Eskapade supports the use of `Apache Spark <https://spark.apache.org>`_ for parallel processing of large data volumes. Jobs can run on a single laptop using Spark libraries as well as on a Spark/Hadoop cluster in combination with YARN. This section describes how to setup and configure Spark for use with Eskapade. For examples on running Spark jobs with Eskapade, see the `Spark tutorial <tutorial_spark.html>`_.

.. note ::

  Eskapade supports both batch and real-time streaming (micro batch) processing with Apache Spark.

Requirements
------------

A default working setup of the Apache Spark libraries is included in both the Eskapade docker and vagrant image (see section `Installation <installation.html>`_). For installation of Spark libraries in a custom setup, please refer to the Spark `documentation <https://spark.apache.org/docs/latest/>`_.

NB: not all combinations of Spark and Python versions work properly together.


Spark installation
~~~~~~~~~~~~~~~~~~

The environment variables ``SPARK_HOME`` and ``PYTHONPATH`` need be set and to point to the location of the Spark installation and the Python libraries of Spark and py4j (dependency). In the Eskapade docker, for example, it is set to:

.. code-block:: bash

  $ echo $SPARK_HOME
  /opt/spark/pro/
  $ echo $PYTHONPATH
  /opt/spark/pro/python:/opt/spark/pro/python/lib/py4j-0.10.4-src.zip:...


Configuration
-------------

The Spark configuration can be set using three different methods:

1. environment variables
2. an Eskapade macro (preferred)
3. an Eskapade link 

This is demonstrated in the following tutorial macro:

.. code-block:: bash

  $ eskapade_run python/eskapade/tutorials/esk601_spark_configuration.py

The methods are described in the sections below. For a description of configuration settings, see `Spark Configuration <http://spark.apache.org/docs/latest/configuration.html>`_. In case configuration settings seem not to be picked up correctly, please check `Notes`_ at the end of this section.

Environment variables
~~~~~~~~~~~~~~~~~~~~~

Configuration for Spark jobs can be set through the ``PYSPARK_SUBMIT_ARGS`` environment variable, e.g.:

.. code-block:: bash

  PYSPARK_SUBMIT_ARGS=--master local[4] --num-executors 1 --executor-cores 4 --executor-memory 4g pyspark-shell

The Spark session is then started directly from a macro with specified settings.

.. code-block:: python

  sm = process_manager.service(SparkManager)
  sm.spark_session
  sm.spark_context.setLogLevel('INFO')


Eskapade macro (preferred)
~~~~~~~~~~~~~~~~~~~~~~~~~~

This method allows to specify settings per macro, i.e. per analysis, and is therefore the preferred way for bookkeeping analysis-specific settings. 

Configuration for Spark jobs can be set through the ``SparkConf`` class that holds a list of key/value pairs with configuration settings, e.g.:

.. code-block:: python

  conf = pyspark.conf.SparkConf()
  conf.setAppName(settings['analysisName'])
  conf.setAll([('spark.driver.host', '127.0.0.1'), ('spark.master', 'local[2]')])

The Spark session is then started directly from a macro with specified settings.

.. code-block:: python

  sm = process_manager.service(SparkManager)
  sm.spark_conf = conf
  sm.spark_session
  sm.spark_context.setLogLevel('INFO')


Eskapade link
~~~~~~~~~~~~~

This method allows to (re-)start Spark sessions from within a ``SparkConfigurator`` link. This means that by specifying multiple instances of this link in a macro, multiple Spark sessions with different settings can sequentially be run. This can be useful for larger analysis jobs that contain multiple Spark queries with very different CPU/memory needs - although the recently introduced `Dynamic allocation`_ feature is a more elegant way to achieve this behaviour.

Configurations for Spark jobs are set via the ``SparkConf`` class that holds a list of key/value pairs with settings, e.g.:

.. code-block:: python

  conf_link = spark_analysis.SparkConfigurator(name='SparkConfigurator')
  conf_link.sparkConf = [('spark.master', 'local[3]')]
  conf_link.setLogLevel = 'INFO'
  config = Chain('Config)
  config.add(conf_link)

Note that the ``SparkConfigurator`` stops any existing Spark session before starting a new one. This means that the user should make sure all relevant data is stored at this point, since all cached Spark data will be cleared from memory.


Parameters
----------

The most important parameters to play with for optimal performance:

- ``num-executors``
- ``executor-cores``
- ``executor-memory``
- ``driver-memory``


Dynamic allocation
~~~~~~~~~~~~~~~~~~
Since version 2.1, Spark allows for `dynamic resouce allocation <https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation>`_. This requires the following settings:

- ``spark.dynamicAllocation.enabled=true``
- ``spark.shuffle.service.enabled=true``

Depending on the mode (standalone, YARN, Mesos), an additional shuffle service needs to be set up. See the documentation for details.


Logging
-------

The logging level of Spark can be controlled in two ways:

1. through ``$SPARK_HOME/conf/log4j.properties`` 

.. code-block:: bash

  log4j.logger.org.apache.spark.api.python.PythonGatewayServer=INFO

2. through the ``SparkContext`` in Python:

.. code-block:: python

  process_manager.service(SparkManager).spark_context.setLogLevel('INFO')


PS: the loggers in Python can be controlled through:

.. code-block:: python

   import logging
   print(logging.Logger.manager.loggerDict) # obtain list of all registered loggers
   logging.getLogger('py4j').setLevel('INFO')
   logging.getLogger('py4j.java_gateway').setLevel('INFO')  

However, not all Spark-related loggers are available here (as they are JAVA-based).


Notes
-----

There are a few pitfalls w.r.t. setting up Spark correctly: 

1. If the environment variable ``PYSPARK_SUBMIT_ARGS`` is defined, its settings may override those specified in the macro/link. This can be prevented by unsetting the variable:

.. code-block:: bash

  $ unset PYSPARK_SUBMIT_ARGS

or in the macro: 

.. code-block:: python

  import os
  del os.environ['PYSPARK_SUBMIT_ARGS']

The former will clear the variable from the shell session, whereas the latter will only clear it in the Python session.

2. In client mode not all driver options set via ``SparkConf`` are picked up at job submission because the JVM has already been started. Those settings should therefore be passed through the ``SPARK_OPTS`` environment variable, instead of using ``SparkConf`` in an Eskapade macro or link: 

.. code-block:: bash

  SPARK_OPTS=--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info --driver-memory 2g 

3. In case a Spark machine is not connected to a network, setting the ``SPARK_LOCAL_HOSTNAME`` environment variable or the ``spark.driver.host`` key in ``SparkConf`` to the value ``localhost`` may fix DNS resolution timeouts which prevent Spark from starting jobs.

