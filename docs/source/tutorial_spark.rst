Tutorial 5: going Spark
-----------------------

This section provides a tutorial on how to use Apache Spark in Eskapade. Spark works 'out of the box' in the Eskapade docker/vagrant image. For details on how to setup a custom Spark setup, see the `Spark <spark.html>`_ section in the Appendix.

In this tutorial we will basically redo Tutorial 1 but use Spark instead of Pandas for data processing. The following paragraphs describe step-by-step how to run a Spark job, use existing links and write your own links for Spark queries.

.. note::

  To get familiar with Spark in Eskapade you can follow the exercises in ``tutorials/tutorial_5.py``.


Running the tutorial macro
~~~~~~~~~~~~~~~~~~~~~~~~~~

The very first step to run the tutorial Spark job is:

.. code-block:: bash

  $ eskapade_run python/eskapade/tutorials/tutorial_5.py

Eskapade will start a Spark session, do nothing, and quit - there are no chains/links defined yet. The Spark session is created via the ``SparkManager`` which, like the ``DataStore``, is a singleton that configures and controls Spark sessions centrally. It is activated through the magic line:

.. code-block:: python

  process_manager.service(SparkManager).spark_session

Note that when the Spark session is created, the following line appears in logs:

.. code-block:: bash

  Adding Python modules to ZIP archive /Users/gossie/git/gitlab-nl/decision-engine/eskapade/es_python_modules.zip

This is the ``SparkManager`` that ensures all Eskapade source code is uploaded and available to the Spark cluster when running in a distributed environment.

If there was an ``ImportError: No module named pyspark`` then, most likely, ``SPARK_HOME`` and ``PYTHONPATH`` are not set up correctly. For details, see the `Spark <spark.html>`_ section in the Appendix.

Reading data
~~~~~~~~~~~~

Spark can read data from various sources, e.g. local disk, HDFS, HIVE tables. Eskapade provides the ``SparkDfReader`` link that uses the ``pyspark.sql.DataFrameReader`` to read flat CSV files into Spark DataFrames, RDD's, and Pandas DataFrames. To read in the Tutorial data, the following link should be added to the ``Data`` chain:

.. code-block:: python

  reader = spark_analysis.SparkDfReader(name='Read_LA_ozone', store_key='data', read_methods=['csv'])
  reader.read_meth_args['csv'] = (DATA_FILE_PATH,)
  reader.read_meth_kwargs['csv'] = dict(sep=',', header=True, inferSchema=True)
  process_manager.get_chain('Data').add_link(reader)

The ``DataStore`` holds a pointer to the Spark dataframe in (distributed) memory. This is different from a Pandas dataframe, where the entire dataframe is stored in the ``DataStore``, because a Spark dataframe residing on the cluster may not fit entirely in the memory of the machine running Eskapade. This means that Spark dataframes are never written to disk in ``DataStore`` pickles!

Using existing links
~~~~~~~~~~~~~~~~~~~~

Spark has a large set of standard functions for Spark DataFrames and RDD's. Although the purpose of Eskapade is not to duplicate this functionality, there are some links created for generic functions to facilitate specifying Spark queries directly in the macro, instead of hard-coding them in links. This is handy for bookkeeping queries at a central place and reducing code duplication, especially for smaller analysis steps. For example, the ``SparkExecuteQuery`` link takes any string containig SQL statements to perform a custom query with Spark on a dataframe.

Column transformations
**********************

To add two columns to the Tutorial data using the conversion functions defined earlier in the macro, two ``SparkWithColumn`` links need to be added to the ``Data`` chain, one for each additional column:

.. code-block:: python

  transform = spark_analysis.SparkWithColumn(name='Transform_doy', read_key=reader.store_key, store_key='transformed_data', col_select=['doy'], func=udf(comp_date, TimestampType()), new_column='date')
  process_manager.get_chain('Data').add_link(transform)
  transform = spark_analysis.SparkWithColumn(name='Transform_vis', read_key=transform.store_key, store_key='transformed_data', col_select=['vis'], func=udf(mi_to_km, FloatType()), new_column='vis_km')
  process_manager.get_chain('Data').add_link(transform)

Note that the functions defined in the macro are converted to user-defined functions with ``pyspark.sql.function.udf`` and their output types are explicitly specified in terms of ``pyspark.sql.types``. Omitting these type definitions can lead to obscure errors when executing the job.

Histogramming
*************

As was demonstrated in Tutorial 1, the ``DfSummary`` link creates LaTeX/PDF reports with histograms. Those histograms are obtained directly from a Pandas dataframe or from a dictionary of `Histogrammar <http://histogrammar.org>`_ histograms. This link can be re-used for Tutorial 4. However, an additional step is needed: create histograms of Spark dataframe columns with Histogrammar. This step can be carried out with the ``SparkHistogrammarFiller`` link. The code snippet for generating a report of Spark dataframe histograms then looks like:

.. code-block:: python

  histo = spark_analysis.SparkHistogrammarFiller(name='Histogrammer', read_key=transform.store_key, store_key='hist')
  histo.columns = ['vis', 'vis_km', 'doy', 'date']
  process_manager.get_chain('Summary').add_link(histo)
 
  summarizer = visualization.DfSummary(name='Create_stats_overview', read_key=histo.store_key, var_labels=VAR_LABELS, var_units=VAR_UNITS)
  process_manager.get_chain('Summary').add_link(summarizer)


Creating custom links
~~~~~~~~~~~~~~~~~~~~~

More complex queries deserve their own links since links provide full flexibility w.r.t. specifying custom data operation. For this Tutorial the 'complex query' is to just print 42 rows of the Spark dataframe. Of course, more advanced Spark functions can be applied in a similar fashion. A link is created just like was done before, e.g.:

.. code-block:: bash

  $ eskapade_generate_link --dir python/eskapade/spark_analysis SparkDfPrinter

This creates the link ``python/eskapade/spark_analysis/sparkdfprinter.py``. Do not forget to include the ``import`` statements in the ``__init__.py`` file as indicated by the ``eskapade_generate_link`` command.

The next step is to add the desired functionality to the link. In this case, the Spark dataframe needs to be retrieved from the ``DataStore`` and a ``show()`` method of that dataframe needs to be executed. The ``execute()`` method of the link is the right location for this:

.. code-block:: python

      def execute(self):
        """Execute SparkDfPrint"""

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {name}.', name=self.name)
        df = ds[self.read_key]
        df.show(self.nrows)

        return StatusCode.Success

In order to configure Eskapade to run this link, the link needs to be added to a chain, e.g. ``Summary``, in the ``tutorial/tutorial_5.py`` macro. This should look similar to:

.. code-block:: python

  from eskapade.spark_analysis import SparkDfPrint
  ...

  printer = SparkDfPrint(name='Print_spark_df', read_key=transform.store_key, nrows=42)
  process_manager.get_chain('Summary').add_link(printer)

The name of the dataframe is the output name of the ``transform`` link and the number of rows to print is specified by the ``nrows`` parameter.

Eskapade should now be ready to finally execute the macro and provide the desired output:

.. code-block:: bash

  $ eskapade_run python/eskapade/tutorials/tutorial_5.py

  * * * Welcome to Eskapade * * *
  ...

  +-----+----+----+--------+----+----+---+---+---+---+--------------------+--------+
  |ozone|  vh|wind|humidity|temp| ibh|dpg|ibt|vis|doy|                date|  vis_km|
  +-----+----+----+--------+----+----+---+---+---+---+--------------------+--------+
  |    3|5710|   4|      28|  40|2693|-25| 87|250|  3|1976-01-03 00:00:...| 402.335|
  |    5|5700|   3|      37|  45| 590|-24|128|100|  4|1976-01-04 00:00:...| 160.934|
  |    5|5760|   3|      51|  54|1450| 25|139| 60|  5|1976-01-05 00:00:...| 96.5604|
  ...

  |    6|5700|   4|      86|  55|2398| 21|121|200| 44|1976-02-13 00:00:...| 321.868|
  |    4|5650|   5|      61|  41|5000| 51| 24|100| 45|1976-02-14 00:00:...| 160.934|
  |    3|5610|   5|      62|  41|4281| 42| 52|250| 46|1976-02-15 00:00:...| 402.335|
  +-----+----+----+--------+----+----+---+---+---+---+--------------------+--------+
  only showing top 42 rows
  ...

  * * * Leaving Eskapade. Bye! * * *

That's it!


Spark examples
~~~~~~~~~~~~~~

Example Eskapade macros using Spark can be found in the ``tutorials`` directory, see ``esk601_spark_configuration.py`` and further.


Spark Streaming
---------------

Eskapade supports the use of Spark Streaming as demonstrated in the word count example ``tutorials/esk610_spark_streaming_wordcount.py``. The data is processed in (near) real-time as micro batches of RDD's, so-called discretized streaming, where the stream originates from either new incoming files or network connection. As with regulard Spark queries, various transformations can be defined and applied in subsequent Eskapade links.

For details on Spark Streaming, see also https://spark.apache.org/docs/latest/streaming-programming-guide.html.

File stream
~~~~~~~~~~~

The word count example using the file stream method can be run by executing in two different terminals:

.. code-block:: bash 

  terminal 1 $ for ((i=0; i<=100; i++)); do echo "Hello world" > /tmp/dummy_$(printf %05d ${i}); sleep 0.1; done
  terminal 2 $ eskapade_run -c stream_type='tcp' $ESKAPADE/python/eskapade/tutorials/esk610_spark_streaming.py

Where bash ``for``-loop will create a new file containing ``Hello world`` in the /tmp directory every 0.1 second. Spark Streaming will pick up and process these files and in ``terminal 2`` a word count of the processed data will by dispayed. Output is stored in ``$ESKAPADE/results/esk610_spark_streaming/data/v0/dstream/wordcount``.


TCP stream
~~~~~~~~~~

The word count example using the TCP stream method can be run by executing in two different terminals:

.. code-block:: bash 

  terminal 1 $ nc -lk 9999
  terminal 2 $ eskapade_run -c stream_type='tcp' $ESKAPADE/python/eskapade/tutorials/esk610_spark_streaming.py

Where ``nc`` (netcat) will stream data to port 9999 and Spark Streaming will listen to this port and process incoming data. In ``terminal 1`` random words can be type (followed by enter) and in ``terminal 2`` a word count of the processed data will by dispayed. Output is stored in ``$ESKAPADE/results/esk610_spark_streaming/data/v0/dstream/wordcount``.


