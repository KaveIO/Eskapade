======================
Command Line Arguments
======================

Overview
--------

We start this section with a short overview of a few often used
arguments of the Eskapade command ``eskapade_run``.  The
only required argument is a configuration file, which can be a Python
script (Eskapade macro) or a pickled Eskapade configuration object.
This section gives an overview of the optional arguments of the run command.

At the end of running the Eskapade program, by default the DataStore and configuration object are pickled and written
out to:

.. code-block:: bash

  $ ls -l results/Tutorial_1/proc_service_data/v0/latest/
  
When you are working on a macro, once you are done tweaking it, you can also store the results of each chain in pickle
files:

.. code-block:: bash

  $ eskapade_run --store-all python/eskapade/tutorials/tutorial_1.py

Eskapade uses these pickle files to load the trained models and uses them to predict new samples real-time,
but also to pick up running at a later stage in the chain setup.

For example, if running Eskapade takes a long time, you can run one chain as well:

.. code-block:: bash

  $ eskapade_run --single-chain=Data python/eskapade/tutorials/tutorial_1.py

This command uses as input the stored pickle files from the previous chain.
This might come in handy when, for example, data pre-processing of your training set takes a long time.
In that case, you can run the pre-processing chain over night, store the results in a pickle file and start with
the training chain the next day.

Start running Eskapade from a specified chain:

.. code-block:: bash

  $ eskapade_run --begin-with=Summary python/eskapade/tutorials/tutorial_1.py

Stop running after a specified chain:

.. code-block:: bash

  $ eskapade_run --end-with=Data python/eskapade/tutorials/tutorial_1.py

Below the most important command-line options are explained in detail.

Table of all arguments
----------------------

The following table summarizes the available command-line options.  Most
of these options set variables in the Eskapade configuration object and
can be overwritten by settings in the configuration macro.

+--------------------+--------------+-------------------+---------------------------------------------------------+
| Option             | Short option | Argument          | Description                                             |
+====================+==============+===================+=========================================================+
| --help             | -h           |                   | show help message and exit                              |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --analysis-name    | -n           | NAME              | set name of analysis in run                             |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --analysis-version | -v           | VERSION           | set version of analysis version in run                  |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --batch-mode       |              |                   | run in batch mode (no X Windows)                        |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --interactive      | -i           |                   | start IPython shell after run                           |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --log-level        | -L           | LEVEL             | set logging level                                       |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --log-format       |              | FORMAT            | set log-message format                                  |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --unpickle-config  |              |                   | interpret first CONFIG_FILE as path to pickled settings |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --profile          |              |                   | run profiler for Python code                            |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --conf-var         | -c           | KEY=VALUE         | set configuration variable                              |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --begin-with       | -b           | CHAIN_NAME        | begin execution with chain CHAIN_NAME                   |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --end-with         | -e           | CHAIN_NAME        | end execution with chain CHAIN_NAME                     |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --single-chain     | -s           | CHAIN_NAME        | only execute chain CHAIN_NAME                           |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --store-all        |              |                   | store run-process services after every chain            |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --store-one        |              | CHAIN_NAME        | store run-process services after chain CHAIN_NAME       |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --store-none       |              |                   | do not store run-process services                       |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --results-dir      |              | RESULTS_DIR       | set directory path for results output                   |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --data-dir         |              | DATA_DIR          | set directory path for data                             |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --macros-dir       |              | MACROS_DIR        | set directory path for macros                           |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --templates-dir    |              | TEMPLATES_DIR     | set directory path for template files                   |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --spark-cfg-file   |              | SPARK_CONFIG_FILE | set path of Spark configuration file                    |
+--------------------+--------------+-------------------+---------------------------------------------------------+
| --seed             |              | SEED              | set seed for random-number generation                   |
+--------------------+--------------+-------------------+---------------------------------------------------------+

Description and examples
------------------------

This section contains the most used options with a longer description of
what it does and how it works combined with examples.

Set log level
~~~~~~~~~~~~~

The log level is controlled with the ``--log-level`` option.  For example,
to set the log level to "debug", add::

  --log-level=DEBUG

to the command line:

.. code-block:: bash

  $ eskapade_run -L DEBUG python/eskapade/tutorials/tutorial_1.py

The available log levels are::

  NOTSET,
  DEBUG,
  INFO,
  WARNING,
  ERROR,
  FATAL

They correspond to the appropriate POSIX levels.

When writing your own Link, these levels can be accessed with the logger module:

.. code-block:: python

  self.logger.debug('Text to be printed when logging at DEBUG level')

All output is done in this manner, never with the python print function, since this yields us more control over the
process.

Help
~~~~

Help can be called by running the following:

.. code-block:: bash

  $ eskapade_run --help

Interactive python mode
~~~~~~~~~~~~~~~~~~~~~~~

To keep the results in memory at end of session and access them in an
interactive session, run Eskapade in interactive mode.  This is
controlled with ``--interactive``:

.. code-block:: bash

  $ eskapade_run -i python/eskapade/tutorials/tutorial_1.py

At the end of the session an ``IPython`` console is started from which
e.g. the data store can be accessed.
  
Saving states
~~~~~~~~~~~~~

To write out the intermediate results from every chain, add the command
line argument ``--store-all``.  This will write pickles in
``results/NAME/proc_service_data/VERSION/``, containing the state of Eskapade at the
end of the chain:

.. code-block:: bash

  $ eskapade_run --store-all python/eskapade/tutorials/tutorial_1.py

To write out the state after a particular chain, use option
``--store-one``:
  
.. code-block:: bash

  $ eskapade_run --store-one=Data python/eskapade/tutorials/tutorial_1.py

To not store any pickle files, run with the option ``--store-none``:

.. code-block:: bash

  $ eskapade_run --store-none python/eskapade/tutorials/tutorial_1.py
  
Single Chain
~~~~~~~~~~~~

To run a single chain, use the option ``--single-chain``.  This picks up
the data stored by the previous chain in the macro.  It is, therefore,
necessary to have run the previous chain, otherwise the engine can not
start:

.. code-block:: bash

  $ eskapade_run -s Summary python/eskapade/tutorials/tutorial_1.py

Start from a Chain
~~~~~~~~~~~~~~~~~~

To start from a chain use the command line argument ``--begin-with``.
This picks up the data stored by the previous chain in the macro.

.. code-block:: bash

  $ eskapade_run -b Summary python/eskapade/tutorials/tutorial_1.py

Stop at a Chain
~~~~~~~~~~~~~~~

To end the running of the engine at a chain use, the command line
argument ``--end-with``:

.. code-block:: bash

  $ eskapade_run -e Data python/eskapade/tutorials/tutorial_1.py


Changing analysis version
~~~~~~~~~~~~~~~~~~~~~~~~~

A version number is assigned to each analysis, which by default is 0. It
can be upgraded by using the option ``--analysis-version``.   When
working on an analysis, it is recommended to update this number
regularly for bookkeeping purposes. The command line always has higher priority over the macro. If the macro is version
0 and the command line uses version 1, the command line will overrule the macro.

.. code-block:: bash

  $ eskapade_run -v 1 python/eskapade/tutorials/tutorial_1.py

Notice that the output of this analysis is now stored in the directory:

.. code-block:: bash

  $ ls -l results/Tutorial_1/data/v1/report/

Notice as well that, for bookkeeping purposes, a copy of the (evolving) configuration macro is always stored as well,
under:

.. code-block:: bash

  $ ls -l results/Tutorial_1/config/v1/tutorial_1.py


Running an old configuration (macro)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Settings for the Eskapade run are stored in a configuration object,
which is accessed as a run-process service.  This run-time service can
be persisted as a file, which is normally done at the end of the run.

Persisted settings can be used in a following run by providing the file
path of the ``ConfigObject`` pickle file as the configuration file
argument.  The option ``--unpickle-config`` is required to indicate that
this file contains persisted settings:

.. code-block:: bash

  $ eskapade_run --unpickle-config results/Tutorial_1/proc_service_data/v0/latest/eskapade.core.process_services.ConfigObject.pkl

In this way, rolling back to a previous point is straight-forward.

For lookup purposes a copy of the configuration macro is always stored as well, under:

.. code-block:: bash

  $ ls -l results/Tutorial_1/config/v0/tutorial_1.py


Profiling your code
~~~~~~~~~~~~~~~~~~~

Your can profile the execution of your analysis functions with the
option ``--profile``:

.. code-block:: bash

  $ eskapade_run --profile=cumulative python/eskapade/tutorials/tutorial_1.py

After running this prints out a long list of all functions called,
including the time it took to run each of of them, where the functions
are sorted based on cumulative time.

To get the the list of sorting options for the profiling, run:

.. code-block:: bash

  $ eskapade_run --help


Combining arguments
~~~~~~~~~~~~~~~~~~~

Of course you can add multiple arguments to the command line, the result would be for example an interactive session in
debug mode that writes out intermediate results from each chain:

.. code-block:: bash

  $ eskapade_run -i --store-all -L DEBUG -c do_chain0=False -c mydict="{'f': 'y=pi', 'pi': 3.14}" python/eskapade/tutorials/esk106_cmdline_options.py
