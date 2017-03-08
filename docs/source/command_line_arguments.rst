======================
Command Line Arguments
======================

Overview
--------

We start this section with a short overview of a few often used arguments that Eskapade has.

At the end of running the Eskapade program, by default the DataStore and configuration object are pickled and written
out to:

.. code-block:: bash

  $ ls -l results/Tutorial_1/proc_service_data/v0/latest/
  
When you are working on a macro, once you are done tweaking it, you can store also the results of each chain in pickle
files:

.. code-block:: bash

  $ run_eskapade.py -w tutorials/tutorial_1.py

Eskapade uses these pickle files to load the trained models and uses them to predict new samples real-time,
but also to pick up running at a later stage in the chain setup.

For example, if running Eskapade takes a long time, you can run one chain as well:

.. code-block:: bash

  $ run_eskapade.py -s Name_Chain tutorials/tutorial_1.py

This command uses as input the stored pickle files from the previous chain.
This might come in handy when, for example, data pre-processing of your training set takes a long time.
In that case, you can run the pre-processing chain over night, store the results in a pickle file and start with
the training chain the next day.

Start running Eskapade from a specified chain:

.. code-block:: bash

  $ run_eskapade.py -b Name_Chain tutorials/tutorial_1.py

Stop running after a specified chain:

.. code-block:: bash

  $ run_eskapade.py -e Name_Chain tutorials/tutorial_1.py

Below we explain the most important command-line arguments in detail after the summary table of all commands.

Table of all arguments
----------------------

The following table summarizes the available command line arguments:

+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| Command                               | Short command         | Argument              | Description                                                      |
+=======================================+=======================+=======================+==================================================================+
| --log-level                           | -L                    | LOG_LEVEL             | Set the loglevel of your run.                                    |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --help                                | -h                    |                       | Help options.                                                    |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --log-format                          | -F                    | LOG_FORMAT            | Set the format of log messages.                                  |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --seed                                | -S                    | SEED                  | Set the random seed for toy generation.                          |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --batch-mode                          | -B                    |                       | Run in batch mode, not using X Windows.                          |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --interactive                         | -i                    |                       | Remain in interactive mode after running.                        |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --cmd                                 | -c                    | "CMD;"                | Python commands to process (semi-colon-seperated and in quotes). |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --userArg                             | -U                    | USER_ARG              | Arbitrary user argument(s).                                      |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --store-intermediate-result           | -w                    |                       | Store intermediate result after each chain, not only at end.     |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --store-intermediate-result-one-chain | -W                    | CHAIN_NAME            | Store intermediate result of chain_name.                         |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --single-chain                        | -s                    | CHAIN_NAME            | Run only single chain: chain_name.                               |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --begin-with-chain                    | -b                    | CHAIN_NAME            | Start run at: chain_name.                                        |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --end-with-chain                      | -e                    | CHAIN_NAME            | End run at: chain_name.                                          |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --data-version                        | -v                    | INTEGER               | Set version number.                                              |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --analysis-name                       | -a                    | ANALYSIS_NAME         | Set the analysis name.                                           |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --results-dir                         | -r                    | RESULTS_DIR           | Set path of the storage results directory.                       |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --data-dir                            | -d                    | DATA_DIR              | Set path of the data directory.                                  |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --macros-dir                          | -m                    | MACRO_DIR             | Set path of the macros directory.                                |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --do-not-store-results                | -n                    |                       | Do not store results in pickle files.                            |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --unpickle-config                     | -u                    | CONFIG_OBJECT_PICKLE  | Unpickle a configuration from a saved config file.               |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+
| --run-profiling                       | -P                    |                       | Profile your code.                                               |
+---------------------------------------+-----------------------+-----------------------+------------------------------------------------------------------+

These arguments can be combined when running Eskapade.

Description and examples
------------------------

This section contains the most used arguments with a longer description of what it does and how it works combined with
examples.

Set log level
~~~~~~~~~~~~~

The log level is controlled with the `-L` argument. To set the log level to debug level, add::

  -L DEBUG

to the command line:

.. code-block:: bash

  $ run_eskapade.py -L DEBUG tutorials/tutorial_1.py

The available log levels are::

  DEBUG,
  INFO,
  WARN,
  WARNING,
  ERROR,
  FATAL,
  CRITICAL,
  OFF

They correspond to the appropriate POSIX levels.

When writing your own Link, these levels can be accessed with the logger module:

.. code-block:: python

  self.log().debug('Text to be printed when logging at DEBUG level')

All output is done in this manner, never with the python print function, since this yields us more control over the
process.

Help
~~~~

Help can be called by running the following:

.. code-block:: bash

  $ run_eskapade.py --help

Interactive python mode
~~~~~~~~~~~~~~~~~~~~~~~

To keep the results in memory at end of session run Eskapade in interactive mode. This is controlled with
the argument ``-i``:

.. code-block:: bash

  $ run_eskapade.py -i tutorials/tutorial_1.py

At the end of the session an ``ipython`` console is started from which e.g. the data store can be accessed.
  
Saving states
~~~~~~~~~~~~~

To write out the intermediate results from each chain add the command line argument ``-w``. This will write pickles in
``results/$analysis_name/data/$version/`` containing the state of Eskapade at the end of the chain:

.. code-block:: bash

  $ run_eskapade.py -w tutorials/tutorial_1.py

To write out the state after one particular chain, use option ``-W`` (capital):
  
.. code-block:: bash

  $ run_eskapade.py -W SomeParticularChain tutorials/tutorial_1.py

To not store any pickle files, run with the option ``-n``:

.. code-block:: bash

  $ run_eskapade.py -n tutorials/tutorial_1.py
  
Single Chain
~~~~~~~~~~~~

To run a single chain add the command line argument ``-s chain_name``. By default this picks up the data stored by
the previous chain in the macro. It is therefore necessary to have run the previous chain, otherwise the engine can
not start:

.. code-block:: bash

  $ run_eskapade.py -s chain_name tutorials/tutorial_1.py

Start from a Chain
~~~~~~~~~~~~~~~~~~

To start from a chain use the command line argument ``-b chain_name``, by default this picks up the data stored by
the previous chain in the macro:

.. code-block:: bash

  $ run_eskapade.py -b chain_name tutorials/tutorial_1.py

Stop at a Chain
~~~~~~~~~~~~~~~

To end the running of the engine at a chain use the command line argument ``-e chain_name``, by default this picks
up the data stored by the previous chain in the macro:

.. code-block:: bash

  $ run_eskapade.py -e chain_name tutorials/tutorial_1.py


Changing analysis version
~~~~~~~~~~~~~~~~~~~~~~~~~

A version number is assigned to each analysis, which by default is 0. It can be upgraded by using the option ``-v``.  
When working on an analysis, it is recommended to update this number regularly for bookkeeping purposes.

.. code-block:: bash

  $ run_eskapade.py -v 1 tutorials/tutorial_1.py

Notice that the output of this analysis is now stored in the directory:

.. code-block:: bash

  $ ls -l results/Tutorial_1/data/v1/report/

Notice as well that, for bookkeeping purposes, a copy of the (evolving) configuration macro is always stored as well,
under:

.. code-block:: bash

  $ ls -l results/Tutorial_1/config/v*/tutorial_1.py


Running an old configuration (macro)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you run an Eskapade macro, at the end of the program the settings you used for the run are saved in a config file.
In the python session you ran in, this config is kept in the object called 'configObject'.
After the sessions ends, this configObject is pickled for later use.

This allows for rerunning older versions of the code, since they are saved in the state they were in at run-time.
If you want to run an old version of the configObject named ``$analysis``, this can easily by done with the
argument ``-u``:

.. code-block:: bash

  $ run_eskapade.py -u results/Tutorial_1/proc_service_data/v0/latest/eskapade.core.process_services.ConfigObject.pkl

In this way, rolling back to a previous point is straight-forward.

For lookup purposes a copy of the configuration macro is always stored as well, under:

.. code-block:: bash

  $ ls -l results/Tutorial_1/config/v0/tutorial_1.py


Profiling your code
~~~~~~~~~~~~~~~~~~~

Your can profile the speed of your analysis functions by running the option ``-P``:

.. code-block:: bash

  $ run_eskapade.py -P tutorials/tutorial_1.py

After running this prints out a long list of all functions called, including the time it took to run each of of them.


Combining arguments
~~~~~~~~~~~~~~~~~~~

Of course you can add multiple arguments to the command line, the result would be for example an interactive session in
debug mode that writes out intermediate results from each chain:

.. code-block:: bash

  $ run_eskapade.py -i -w -L DEBUG tutorials/tutorial_1.py