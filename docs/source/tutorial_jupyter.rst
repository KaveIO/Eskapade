Tutorial 3: Jupyter notebook
----------------------------

This section contains materials on how to use Eskapade in Jupyter Notebooks. There are additional side notes on how certain
aspects work and where to find parts of the code. For more in depth explanations, try the `API-docs <code.html>`_.

Next we will demonstrate how Eskapade can be run and debugged interactively from within a Jupyter notebook:

.. code-block:: bash

  $ jupyter notebook


An Eskapade notebook
~~~~~~~~~~~~~~~~~~~~

To run Eskapade use the ``eskapade_generate_notebook`` command to create a template notebook. For example:

.. code-block:: bash

  $ eskapade_generate_notebook --dir ./ test_notebook

The minimal code you need to run a notebook is the following:

.. code-block:: python

  from eskapade import process_manager, resources, ConfigObject, DataStore
  from eskapade.core import execution, persistence
  from eskapade.logger import LogLevel

  # --- basic config
  settings = process_manager.service(ConfigObject)
  settings['macro'] = resources.tutorial('tutorial_1.py')
  settings['analysisName'] = 'test_notebook'
  settings['version'] = 0
  settings['logLevel'] = LogLevel.DEBUG

  # --- optional running parameters
  #settings['beginWithChain'] = 'startChain'
  #settings['endWithChain'] = 'endChain'
  #settings['resultsDir'] = 'resultsdir'
  settings['storeResultsEachChain'] = True

  # --- other global flags (just some examples)
  # settings['set_mongo'] = False
  # settings['set_training'] = False

  # --- run eskapade!
  execution.run_eskapade(settings)

  # --- To rerun eskapade, clear the memory state first!
  # execution.reset_eskapade()


Make sure to fill out all the necessary parameters for it to run. The macro has to be set obviously, but not all
settings in this example are needed to be set to a value. The function ``execution.run_eskapade(settings)`` runs
Eskapade with the settings your specified.


To inspect the state of the Eskapade objects (datastore and configurations) after the various chains see the
command line examples below.
.. note::

  Inspecting intermediate states requires Eskapade to be run with the option storeResultsEachChain
  (command line: ``-w``) on.

.. code-block:: python

  from eskapade import process_manager, ConfigObject, DataStore

  # --- example inspecting the data store after the preprocessing chain
  ds = DataStore.import_from_file(os.environ['ESKAPADE']+'/results/Tutorial_1/proc_service_data/v0/_Summary/eskapade.core.process_services.DataStore.pkl')
  ds.keys()
  ds.Print()
  ds['data'].head()

  # --- example showing Eskapade settings
  co = ConfigObject.import_from_file(os.environ['ESKAPADE']+'/results/Tutorial_1/proc_service_data/v0/_Summary/eskapade.core.process_services.ConfigObject.pkl')
  co.Print()

The ``import_from_file`` function imports a pickle file that was written out by Eskapade, containing the DataStore.
This can be used to start from an intermediate state of your Eskapade. For example, you do some operations on your
DataStore and then save it. At a later time you load this saved DataStore and continue from there.


Running in a notebook
~~~~~~~~~~~~~~~~~~~~~

In this tutorial we will make a notebook and run the macro from `tutorial 1 <tutorial.html#advanced-macro-s>`_. This
macro shows the basics of Eskapade. Once we have Eskapade running in a terminal, we can run it also in Jupyter.
Make sure you have properly `installed Jupyter <installation#making-jupyter-run-with-the-right-python-kernel>`_.

We start by making a notebook:

.. code-block:: bash

  $ eskapade_generate_notebook --dir tutorials/ tutorial_3_notebook

This will create a notebook in ``tutorials/`` with the name ``tutorial_3_notebook`` running
macro ``tutorial_1.py``. Now open Jupyter and take a look at the notebook.

.. code-block:: bash

  $ jupyter notebook

Try to run the notebook. You might get an error if the notebook can not find the data for the data reader. Unless
you luckily are in the right folder. Use:

::

  !pwd

In Jupyter to find which path you are working on, and change the load path in the macro to the proper one.
This can be for example:

.. code-block:: python

  os.environ['ESKAPADE'] + '/data/LAozone.data'

but in the end it depends on your setup.

*Intermezzo: you can run bash commands in Jupyter by prepending the command with a !*

Now run the cells in the notebook and check if the macro runs properly. The output be something like::

  2017-02-14 14:04:55,506 DEBUG [link/execute_link]: Now executing link 'LA ozone data'
  2017-02-14 14:04:55,506 DEBUG [readtodf/execute]: reading datasets from files ["../data/LAozone.data"]
  2017-02-14 14:04:55,507 DEBUG [readtodf/pandasReader]: using Pandas reader "<function _make_parser_function.<locals>.parser_f at 0x7faaac7f4d08>"
  2017-02-14 14:04:55,509 DEBUG [link/execute_link]: Done executing link 'LA ozone data'
  2017-02-14 14:04:55,510 DEBUG [link/execute_link]: Now executing link 'Transform'
  2017-02-14 14:04:55,511 DEBUG [applyfunctodataframe/execute]: Applying function <function <lambda> at 0x7faa8ba2e158>
  2017-02-14 14:04:55,512 DEBUG [applyfunctodataframe/execute]: Applying function <function <lambda> at 0x7faa8ba95f28>
  2017-02-14 14:04:55,515 DEBUG [link/execute_link]: Done executing link 'Transform'
  2017-02-14 14:04:55,516 DEBUG [chain/execute]: Done executing chain 'Data'
  2017-02-14 14:04:55,516 DEBUG [chain/finalize]: Now finalizing chain 'Data'
  2017-02-14 14:04:55,517 DEBUG [link/finalize_link]: Now finalizing link 'LA ozone data'
  2017-02-14 14:04:55,518 DEBUG [link/finalize_link]: Done finalizing link 'LA ozone data'
  2017-02-14 14:04:55,518 DEBUG [link/finalize_link]: Now finalizing link 'Transform'
  2017-02-14 14:04:55,519 DEBUG [link/finalize_link]: Done finalizing link 'Transform'
  2017-02-14 14:04:55,519 DEBUG [chain/finalize]: Done finalizing chain 'Data'

with a lot more text surrounding this output. Now try to run the macro again.
The run should fail, and you get the following error::

  RuntimeError: tried to add chain with existing name to process manager

This is because the ProcessManager is a singleton. This means there is only one of this in memory allowed, and since
the Jupyter python kernel was still running the object still existed and running the macro gave an error. The macro
tried to make a singleton, but it already exists. Therefore the final line in the notebook template has to be ran every
time you want to rerun Eskapade. So run this line:

.. code-block:: python

  execution.reset_eskapade()

And try to rerun the notebook without restarting the kernel.

.. code-block:: python

  execution.run_eskapade(settings)

If one wants to call the objects used in the run, ``execute`` contains them. For example calling

.. code-block:: python

  ds = process_manager.service(DataStore)

is the DataStore, and similarly the other 'master' objects can be called.
Resetting will clear the process manager singleton from memory, and now the macro can be rerun without any errors.

Note: restarting the Jupyter kernel also works, but might take more time because you have to re-execute all of the
necessary code.



Reading data from a pickle
~~~~~~~~~~~~~~~~~~~~~~~~~~

Continuing with the notebook we are going to load a pickle file that is automatically written away when the engine
runs. First we must locate the folder where it is saved. By default this is in:

::

  ESKAPADE/results/$MACRO/proc_service_data/v$VERSION/latest/eskapade.core.process_services.DataStore.pkl'

Where ``$MACRO`` is the macro name you specified in the settings, ``$VERSION`` is the version you specified and
``latest`` refers to the last chain you wrote to disk. By default, the version is ``0`` and the name is ``v0`` and the chain is
the last chain of your macro.

You can write a specific chain with the `command line arguments <command_line_arguments.html>`_,
otherwise use the default, the last chain of the macro.

Now we are going to load the pickle from tutorial_1.

So make a new cell in Jupyter and add:

.. code-block:: python

  from eskapade import DataStore

to import the DataStore module. Now to import the actual pickle and convert it back to the DataStore do:

.. code-block:: python

  ds = DataStore.import_from_file(os.environ['ESKAPADE']+'/results/Tutorial_1/proc_service_data/v0/latest/eskapade.core.process_services.DataStore.pkl')

to open the saved DataStore into variable ``ds``. Now we can call the keys of the DataStore with

.. code-block:: python

  ds.Print()

We see there are two keys: ``data`` and ``transformed_data``. Call one of them and see what is in there. You will find
of course the pandas DataFrames that we used in the tutorial. Now you can use them in the notebook environment
and directly interact with the objects without running the entirety of Eskapade.

Similarly you can open old ConfigObject and DataStore objects if they are available.
By importing and calling:

.. code-block:: python

  from eskapade import ConfigObject
  settings = ConfigObject.import_from_file(os.environ['ESKAPADE']+'/results/Tutorial_1/proc_service_data/v0/latest/eskapade.core.process_services.ConfigObject.pkl')

one can import the saved singleton at the path. The singleton can be any of the above mentioned stores/objects.
Finally, by default there are soft-links in the results directory at ``results/$MACRO/proc_service_data/$VERSION/latest/``
that point to the pickles of the associated objects from the last chain in the macro.
