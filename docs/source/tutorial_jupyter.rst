=========================
Tutorial Jupyter Notebook
=========================

This section contains materials on how to use Eskapade in Jupyter Notebooks. There are additional side notes on how certain
aspects work and where to find parts of the code. For more in depth explanations, try the `API-docs <code.html>`_.


Eskapade in Jupyter notebook
============================

In this section we will demonstrate how Eskapade can be run and debugged interactively from within a jupyter notebook.
Do not forget to set up the environment before starting the notebook (and in case you use a virtual environment
activate it):

.. code-block:: bash

  $ cd $ESKAPADE
  $ source setup.sh
  $ cd some-working-dir
  $ jupyter notebook

Executing Eskapade
------------------

To run Eskapade use the ``make_notebook.sh`` script in ``scripts/`` to create a template notebook. For example:

.. code-block:: bash

  $ make_notebook.sh ./ TestRun

The minimal code you need to run a notebook is the following:

.. code-block:: python

  import imp
  import logging
  imp.reload(logging)
  log = logging.getLogger()
  log.setLevel(logging.DEBUG) # Set the LogLevel here

  from eskapade.core import execution
  from eskapade import ConfigObject, DataStore, ProcessManager

  # --- basic config
  settings = ProcessManager().service(ConfigObject)
  settings['macro'] = os.environ['ESKAPADE'] + '/tutorials/tutorial_1.py'
  settings['analysisName'] = 'Tutorial_1'
  settings['version'] = 0
  settings['logLevel'] = logging.DEBUG # and set the LogLevel here 

  # --- optional running parameters
  #settings['beginWithChain'] = 'startChain'
  #settings['endWithChain'] = 'endChain'
  #settings['resultsDir'] = 'resultsdir'
  settings['storeResultsEachChain'] = True

  # --- other global flags (just some examples)
  settings['set_mongo'] = False
  settings['set_training'] = False

  # --- run eskapade!
  execution.run_eskapade(settings)

  # --- To rerun eskapade, clear the memory state first!
  #execution.reset_eskapade()
  

Make sure to fill out all the necessary parameters for it to run. The macro has to be set obviously, but not all
settings in this example are needed to be set to a value. The function ``execution.run_eskapade(settings)`` runs
Eskapade with the settings your specified.

Debugging
---------

To inspect the state of the Eskapade objects (DataStore and Configurations) after the various chains see the
command line examples below.
.. note::

  Inspecting intermediate states requires Eskapade to be run with the option storeResultsEachChain
  (command line: ``-w``) on.

.. code-block:: python

  import imp
  import logging
  imp.reload(logging)
  log = logging.getLogger()
  log.setLevel(logging.DEBUG) 

  from eskapade import DataStore, ConfigObject, ProcessManager

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

Tutorial 3: running in a notebook
=================================

In this tutorial we will make a notebook and run the macro from `tutorial 1 <tutorial.html#advanced-macro-s>`_. This
macro shows the basics of Eskapade. Once we have Eskapade running in a terminal, we can run it also in jupyter.
Make sure you have properly `installed jupyter <installation#making-jupyter-run-with-the-right-python-kernel>`_.

We start by making a notebook:

.. code-block:: bash

  $ make_notebook.sh tutorials/ tutorial_3_notebook 

This will create a notebook in ``tutorials/`` with the name ``tutorial_3_notebook`` running
macro ``tutorial_1.py``. Now open jupyter and take a look at the notebook.

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

*Intermezzo: you can run bash commands in jupyter by prepending the command with a !*

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
the jupyter python kernel was still running the object still existed and running the macro gave an error. The macro
tried to make a singleton, but it already exists. Therefore the final line in the notebook template has to be ran every
time you want to rerun Eskapade. So run this line:

.. code-block:: python

  execution.reset_eskapade()

And try to rerun the notebook without restarting the kernel.

.. code-block:: python

  execution.run_eskapade(settings)

If one wants to call the objects used in the run, ``execute`` contains them. For example calling

.. code-block:: python

  ds = ProcessManager().service(DataStore)

is the DataStore, and similarly the other 'master' objects can be called.
Resetting will clear the process manager singleton from memory, and now the macro can be rerun without any errors.

Note: restarting the jupyter kernel also works, but might take more time because you have to re-execute all of the
necessary code.


Reading data from a pickle
==========================

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

So make a new cell in jupyter and add:

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


Writing a new Link using jupyter and notebooks
==============================================

This section contains a general description on how to use Eskapade in combination with other tools. *This is not part
of the tutorial.*

Running the framework works best from the command line (in our experience), but running experiments and trying new
ideas is better left to an interactive environment like jupyter. How can we reconcile the difference in these work
flows? How can we use them together to get the most out of it?

Well, when using the `data and config import functionality <tutorial_jupyter.html#reading-data-from-a-pickle>`_ of
Eskapade together with jupyter we can interactively work on our objects and when we are satisfied with the results
integration into links is straight-forward. The steps to undertake this are *in general* the following:

  1. Import the DataStore and/or ConfigObject. Once you have imported the ConfigObject, run it to generate the output you want to use.
  2. Grab the data you want from the DataStore using ``ds = DataStore`` and ``data = ds[key]``.
  3. Now you can apply the operation you want to do on the data, experiment on it and work towards the end result you
     want to have.
  4. Create a new link in the appropriate link folder using the make_link script.
  5. Copy the operations (code) you want to do to the link.
  6. Add assertions and checks to make sure the Link is safe to run.
  7. Add the Link to your macro and run it!

These steps are very general and we will now go into steps 5, 6 and 7. Steps 1, 2, 3 and 4 have already been covered
by various parts of the documentation.

So assuming you wrote some new functionality that you want to add to a Link called YourLink and you have created a new
Link from the template we are going to describe how you can get your code into the Link and test it.


Developing Links in notebooks
=============================

This subsection starts with a short summary of the workflow for developing Links:

  1. Make your code in a notebook
  2. Make a new Link
  3. Port the code into the Link
  4. Import the Link into your notebook
  5. Test if the Link has the desired effect.
  6. Finish the Link code
  7. Write a unit test (optional but advised if you want to contribute)

We continue with a longer description of the steps above.

When adding the new code to a new link the following conventions are used:

In the ``__init__`` you specify the key word arguments of the Link and their default values, if you want to get an
object from the DataStore or you want to write an object back into it, use the name ``read_key`` and ``store_key``.
Other keywords are free to use as you see fit.

In the ``initialize`` function in the Link you define and initialize functions that you want to call when executing the
code on your objects. If you want to import something, you can do this at the root of the Link, as per PEP8.

In the ``execute`` function you put the actual code in this format:

.. code-block:: python

  settings = ProcessManager().service(ConfigObject)
  ds = ProcessManager().service(DataStore)

  ## --- your code follows here

Now you can call the objects that contain all the settings and data of the macro in your Link, and in the code below
you can add your analysis code that calls from the objects and writes back in case that this is necessary. Another
possibility is writing a file to the disk, for example writing out a plot you made.

If you quickly want to test the Link without running the entire Eskapade framework, you can import it into your
notebook sessions:

.. code-block:: python

  import eskapade.analysis.links.yourlink
  from yourlink import YourLink
  l = YourLink()
  l.execute()

should run your link. You can also call the other functions. However, ``execute()`` is supposed to contain the bulk of your
operations, so running that should give you your result. Now you can change the code in your link if it is not how you
want it to run. The notebook kernel however keeps everything in memory, so you either have to restart the kernel, or
use

.. code-block:: python

  import imp
  imp.reload(eskapade.analysis.links.yourlink)
  from yourlink import YourLink
  l = YourLink()
  l.execute()

to reload the link you changed. This is equivalent to the python2 function ``reload(eskapade)``.

Combined with the importing of the other objects it becomes clear that you can run every piece of the framework from
a notebook. However working like this is only recommended for development purposes, running an entire analysis should
be done from the command line.

Finally after finishing all the steps you use the function ``finalize()`` to clean up all objects you do not want to
save.

After testing whether the Link gives the desired result you have to add the proper assertions and other types of checks
into your Link code to make sure that it does not have use-cases that are improperly defined. It is advised that you
also write a unit test for the Link, but unless you want it merged into the master, it will not be enforced.

Now you can run Eskapade with your macro from your command line, using the new functionality that you first created
in a notebook and then ported into a stand-alone Link.
