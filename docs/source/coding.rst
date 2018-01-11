Tips on coding
--------------

This section contains a general description on how to use Eskapade in combination with other tools,
in particular for the purpose of developing code.



Eskapade in PyCharm
~~~~~~~~~~~~~~~~~~~

PyCharm is a very handy IDE for debugging Python source code. It can be used to run Eskapade stand-alone
(i.e. like from the command line) and with an API.

Stand-alone
  * Install PyCharm on your machine.
  * Open project and point to the Eskapade source code
  * Configuration, in 'Preferences', check the following desired values:
      - Under 'Project: eskapade' / 'Project Interpreter':
          - The correct Python version (Python 3)
      - Under 'Build, Execution & Deployment' / 'Console' / 'Python Console':
          - The correct Python version (Python 3)
  * Install Eskapade in editable mode
  * Run/Debug Configuration:
      - Under 'Python' add new configuration
      - Script: path to the console script ``eskapade_run`` (located in the same directory as the interpreter
        specified above in 'Project Interpreter')
      - Script parameters: path to a macro to be debugged, e.g. ``$ESKAPADE/python/eskapade/tutorials/tutorial_1.py``,
        and ``eskapade_run`` command line arguments, e.g. ``--begin-with=Summary``
      - Python interpreter: check if it is the correct Python version (Python 3)

You should now be able to press the 'play button' to run Eskapade with the specified parameters.



Writing a new Link using Jupyter and notebooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Running the framework works best from the command line (in our experience), but running experiments and trying new
ideas is better left to an interactive environment like Jupyter. How can we reconcile the difference in these work
flows? How can we use them together to get the most out of it?

Well, when using the `data and config import functionality <tutorial_jupyter.html#reading-data-from-a-pickle>`_ of
Eskapade together with Jupyter we can interactively work on our objects and when we are satisfied with the results
integration into links is straight-forward. The steps to undertake this are *in general* the following:

  1. Import the DataStore and/or ConfigObject. Once you have imported the ConfigObject, run it to generate the output you want to use.
  2. Grab the data you want from the DataStore using ``ds = DataStore.import_from_file`` and ``data = ds[key]``.
  3. Now you can apply the operation you want to do on the data, experiment on it and work towards the end result you
     want to have.
  4. Create a new link in the appropriate link folder using the eskapade_generate_link command.
  5. Copy the operations (code) you want to do to the link.
  6. Add assertions and checks to make sure the Link is safe to run.
  7. Add the Link to your macro and run it!

These steps are very general and we will now go into steps 5, 6 and 7. Steps 1, 2, 3 and 4 have already been covered
by various parts of the documentation.

So assuming you wrote some new functionality that you want to add to a Link called YourLink and you have created a new
Link from the template we are going to describe how you can get your code into the Link and test it.


Developing Links in notebooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

  settings = process_manager.service(ConfigObject)
  ds = process_manager.service(DataStore)

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

  import importlib
  importlib.reload(eskapade.analysis.links.yourlink)
  from yourlink import YourLink
  l = YourLink()
  l.execute()

to reload the link you changed. This is equivalent to the Python2 function ``reload(eskapade)``.

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
