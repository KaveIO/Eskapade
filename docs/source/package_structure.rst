=================
Package structure
=================

Eskapade contains many tools, and to find and use them most efficiently it is necessary to understand how the
repository is build up. This section discusses the structure of the code and how the framework handles

Structure
---------

When using Eskapade it is important to understand where all components are located. The components can be for
example Links or utilities that you want to use.

The ``$eskapade/python`` folder contains the framework and has the following structure: Every specific subject has is a module
containing the utilities it needs, the links that are defined for the subject, and the tests.
There are two exceptions to this: The ``core`` module contains the core of the framework, like the process manager.
The ``core_ops`` module contains higher level functionality built on top of the core.

For example:

The visualization module is in ``/python/eskapade/visualisation`` and contains a link called ``DfSummary`` in the
``df_summary`` file. This link is located in the ``links`` folder, and uses some tools from ``vis_utils``, the
visualization utilities, contained in the root of module. The unittests are located in ``tests`` and can be run with
the unittest framework. An overview of the structure for visualization::

  |-python
     |-eskapade
        |-visualization
           |-tests
           |-links


This structure is used for every module.
Since every Link is a class, it uses the camel case naming convention, while the files in the modules use snake case.
This can be seen in the way imports work, as seen below.

Imports
-------

Taking the structure into account, it becomes clear how to import functionality into a macro. For importing the core
objects use:

.. code-block:: python

  from eskapade import ConfigObject, ProcessManager

And for getting links and other modules, for example, use:

.. code-block:: python

  from eskapade.visualization import DfSummary

Now you can call your Link ``DfSummary`` in your macro and add it to the process manager.

Results
-------

Results of a macro are written out by default in the ``$eskapade/results`` folder. Analyses are saved in the results
folder by their ``$analysis_name`` given in the macro. Every saved analysis has the following underlying folders:

  * config, containing the configuration of the macro
  * data, containing input data (or pointers), in-between states that you want to save explicitly, results that
    such as graphs, and a trained model

The subfolders save the files by version of the analysis code that you run, defaulting to: ``v0, v1, v2, ...``
For example the output of tutorial ``esk304_df_boxplot`` is saved in the folder:
``./results/esk304_df_boxplot/data/v0/report/``.

Similarly config and other objects are saved in their respective folders.

Debugging
---------

When building new Links or other functionality you will want to debug at some point. There are multiple ways to do
this, because there are multiple ways of running the framework. A few ways are:

  * Running in the terminal. In this scenario you have to work in a virtual environment (or adjust your own until it
    has all dependencies) and debug using the terminal output.
  * Running in a notebook. This way the code is run in a notebook and you can gather the output from the browser.
  * Running in a docker. The code is run in the docker and the repository is mounted into the container. The docker
    (terminal) returns output.
  * Running in a VM. In this case you run the code in the VM and mount the code into the VM. The output can be
    gathered in the VM and processed in the VM.

In the first three options you want to use an IDE or text-editor in a 'normal' environment to debug your code and in
the last option you can use an editor in the VM or outside of it.

Troubleshooting
---------------

One of the easiest mistakes to make when running the framework is not sourcing the right files or opening a new
terminal without setting the right environment. Be careful of this, if you want to run eskapade you have to:

  * Source the right virtual environment
  * Source the eskapade repository
  * Start your notebook / start your IDE / run the code

The least error prone ways are docker and VMs, because they automatically have the right environment variables set.