=========
Tutorials
=========

This section contains materials on how to use Eskapade. There are additional side notes on how certain
aspects work and where to find parts of the code. For more in depth explanations on the functionality of the code-base,
try the `API docs <eskapade_index.html>`_.

All command examples below can be run from any directory with write access.



Running your first macro
------------------------

After successfully `installing <installation.html>`_ Eskapade, it is now time to run your very first
macro, the classic code example: Hello World!

For ease of use, let's make a shortcut to the directory containing the Eskapade tutorials:

.. code-block:: bash

  $ export TUTDIR=`pip show Eskapade | grep Location | awk '{ print $2"/eskapade/tutorials" }'`
  $ ls -l $TUTDIR/


Hello World!
~~~~~~~~~~~~

If you just want to run it plain and simple, go to the root of the repository and run the following:

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk101_helloworld.py

This will run the macro that prints out Hello World. There is a lot of output, but try to find back these
lines (or similar):

.. code-block:: python

   2017-11-13T12:37:07.473512+00:00 [eskapade.core_ops.links.hello_world.HelloWorld#INFO] Hello World
   2017-11-13T12:37:07.473512+00:00 [eskapade.core_ops.links.hello_world.HelloWorld#INFO] Hello World

Congratulations, you have just successfully run Eskapade!


Internal workings
~~~~~~~~~~~~~~~~~

To see what is actually happening under the hood, go ahead and open up ``tutorials/esk101_helloworld.py``.
The macro is like a recipe and it contains all of your analysis. It has all the 'high level' operations that are to be
executed by Eskapade.

When we go into this macro we find the following piece of code:

.. code-block:: python

  hello = Chain(name='Hello')
  link = core_ops.HelloWorld(name='HelloWorld')
  link.logger.log_level = LogLevel.DEBUG
  link.repeat = settings['n_repeat']
  hello.add(link)

Which is the code that does the actual analysis (in this case, print out the statement). In this case ``link`` is an
instance of the class HelloWorld, which itself is a Link. The Link class is the fundamental building block in Eskapade that
contains our analysis steps. The code for HelloWorld can be found at:

.. code-block:: bash

  $ less python/eskapade/core_ops/links/hello_world.py

Looking into this class in particular, in the code we find in the ``execute()`` function:

.. code-block:: python

  self.logger.info('Hello {hello}', hello=self.hello)

where ``self.hello`` is a parameter set in the ``__init__`` of the class. This setting can be overwritten as can be seen
below. For example, we can make another link, ``link2`` and change the default ``self.hello`` into something else.

.. code-block:: python

  link2 = core_ops.HelloWorld(name='Hello2')
  link2.hello = 'Lionel Richie'
  ch.add(link2)

Rerunning results in us greeting the famous singer/songwriter.

There are many ways to run your macro and control the flow of your analysis. You can read more on this in
the `Short introduction to the Framework`_ subsection below.


Tutorial 1: transforming data
-----------------------------

Now that we know the basics of Eskapade we can go on to more advanced macros, containing an actual analysis.

Before we get started, we have to fetch some data, on your command line, type:

.. code-block:: bash

  $ wget https://s3-eu-west-1.amazonaws.com/kpmg-eskapade-share/data/LAozone.data

To run the macro type on your CLI:

.. code-block:: bash

  $ eskapade_run $TUTDIR/tutorial_1.py

If you want to add command line arguments, for example to change the output logging level, read the
page on `command line arguments <command_line_arguments.html>`_.

When looking at the output in the terminal we read something like the following:

::

   2017-11-13T13:37:07.473512+00:00 [eskapade.core.execution#INFO] *              Welcome to Eskapade!                *
   ...
   2017-11-13T13:37:08.085577+00:00 [eskapade.core.process_manager.ProcessManager#INFO] Number of registered chains: 2
   ...
   2017-11-13T13:37:11.316414+00:00 [eskapade.core.execution#INFO] *              Leaving Eskapade. Bye!              *

There is a lot more output than these lines (tens or hundred of lines depending on the log level).
Eskapade has run the code from each link, and at the top of the output in your terminal you can see a summary.

When you look at the output in the terminal you can see that the macro contains two chains and a few Link are contained
in these chains. Note that chain 2 is empty at this moment. In the code of the macro we see that in the first chain
that data is loaded first and then a transformation is applied to this data.

Before we are going to change the code in the macro, there will be a short introduction to the framework.

Short introduction to the Framework
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At this point we will not go into the underlying structure of the code that is underneath the macro, but later in this
tutorial we will. For now we will take a look in the macro. So open ``$TUTDIR/tutorial_1.py`` in your
favorite editor. We notice the structure: first imports, then defining all the settings, and finally the actual
analysis: Chains and Links.

A chain is instantiated as follows:

.. code-block:: python

  data = Chain('Data')

and registered automatically with the ProcessManager. The ProcessManager is the main event
processing loop and is responsible for processing the Chains and Links.

Next a Pandas data frame converter Link is initialized and its properties are set, and finally added to the data chain:

.. code-block:: python

  reader = analysis.ReadToDf(name='Read_LA_ozone', path='LAozone.data', reader=pd.read_csv, key='data')
  data.add(reader)

This means the Link is added to the chain and when Eskapade runs, it will execute the code in the Link.

Now that we know how the framework runs the code on a higher level, we will continue with the macro.

In the macro notice that under the second chain some code has been commented out.
Uncomment the code and run the macro again with:

.. code-block:: bash

  $ eskapade_run $TUTDIR/tutorial_1.py

And notice that it takes a bit longer to run, and the output is longer, since it now executes the Link in chain 2. This Link takes the data from chain 1
and makes plots of the data in the data set and saves it to your disk. Go to this path and open one of the pdfs found
there:

.. code-block:: bash

  $ results/Tutorial_1/data/v0/report/

The pdfs give an overview of all numerical variables in the data in histogram form. The binning, plotting and saving
of this data is all done by the chain we just uncommented. If you want to take a look at how the Link works, it can be
found in:

.. code-block:: bash

  $ python/eskapade/visualization/links/df_summary.py

But for now, we will skip the underlying functionality of the links.

Let's do an exercise. Going back to the first link, we notice that the transformations that are executed are defined in ``conv_funcs`` passed to the link.
We want to include in the plot the wind speed in km/h. There is already a
part of the code available in the ``conv_funcs`` and the functions ``comp_date`` and ``mi_to_km``. Use these functions
as examples to write a function that converts the wind speed.

Add this to the transformation by adding your own code. Once this works you can also try to add the
temperature in degrees Celsius.

Making a Link
~~~~~~~~~~~~~

Now we are going to add a new link that we create! To make a new link type the following:

.. code-block:: bash

  $ eskapade_generate_link --dir python/eskapade/analysis/links YourLink

The command will make a link object named ``YourLink`` in the path specified in the first argument.
The link we wish to add will do some textual transformation, so name it accordingly.
And be sure to follow the instructions given by the command!

The command creates the skeleton file:

.. code-block:: bash

  $ python/eskapade/analysis/links/yourlink.py

This skeleton file can be modified with your custom editor and then be imported and called inside a macro with
``analysis.YourLink()``. Notice that the name of the class is CamelCase and that the name of the file is lowercase
to conform to coding guidelines.

Now open up the link in your editor.
In the ``execute`` function of the Link, we see that a DataStore is called. This is the central in-memory object in
which all data is saved. DataStore inherits from a dict, so by calling the right key we can get objects. Call:

.. code-block:: python

  df = ds['data']

to get the DataFrame that includes the latest transformations.

Now we are going to make a completely different
transformation in the Link and apply it to the object in the DataStore. We want to add a column to the data that
states how humid it is. When column 'humidity' is less than 50 it is 'dry', otherwise it is 'humid'.
You will have to use some pandas functionality or perhaps something else if you prefer. Save the
new column back into the DataFrame and then put the DataFrame in the DataStore under the key 'data_new'.

We are going to let our plot functionality loose on this DataFrame once more, to see what happens to our generated
textual data. It can not be plotted. In the future this functionality will be available for most data types.

Now run the entire macro with the new code and compile the output .tex file. This can be done on the command line with

.. code-block:: bash

  $ cd results/Tutorial_1/data/v0/report/
  $ pdflatex report.tex

If you have pdflatex installed on your machine.

.. note::

   If you don't have pdflatex installed on your machine you can install it by executing the following command:
   .. code-block:: bash

      $ yum install texlive-latex-recommended


Now take a look at the output pdf. The final output should look something like this:

.. image:: ../images/output_tutorial_1.png

Your plot should be quite similar (though it might be different in its make up.)

In summary, the work method of Eskapade is to run chains of custom code chunks (links).
Each chain should have a specific purpose, for example pre-processing incoming data, booking and/or
training predictive algorithms, validating these predictive algorithms, evaluating the algorithms.

By using this work method, links can be easily reused in future projects. Some links are provided by default.
For example, links used to load data from a json file, book predictive algorithms, predict the training and
test data set and produce evaluation metrics. If you want to use your own predictive model just go ahead and add your own links!


Tutorial 2: macro from basic links
----------------------------------

In this tutorial we are going to build a macro using existing Links. We start by using templates to make a new macro.
The command

.. code-block::  bash

  $ eskapade_generate_macro --dir python/eskapade/tutorials tutorial_2

makes a new macro from a template macro.
When we open the macro we find a lot of options that we can use. For now we will actually not use them, but if you want
to learn more about them, read the `Examples <tutorial.html#examples>`_ section below.

First we will add new chains to the macro. These are the higher level building blocks that can be controlled when
starting a run of the macro. At the bottom of the macro we find a commented out Link, the classic Hello World link.
You can uncomment it and run the macro if you like, but for now we are going to use the code to make a few chains.

So use the code and add 3 chains with different names:

.. code-block:: python

  ch = Chain('CHAINNAME')

When naming chains, remember that the output of Eskapade will print per chain-link combination the logs that are
defined in the Links. So name the chains appropriately, so when you run the macro the logging actually makes sense.

This tutorial will be quite straight-forward, it has 3 short steps, which is why we made 3 chains.

1. In the first chain: Read a data file of your choosing into Eskapade using the pandas links in the analysis
   subpackage.
2. In the second chain: Copy the DataFrame you created in the DataStore using the core_ops subpackage.
3. In the third chain: Delete the entire DataStore using a Link in the core_ops subpackage.

To find the right Links you have to go through the Eskapade documentation (or code!), and to find within its subpackages
the proper Links you have to understand the package structure.
Every package is specific for a certain task, such as analysis, core tasks (like the ``ProcessManager``), or data
quality. Every subpackage contains links in its ``links/`` subdirectory.
See for example the subpackages ``core_ops``, ``analysis`` or ``visualization``.

In `All available examples`_ we give some tips to find the right Links your analysis, and how to configure them properly.


.. include:: tutorial_jupyter.rst
.. include:: tutorial_bootstrap.rst
.. include:: tutorial_data_mimic.rst

All available examples
----------------------

To see the available Eskapade example, do:

.. code-block:: bash

  $ export TUTDIR=`pip show Eskapade | grep Location | awk '{ print $2"/eskapade/tutorials" }'`
  $ ls -l $TUTDIR/

Many Eskapade example macros exist in the tutorials directory.
The numbering of the example macros follows the package structure:

* ``esk100+``: basic macros describing the chains, links, and datastore functionality of Eskapade.
* ``esk200+``: macros describing links to do basic processing of pandas dataframes.
* ``esk300+``: visualization macros for making histograms, plots and reports of datasets.
* ``esk500+``: macros for doing data quality assessment and cleaning.

The Eskapade macros are briefly described below.
They explain the basic architecture of Eskapade,
i.e. how the chains, links, datastore, and process manager interact.

Hopefully you now have enough knowledge to run and explore Eskapade by yourself.
You are encouraged to run all examples to see what Eskapade can do for you!



Example esk101: Hello World!
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 101 runs the Hello World Link. It runs the Link twice using a repeat kwarg, showing how to use kwargs in Links.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk101_helloworld.py 


Example esk102: Multiple chains
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 102 uses multiple chains to print different kinds of output from one Link. This link is initialized multiple
times with different kwargs and names. There are if-statements in the macro to control the usage of the chains.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk102_multiple_chains.py


Example esk103: Print the DataStore
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 103 has some objects in the DataStore. The contents of the DataStore are printed in the standard output.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk103_printdatastore.py


Example esk104: Basic DataStore operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 104 adds some objects from a dictionary to the DataStore and then moves or deletes some of the items. Next it
adds more items and prints some of the objects.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk104_basic_datastore_operations.py


Example esk105: DataStore Pickling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 105 has 3 versions: A, B and C. These are built on top of the basic macro esk105. Each of these 3 macro's does
something slightly different:

* A does not store any output pickles,
* B stores all output pickles,
* C starts at the 3rd chain of the macro.

Using these examples one can see how the way macro's are run can be controlled and what it saves to disk.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk105_A_dont_store_results.py
  $ eskapade_run $TUTDIR/esk105_B_store_each_chain.py
  $ eskapade_run $TUTDIR/esk105_C_begin_at_chain3.py


Example esk106: Command line arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 106 shows us how command line arguments can be used to control the chains in a macro. By adding the arguments
from the message inside of the macro we can see that the chains are not run.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk106_cmdline_options.py


Example esk107: Chain loop
~~~~~~~~~~~~~~~~~~~~~~~~~~

Example 107 adds a chain to the macro and using a repeater Link it repeats the chain 10 times in a row.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk107_chain_looper.py


Example esk108: Event loop
~~~~~~~~~~~~~~~~~~~~~~~~~~

Example 108 processes a textual data set, to loop through every word and do a Map and Reduce operation on the data set.
Finally a line printer prints out the result.

.. code-block:: bash

  $ source $TUTDIR/esk108_eventlooper.sh


Example esk109: Debugging tips
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro illustrates basic debugging features of Eskapade.
The macro shows how to start a python session while
running through the chains, and also how to break out of a chain.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk109_debugging_tips.py


Example esk110: Code profiling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro demonstrates how to run Eskapade with code profiling turned on.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk110_code_profiling.py


Example esk201: Read data
~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 201 illustrates how to open files as pandas datasets.
It reads a file into the DataStore. The first chain reads one csv into the DataStore, the second chain reads
multiple files (actually the same file multiple times) into the DataStore. (Looping over data is shown in example
esk209.)

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk201_readdata.py


Example esk202: Write data
~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro 202 illustrate writing pandas dataframes to file.
It reads a DataFrame into the data store and then writes the DataFrame to csv format on the disk.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk202_writedata.py


Example esk203: apply func to pandas df
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Illustrates the link that calls basic apply() to columns of a pandas dataframes.
See for more information pandas documentation:

http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.apply.html

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk203_apply_func_to_pandas_df.py



Example esk204: apply query to pandas df
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Illustrates the link that applies basic queries to pandas dataframe.
See for more information pandas documentation:

http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.query.html

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk204_apply_query_to_pandas_df.py



Example esk205: concatenate pandas dfs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Illustrates the link that calls basic concat() of pandas dataframes.
See for more information pandas documentation:

http://pandas.pydata.org/pandas-docs/stable/merging.html

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk205_concatenate_pandas_dfs.py



Example esk206: merge pandas dfs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Illustrate link that calls basic merge() of pandas dataframes.
For more information see pandas documentation:

http://pandas.pydata.org/pandas-docs/stable/merging.html

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk206_merge_pandas_dfs.py



Example esk207: record vectorizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro performs the vectorization of an input column of an input dataframe.
E.g. a columnn x with values 1, 2 is tranformed into columns x_1 and x_2,
with values True or False assigned per record.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk207_record_vectorizer.py


Example esk208: record factorizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro performs the factorization of an input column of an input
dataframe.  E.g. a columnn x with values 'apple', 'tree', 'pear',
'apple', 'pear' is tranformed into columns x with values 0, 1, 2, 0, 2.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk208_record_factorizer.py



Example esk209: read big data itr
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro to that illustrates how to loop over multiple (possibly large!) datasets in chunks.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk209_read_big_data_itr.py



Example esk301: dfsummary plotter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro shows how to plot the content of a dataframe in a nice summary pdf file.
(Example similar to ``$TUTDIR/tutorial_1.py``.)

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk301_dfsummary_plotter.py



Example esk302: histogram_filler_plotter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro that illustrates how to loop over multiple (possibly large!)
datasets in chunks, in each loop fill a (common) histogram, and plot the
final histogram.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk302_histogram_filler_plotter.py



Example esk303: histogrammar filler plotter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro that illustrates how to loop over multiple (possibly large!)
datasets in chunks, in each loop fill a histogrammar histograms,
and plot the final histograms.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk302_histogram_filler_plotter.py



Example esk304: df boxplot
~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro shows how to boxplot the content of a dataframe in a nice summary pdf file.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk304_df_boxplot.py




Example esk305: correlation summary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro to demonstrate generating nice correlation heatmaps using various types of correlation coefficients.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk305_correlation_summary.py


Example esk306: concatenate reports
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro illustrates how to concatenate the reports of several
visualization links into one big report.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk306_concatenate_reports.py



Example esk501: fix pandas dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro illustrates how to call FixPandasDataFrame link that gives columns
consistent names and datatypes.
Default settings perform the following clean-up steps on an
input dataframe:

* Fix all column names. Eg. remove punctuation and strange characters, and convert spaces to underscores.
* Check for various possible nans in the dataset, then make all nans consistent by turning them into numpy.nan (= float)
* Per column, assess dynamically the most consistent datatype (ignoring all nans in that column). Eg. bool, int, float, datetime64, string.
* Per column, make the data types of all rows consistent, by using the identified (or imposed) data type (by default ignoring all nans)

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk501_fix_pandas_dataframe.py

Example esk701: Mimic dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro that illustrates how to resample a dataset using kernel density estimation (KDE). The macro can handle contiunous data, and both ordered and unordered catagorical data. 
The macro is build up in the following way:

* A dataset is simulated containing mixed data types, representing general input data.
* Some cleaning steps are performed on the dataset
* KDE is applied to the dataset
* Using the estimated bandwidths of the KDE, the data is resampled
* An evaluation is done on the resulting resimulated dataset

.. code-block:: bash
  
  $ eskapade_run $TUTDIR/esk701_mimic_data.py

Example esk702: DoF fitter
~~~~~~~~~~~~~~~~~~~~~~~~~~

Macro to run a DoF fitter

.. code-block:: bash

  $eskapade_run $TUTDIR/esk702_dof_fitter.py


.. include:: coding.rst
