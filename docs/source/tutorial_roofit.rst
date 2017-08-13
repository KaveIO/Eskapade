Tutorial 4: using RooFit
------------------------

This section provides a tutorial on how to use RooFit in Eskapade. RooFit is an advanced fitting library in ROOT, which is great
for modelling all sorts of data sets.
ROOT (and RooFit) works 'out of the box' in the Eskapade docker/vagrant image.

In this tutorial we will illustrates how to define a new probability density function (pdf) in RooFit,
how to compile it, and how to use it in Eskapade to simulate a dataset, fit it, and plot the results.

.. note::

   There are many good RooFit tutorials. See the macros in the directory ``$ROOTSYS/tutorials/roofit/`` of your local ROOT installation.
   This tutorial is partially based on the RooFit tutorial ``$ROOTSYS/tutorials/roofit/rf104_classfactory.C``.


Building a new probability density function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before using a new model in Eskapade, we need to create, compile and load a probability density function model in RooFit.

Move to the directory:

.. code-block:: bash

   $ cd $ESKAPADE/cxx/roofit/src/

Start an interactive python session and type:

.. code-block:: python

   import ROOT
   ROOT.RooClassFactory.makePdf("MyPdfV2","x,A,B","","A*fabs(x)+pow(x-B,2)")

This command creates a RooFit skeleton probability density function class named ``MyPdfV2``,
with the variable ``x, a, b`` and the given formula expression.

Also type:

.. code-block:: python

   ROOT.RooClassFactory.makePdf("MyPdfV3","x,A,B","","A*fabs(x)+pow(x-B,2)",True,False,"x:(A/2)*(pow(x.max(rangeName),2)+pow(x.min(rangeName),2))+(1./3)*(pow(x.max(rangeName)-B,3)-pow(x.min(rangeName)-B,3))")

This creates the RooFit p.d.f. class ``MyPdfV3``, with the variable ``x, a, b`` and the given formula expression,
and the given expression for analytical integral over ``x``.

Exit python ``(Ctrl-D)`` and type:

.. code-block:: bash

   $ ls -l MyPdf*

You will see two cxx files and two header files. Open the file ``MyPdfV2.cxx``.
You should see an ``evaluate()`` method in terms of ``x, a`` and ``b`` with the formula expression we provided.

Now open the file ``MyPdfV3.cxx``. This also contains the method ``analyticalIntegral()`` with the expresssion
for the analytical integral over x that we provided.

If no analytical integral has been provided, as in ``MyPdfV2``, RooFit will try to try to compute the integral
itself. (Of course this is a costly operation.) If you wish, since we know the analytical integral for ``MyPdfV2``,
go ahead and edit ``MyPdfV2.cxx`` to add the expression of the analytical integral to the class.

As another example of a simple pdf class, take a look at the expressions in the file:
``$ESKAPADE/cxx/roofit/src/RooWeibull.cxx``.

Now move the header files to their correct location:

.. code-block:: bash

   $ mv MyPdfV*.h $ESKAPADE/cxx/roofit/include/

To make sure that these classes get picked up in Eskapade roofit libary, open the file ``$ESKAPADE/cxx/roofit/dict/LinkDef.h`` and add the lines:

.. code-block:: c

   #pragma link C++ class MyPdfV2+;
   #pragma link C++ class MyPdfV3+;

Finally, let's compile the c++ code of these classes:

.. code-block:: bash

   $ cd $ESKAPADE
   $ make install

You should see the compiler churning away, processing several existing classes but also ``MyPdfV2`` and ``MyPdfV3``.

We are now able to open the Eskapade roofit library, so we can use these classes in python:

.. code-block:: python

   from eskapade.root_analysis import roofit_utils
   roofit_utils.load_libesroofit()

In fact, this last snippet of code is used in the tutorial macro right below.



Running the tutorial macro
~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's take a look at the steps in tutorial macro  ``$ESKAPADE/tutorials/tutorial_4.py``.
The macro illustrates how do basic statistical data analysis with roofit, by making use of the ``RooWorkspace`` functionality.
The example shows how to define a pdf, simulate data, fit this data, and then plot the fit result.
There are 5 sections; they are detailed in the sections below.

The next step is to run the tutorial macro.

.. code-block:: bash

  $ cd $ESKAPADE
  $ source setup.sh
  $ run_eskapade.py tutorials/tutorial_4.py

Let's discuss what we are seeing on the screen.


Loading the Eskapade ROOT library
*********************************

The macro first checks the existence of the class ``MyPdfV3`` that we just created in the previous section.

.. code-block:: python

   # --- 0. make sure Eskapade RooFit library is loaded

   # --- load and compile the Eskapade roofit library
   from eskapade.root_analysis import roofit_utils
   roofit_utils.load_libesroofit()

   # --- check existence of class MyPdfV3 in ROOT
   pdf_name = 'MyPdfV3'
   log.info('Now checking existence of ROOT class %s' % pdf_name)
   cl = ROOT.TClass.GetClass(pdf_name)
   if not cl:
       log.critical('Could not find ROOT class %s. Did you build and compile it correctly?' % pdf_name)
       sys.exit(1)
   else:
       log.info('Successfully found ROOT class %s' % pdf_name)

In the output on the screen, look for ``Now checking existence of ROOT class MyPdfV3``. If this was successful,
it should then say ``Successfully found class MyPdfV3``.


Instantiating a pdf
*******************

The link ``WsUtils``, which stands for ``RooWorkspace`` utils, allows us to instantiate a pdf.
Technically, one defines a model by passing strings to the rooworkspace factory.
For details on rooworkspace factory see this `RooWorkspace tutorial <https://root.cern.ch/root/html/tutorials/roofit/rf511_wsfactory_basic.C.html>`_ for more details.

.. code-block:: python

   ch = proc_mgr.add_chain('WsOps')

   # --- instantiate a pdf
   wsu = root_analysis.WsUtils(name = 'modeller')
   wsu.factory = ["MyPdfV3::testpdf(y[-10,10],A[10,0,100],B[2,-10,10])"]
   ch.add_link(wsu)

Here we use the pdf class we just created (``MyPdfV3``) to create a pdf called ``testpdf``, with observable ``y`` and parameter ``A`` and ``B``,
having ranges ``(-10,10)``, ``(0,100)`` and ``(-10,10)`` respectively, and with initial values for ``A`` and ``B`` of ``10`` and ``2`` respectively.

Simulating data
***************

The link ``WsUtils`` is then used to simulate records according to the shape of ``testpdf``.

.. code-block:: python

   wsu = root_analysis.WsUtils(name = 'simulater')
   wsu.add_simulate(pdf='testpdf', obs='y', num=400, key='simdata')
   ch.add_link(wsu)

Here we simulate ``400`` records of observable ``y`` with pdf ``testpdf`` (which is of type MyPdfV3).
The simulated data is stored in the datastore under key ``simdata``.


Fitting the data
****************

Another version of the link ``WsUtils`` is then used to fit the simulated records with the pdf ``testpdf``.

.. code-block:: python

   wsu = root_analysis.WsUtils(name = 'fitter')
   wsu.pages_key='report_pages'
   wsu.add_fit(pdf='testpdf', data='simdata', key='fit_result')
   ch.add_link(wsu)

The link performs a fit of pdf ``testpdf`` to dataset ``simdata``.
We store the fit result object in the datastore under key ``fit_result``.
The fit knows from the input dataset that the observable is ``y``, so that
the fit parameters are ``A`` and ``B``.


Plotting the fit result
***********************

Finally, the last version of the link ``WsUtils`` is used to plot the result of the fit on top of simulated data.

.. code-block:: python

   wsu = root_analysis.WsUtils(name = 'plotter')
   wsu.pages_key='report_pages'
   wsu.add_plot(obs='y', data='simdata', pdf='testpdf', pdf_kwargs={'VisualizeError': 'fit_result', 'MoveToBack': ()}, key='simdata_plot')
   wsu.add_plot(obs='y', pdf='testpdf', file='fit_of_simdata.pdf', key='simdata_plot')
   ch.add_link(wsu)

This link is configured to do two things.
First it plots the observable ``y`` of the the dataset ``simdata`` and then plots the fitted uncertainy band of the pdf ``testpdf`` on top of this.
The plot is stored in the datastore under the key ``simdata_plot``.
Then it plots the fitted pdf ``testpdf`` without uncertainty band on top of the same frame ``simdata_plot``.
The resulting plot is stored in the file ``fit_of_simdata.pdf``


Fit report
~~~~~~~~~~

The link ``WsUtils`` produces a summary report of the fit it has just performed.
The pages of this report are stored in the datastore under the key ``report_pages``.
At the end of the Eskapade session, the plots and latex files produced by this tutorial
are written out to disk.

The fit report can be found at:

.. code-block:: bash

  $ cd $ESKAPADE/results/tutorial_4/data/v0/report/
  $ pdflatex report.tex

Take a look at the resulting fit report: ``report.pdf``.
It contains pages summarizing: the status and quality of the fit (including the correlation matrix),
summary tables of the floating and fixed parameters in the fit, as well as the plot we have produced.



Other ROOT Examples in Eskapade
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Other example Eskapade macros using ROOT and RooFit can be found in the ``$ESKAPADE/tutorials`` directory,
e.g. see ``esk401_roothist_fill_plot_convert.py`` and all other 400 numbered macros.
