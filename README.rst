========
Eskapade
========

Version: 0.6

Eskapade is a light-weight, python-based data analysis framework, meant for all sorts of data analysis problems.


Release notes
=============

Version 0.6
-----------

The primary feature of version 0.6 (August 2017) is the inclusion of Spark, but this version
also includes several other new features and analyses.

We include multiple Spark links and 10 Spark examples on:

* The configuration of spark, reading, writing and converting spark dataframes, applying functions and queries to dataframes,
  filling histograms and (very useful!) applying arbitrary functions (e.g. pandas) to groupby calls.

In addition we hade added:

* A ROOT analysis for studying and quantifying between sets of (non-)categorical and observables.
  This is useful for finding outliers in arbitrary datasets (e.g. surveys), and we include a tutorial of how to do this.
* A ROOT analysis on predictive maintenance that decomposes a distribution of time difference between malfunctions
  by fitting this multiple Weibull distributions.
* New flexible features to create and chain analysis reports with several analysis and visualization links.

Version 0.5
-----------

Our 0.5 release (May 2017) contains multiple new features, in particular:

* Support for ROOT, including multiple examples on using data analysis, fitting and simulation examples using RooFit.
* Histogram conversion and filling support, using ROOT, numpy, Histogrammar and Eskapade-internal histograms.
* Automated data-quality fixes for buggy columns datasets, including data type fixing and NaN conversion.
* New visualization utilities, e.g. plotting multiple types of (non-linear) correlation matrices and dendograms.
* And most importantly, many new and interesting example macros illustrating the new features above!

Version 0.4
-----------

In our 0.4 release (Feb 2017) we are releasing the core code to run the framework. It is written in python 3.
Anyone can use this to learn Eskapade, build data analyses with the link-chain methodology,
and start experiencing its advantages.

The focus of the provided documentation is on constructing a data analysis setup in Eskapade.
Machine learning interfaces will be included in an upcoming release. Stay tuned!


Installation
============

Eskapade on your own machine
----------------------------

The repository is hosted on github, clone it to your machine with:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git

See the readme's in other parts of the repository for specific requirements and usage.


Requirements
------------

Eskapade requires Python 3 and Anaconda version 4.3 (or greater), which can be found `here <https://www.continuum.io/downloads>`_.


Path
----
To get started, source Eskapade in the root of the repository:

.. code-block:: bash

  $ source setup.sh

You can now call the path of Eskapade with:

.. code-block:: bash

  $ echo $ESKAPADE

or in python with

.. code-block:: python

  import os
  os.environ['ESKAPADE']


Documentation
=============

The entire documentation including tutorials can be found `here <http://eskapade.readthedocs.io>`_.


Contact and support
===================

Contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.
