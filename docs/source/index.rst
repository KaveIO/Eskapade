.. Eskapade documentation master file, created by
   sphinx-quickstart on Thu Jul  7 14:25:54 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

==================================
Eskapade: Modular Analytics
==================================

.. module:: eskapade

**Version**: |version|

**Release**: |release|

**Date**: |today|

**Web page:** http://eskapade.kave.io

**Repository:** http://github.com/kaveio/eskapade

**Code reference:** `API Documentation <code.html>`_

**Issues & Ideas:** https://github.com/kaveio/eskapade/issues

**Q&A Support:** contact us at: kave [at] kpmg [dot] com

Eskapade
========

Eskapade is a light-weight, python-based data analysis framework, meant for all sorts of data analysis problems.

In particular, Eskapade can be used as a self-learning framework for typical machine learning problems.
Trained algorithms can predict real-time or batch data, these models can be evaluated over time,
and Eskapade can bookkeep and retrain their algorithms.

Eskapade uses a modular approach to analytics, meaning that you can use pre-made operations (called 'links') to
build an analysis. This is implemented in a chain-link framework, where you define a 'Chain', consisting of a number of
Links. These links are the fundamental building block of your analysis. For example, a data loading link and a data
transformation link will frequently be found together in a pre-processing Chain.

Each chain has a specific purpose, for example: data quality checks of incoming data, pre-processing of data,
booking and/or training of predictive algorithms, validation of these algorithms, and their evaluation.
By using this work methodology, analysis links can be more easily reused in future data analysis projects.

Eskapade is analysis-library agnostic. It is used to set up typical data analysis problems from multiple packages.
The machine learning packages include:

  - scikit-learn,
  - Spark MLlib,
  - ROOT.

Likewise, Eskapade uses a manner of different data structures to handle the data, such as:

  - pandas DataFrames,
  - numpy arrays,
  - Spark DataFrames

and more.

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

Contents
========

.. toctree::
   :maxdepth: 2

   introduction
   installation
   tutorials
   package_structure
   command_line_arguments
   developing

API
---

.. toctree::
   :maxdepth: 1

   code

Appendices
----------

.. toctree::
   :maxdepth: 2

   mac_os
   spark


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
