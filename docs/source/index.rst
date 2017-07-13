.. Eskapade documentation master file, created by
   sphinx-quickstart on Thu Jul  7 14:25:54 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

==================================
Eskapade: Modular Analytics
==================================

.. module:: eskapade

**Version**: |version|

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

Release 0.6 (July 2017) includes Spark. 

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
   tutorial
   tutorial_jupyter
   tutorial_roofit
   package_structure
   command_line_arguments
   contributing

Appendices
----------

.. toctree::
   :maxdepth: 2

   mac_os

API
---

.. toctree::
   :maxdepth: 1

   code


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
