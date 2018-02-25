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

Eskapade is analysis-library agnostic. It is used to set up typical data analysis problems from multiple packages, e.g.:
scikit-learn, Spark MLlib, and ROOT. Likewise, Eskapade can use a manner of different data structures to handle
data, such as: pandas DataFrames, numpy arrays, Spark DataFrames/RDDs, and more.


Release notes
=============

Version 0.7
-----------

Version 0.7 of Eskapade (February 2018) contains several major updates:

* The Eskapade code has been made pip friendly. Having checked out the code from git, one can now simply do:

  .. code:: bash

    $ pip install <ESKAPADE>

  where ``<ESKAPADE>`` specifies the path of the Eskapade source code.
  This change has resulted in some restructuring of the python directories, making the overall structure more transparent:
  all python code, including the tutorials, now fall under the (single) ``python/`` directory.
  Additionally, thanks to the pip convention, our prior dependence on environment variables (``$ESKAPADE``)
  has now been fully stripped out of the code.
* There has been a cleanup of the core code, removing obsolete code and making it better maintainable.
  This has resulted in a (small) change in the api of the process manager, adding chains, and using the logger.
  All tutorials and example macro files have been updated accordingly.
  See the `migration section <migration.html#from-version-0-6-to-0-7>`_ for detailed tips on migrating existing Eskapade code to version 0.7.
* All eskapade commands now start with the prefix ``eskapade_``. All tutorials have been updated accordingly. We have the commands:

  - ``eskapade_bootstrap``, for creating a new Eskapade analysis project. See this new `tutorial <tutorials.html#tutorial-4-creating-a-new-analysis-project>`_ for all the details.
  - ``eskapade_run``, for running the Eskapade macros.
  - ``eskapade_trail``, for running the Eskapade unit and integration tests.
  - ``eskapade_generate_link``, ``eskapade_generate_macro``, ``eskapade_generate_notebook``, for generating a new link, macro, or Jupyter notebook respectively.



Version 0.6
-----------

The primary feature of version 0.6 (August 2017) is the inclusion of Spark, but this version
also includes several other new features and analyses.

We include multiple Spark links and 10 Spark examples on:

* The configuration of Spark, reading, writing and converting Spark dataframes, applying functions and queries to dataframes,
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
Machine learning interfaces will be included in an upcoming release.

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
   migration_tips

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
