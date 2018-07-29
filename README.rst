========
Eskapade
========

Version: 0.8

Released: Aug 2018

Eskapade is a light-weight, python-based data analysis framework, meant for modularizing all sorts of data analysis problems into reusable analysis components.


Release notes
=============

Version 0.8
-----------

In version 0.8 of Eskapade (August 2018) the modules ``root-analysis`` and ``spark-analysis`` have been split off
into separate packages called ``Eskapade-ROOT`` and ``Eskapade-Spark``.

This make it much easier for people to try out the core functionality of Eskapade.


Version 0.7
-----------

Version 0.7 of Eskapade (February 2018) contains several major updates:

* The Eskapade code has been made pip friendly. One can now simply do:

  .. code-block:: bash

    $ pip install Eskapade

  or check out the code from out github repository:

  .. code-block:: bash

    $ git clone git@github.com:KaveIO/Eskapade.git
    $ pip install -e Eskapade/

  where in this example the code is installed in edit mode (option -e).

  You can now use Eskapade in Python with:

  .. code-block:: python

    import eskapade

  This change has resulted in some restructuring of the python directories, making the overall structure more transparent:
  all python code, including the tutorials, now fall under the (single) ``python/`` directory.
  Additionally, thanks to the pip convention, our prior dependence on environment variables (``$ESKAPADE``)
  has now been fully stripped out of the code.
* There has been a cleanup of the core code, removing obsolete code and making it better maintainable.
  This has resulted in a (small) change in the api of the process manager, adding chains, and using the logger.
  All tutorials and example macro files have been updated accordingly.
  See the `migration section <misc.html#from-version-0-6-to-0-7>`_ of the documentation for detailed tips on migrating existing Eskapade code to version 0.7.
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
Machine learning interfaces will be included in an upcoming release.


Installation
============

Requirements
------------

Eskapade requires Python 3 and Anaconda version 4.3 (or greater), which can be found `here <https://www.continuum.io/downloads>`_.
See the Readme's in the ``docs/`` directory of the repository for specific requirements and usage.


Eskapade on your own machine
----------------------------

To get started, simply do:

.. code-block:: bash

  $ pip install Eskapade

or check out the code from out github repository:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git
  $ pip install -e Eskapade/

where in this example the code is installed in edit mode (option -e).

You can now use Eskapade in Python with:

.. code-block:: python

  import eskapade


Documentation
=============

The entire documentation including tutorials can be found `here <http://eskapade.readthedocs.io>`_.


Contact and support
===================

Contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.
