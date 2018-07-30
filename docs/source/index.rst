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

Eskapade is a light-weight, python-based data analysis framework, meant for modularizing
all sorts of data analysis problems into reusable analysis components.

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

For example, Eskapade has been used as a self-learning framework for typical machine learning problems.
Trained algorithms can predict real-time or batch data, these models can be evaluated over time,
and Eskapade can bookkeep and retrain their algorithms.


Check it out
============

The Eskapade code is pip friendly. One can now simply do:

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

**Congratulations, you are now ready to use Eskapade!**

For all available examples, please see the `tutorials section <tutorials.html>`_.


Contents
========

.. toctree::
   :maxdepth: 2

   introduction
   installation
   tutorials
   command_line_arguments
   package_structure
   releasenotes
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

   misc


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
