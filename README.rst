===========================
Eskapade: Modular Analytics
===========================

* Version: 1.0.0
* Released: February 2018
* Web page: http://eskapade.kave.io
* Repository: https://github.com/kaveio/eskapade
* Docker: https://github.com/kaveio/eskapade-environment

Eskapade is a light-weight, python-based data analysis framework, meant for developing and modularizing all sorts of
data analysis problems into reusable analysis components.

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


Documentation
=============

The entire Eskapade documentation including tutorials can be found `here <http://eskapade.readthedocs.io/en/latest>`_.


Check it out
============

Eskapade requires Python 3 and is pip friendly. To get started, simply do:

.. code-block:: bash

  $ pip install Eskapade

or check out the code from out github repository:

.. code-block:: bash

  $ git clone https://github.com/KaveIO/Eskapade.git
  $ pip install -e Eskapade/

where in this example the code is installed in edit mode (option -e).

You can now use Eskapade in Python with:

.. code-block:: python

  import eskapade

**Congratulations, you are now ready to use Eskapade!**


Quick run
=========

To see the available Eskapade example, do:

.. code-block:: bash

  $ export TUTDIR=`pip show Eskapade | grep Location | awk '{ print $2"/eskapade/tutorials" }'`
  $ ls -l $TUTDIR/

E.g. you can now run:

.. code-block:: bash

  $ eskapade_run $TUTDIR/tutorial_1.py

For all available Eskapade example macros, please see our `tutorials section <http://eskapade.readthedocs.io/en/latest/tutorials.html>`_.


Release notes
=============

The Eskapade patch release v0.9.0 and corresponding docker images have the following features:

* The core functionality of Eskapade, namely: the ``Link``, ``Chain``, ``process_manager``, ``DataStore``, ``ConfigObject`` and corresponding tutorials,
  have been split off from the growing (analysis) Eskapade repository, into the new package ``Eskapade-Core``.
  `Eskapade-Core <http://eskapade-core.readthedocs.io>`_ is a very light-weight Python3 package.
* A new module ``data_mimic`` has been add to Eskapade, including tutorials, meant for resimulating existing datasets. 
* We have added ``feather`` i/o functionality for reading and writeng dataframes.
* The logger has been fixed, it is now possible to set the log-level of loggers again.
* The Eskapade docker files have been taken out of the Eskapade repository to avoid version conflicts, into the new git repo ``Eskapade-Environment``.
* The Eskapade docker image ``eskapade-usr`` contain the latest working versions of
  ``Eskapade``, ``Eskapade-Core``, ``Eskapade-ROOT``, and ``Eskapade-Spark``. Type:

  .. code-block:: bash

    $ docker pull kave/eskapade-usr:latest

  to pull it in.

See `release notes <http://eskapade.readthedocs.io/en/latest/releasenotes.html>`_ for previous versions of Eskapade.


Contact and support
===================

* Issues & Ideas: https://github.com/kaveio/eskapade/issues
* Q&A Support: contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.
