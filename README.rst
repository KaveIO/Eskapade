===========================
Eskapade: Modular Analytics
===========================

* Version: 0.8.5
* Released: Nov 2018
* Web page: http://eskapade.kave.io
* Repository: https://github.com/kaveio/eskapade


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

  $ eskapade_run $TUTDIR/esk101_helloworld.py 

For all available Eskapade example macros, please see our `tutorials section <http://eskapade.readthedocs.io/en/latest/tutorials.html>`_.


Release notes
=============

The Eskapade patch release v0.9.0 and corresponding docker containers fix two issues:

* The ``matplotlib`` backend is no longer set to batchmode when running Eskapade in a jupyter notebook.
  By default, batch mode is only turned on when no DISPLAY environment variable is set, and when not running in a notebook;
  the batch-mode flag can also be controlled with the command line option ``–batch-mode``.

* The Eskapade docker containers contain the latest working versions of ``Eskapade``, ``Eskapade-ROOT``, and ``Eskapade-Spark``. Type:

  .. code-block:: bash

    $ docker pull kave/eskapade-usr:latest

  to pull it in.


See `release notes <http://eskapade.readthedocs.io/en/latest/releasenotes.html>`_ for previous versions of Eskapade.


Contact and support
===================

* Issues & Ideas: https://github.com/kaveio/eskapade/issues
* Q&A Support: contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.
