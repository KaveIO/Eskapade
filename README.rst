========
Eskapade
========

* Version: 0.8
* Released: Aug 2018

Eskapade is a light-weight, python-based data analysis framework, meant for modularizing all sorts of data analysis problems into reusable analysis components.


Documentation
=============

The entire Eskapade documentation including tutorials can be found `here <http://eskapade.readthedocs.io>`_.

Release notes
=============

In version 0.8 of Eskapade (August 2018) the modules ``root-analysis`` and ``spark-analysis`` have been split off
into separate pip packages called ``Eskapade-ROOT`` and ``Eskapade-Spark``.
This make it much easier for people to try out the core functionality of Eskapade, as this only requires basic Python packages.

See `release notes <http://eskapade.readthedocs.io/en/latest/releasenotes.html>`_ for previous versions of Eskapade.


Installation
============

Eskapade requires Python 3. To get started, simply do:

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


Examples
========

To see the available Eskapade example, do:

.. code-block:: bash

  $ export TUTDIR=`pip show Eskapade | grep Location | awk '{ print $2"/eskapade/tutorials" }'`
  $ ls -l $TUTDIR/

E.g. you can now run:

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk101_helloworld.py 

For all available Eskapade example macros, please see our `tutorials section <http://eskapade.readthedocs.io/en/latest/tutorials.html>`_.


Contact and support
===================

Contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.
