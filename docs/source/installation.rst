============
Installation
============

Let's get Eskapade up and running! In order to make this as easy as possible, 
we provide a virtual machine where everything you need is installed and working properly. 
See `Eskapade on a virtual machine`_ to get started. Alternatively, you can download 
the repository and run it on your own machine. See `Eskapade on your own machine`_ for the
requirements.

This manual is written for Linux systems, but Eskapade also runs fine on MacOS systems.

Eskapade on a virtual machine
-----------------------------

.. include:: ../../vagrant/README.rst

Eskapade on your own machine
----------------------------

The repository is hosted on github, clone it to your machine with:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git

Requirements
------------

Eskapade requires Anaconda, which can be found `here <https://www.continuum.io/downloads>`_. Eskapade was 
tested with version 4.3. 

It also uses some non-standard libraries, which can be found in `requirements.txt` in the repository. These
can be installed by doing: 

.. code-block:: bash

  $ pip install -r requirements.txt

**You are now ready to use Eskapade!**

After installation
-------------------
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

See the readme files in other parts of the repository for specific usage.
