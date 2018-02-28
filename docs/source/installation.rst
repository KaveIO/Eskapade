============
Installation
============

Let's get Eskapade up and running! In order to make this as easy as possible, 
we provide both a Docker image and a virtual machine where everything you need is
installed and working properly. Alternatively, you can download the repository and run it on your own machine.

* See `Eskapade on your own machine`_ for the local installation requirements.
* See `Eskapade with Docker`_ to get started with Docker.
* See `Eskapade on a virtual machine`_ to get started with Vagrant.

This manual is written for Linux systems, but Eskapade also runs fine on `macOS <mac_os.html>`_ systems.


Eskapade on your own machine
----------------------------

Eskapade can be installed as any other Python package with ``easy_install`` or ``pip``. To get started, simply do:

.. code-block:: bash

  $ pip install Eskapade

We have verified that this works on Linux 16.04 and MacOS based machines.

Or check out the code from out github repository:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git
  $ pip install -e Eskapade/

where the code is installed in editable mode (option -e).

You can now use Eskapade in Python with:

.. code-block:: python

  import eskapade

**Congratulations, you are now ready to use Eskapade!**

See the other parts of the documentation for specific usage.


Eskapade with Docker
--------------------

.. include:: ../../docker/README.rst


Eskapade on a virtual machine
-----------------------------

.. include:: ../../vagrant/README.rst


Requirements
____________

Eskapade requires Python 3 and some libraries, which can be found in `setup.py` at the root of the repository.

There are two optional subpackages which require external products: `root_analysis` and `spark_analysis` subpackages.

To be able to run `root_analysis`, `ROOT CERN's data analysis package <http://root.cern.ch>`_
need to be compiled with the following flags:

.. code-block:: bash

  $ -Dfftw3=ON -Dmathmore=ON -Dminuit2=ON -Droofit=ON -Dtmva=ON -Dsoversion=ON -Dthread=ON -Dpython3=ON \
  $ -DPYTHON_EXECUTABLE=path_to_python_exe -DPYTHON_INCLUDE_DIR=path_to_python_include -DPYTHON_LIBRARY=path_to_python_lib

`spark_analysis` requires `Apache Spark <https://spark.apache.org>`_ version 2.1.1 or higher.

Eskapade can be installed as any other Python package with ``easy_install`` or ``pip``:

.. code-block:: bash

  $ pip install /path/to/eskapade

Alternatively, consider installing `KaveToolbox <http://github.com/kaveio/KaveToolbox>`_ version 3.6 or higher.
To install the released version:

.. code-block:: bash

  $ yum -y install wget curl tar zip unzip gzip python
  $ wget http://repos:kaverepos@repos.kave.io/noarch/KaveToolbox/3.6-Beta/kavetoolbox-installer-3.6-Beta.sh
  $ sudo bash kavetoolbox-installer-3.6-Beta.sh [--quiet]

(--quiet is for a quieter install, remove the brackets!)

If anaconda is already installed in your machine, consider creating a conda virtual environment with Python 3.6 to install
all the requirements and Eskapade itself to avoid collisions:

.. code-block:: bash

  $ conda create -n eskapade_env36 python=3.6 anaconda

Then you can activate it as follows:

.. code-block:: bash

  $ source activate eskapade_env36

More information about conda virtual environments can be found
`here <https://conda.io/docs/user-guide/tasks/manage-environments.html>`_


Installing Eskapade on macOS
----------------------------

To install eskapade on macOS, see our `macOS appendix <mac_os.html>`_.
