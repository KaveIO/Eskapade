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

  $ git clone https://github.com/KaveIO/Eskapade.git
  $ pip install -e Eskapade/

where the code is installed in editable mode (option -e).

You can now use Eskapade in Python with:

.. code-block:: python

  import eskapade

**Congratulations, you are now ready to use Eskapade!**

See the other parts of the documentation for specific usage.


Requirements
____________

Eskapade requires Python 3 and some libraries, which can be found in `setup.py` at the root of the repository.

Eskapade can be installed as any other Python package with ``easy_install`` or ``pip``:

.. code-block:: bash

  $ pip install /path/to/eskapade

If anaconda is already installed in your machine, consider creating a conda virtual environment with Python 3.6 to install
all the requirements and Eskapade itself to avoid collisions:

.. code-block:: bash

  $ conda create -n eskapade_env36 python=3.6 anaconda

Then you can activate it as follows:

.. code-block:: bash

  $ source activate eskapade_env36

More information about conda virtual environments can be found
`here <https://conda.io/docs/user-guide/tasks/manage-environments.html>`_



Eskapade with Docker
--------------------

Type:

  .. code-block:: bash

    $ docker pull kave/eskapade-usr:latest

to pull in the Eskapade image from dockerhub.

For more details see the Eskapade repo with the `docker configurations <https://github.com/KaveIO/Eskapade-Environment/>`_.


Eskapade on a virtual machine
-----------------------------

For detailed instruction on how to set up a vagrant box with Eskapade, go to the Eskapade repo with the `vagrant box <https://github.com/KaveIO/Eskapade-Environment/>`_.



Installing Eskapade on macOS
----------------------------

To install eskapade on macOS, see our `macOS appendix <mac_os.html>`_.
