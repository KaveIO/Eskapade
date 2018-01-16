============
Installation
============

Let's get Eskapade up and running! In order to make this as easy as possible, 
we provide both a Docker image and a virtual machine where everything you need is
installed and working properly. Alternatively, you can download the repository and run it on your own machine.

* See `Eskapade with Docker`_ to get started with Docker.
* See `Eskapade on a virtual machine`_ to get started with Vagrant.
* See `Eskapade on your own machine`_ for the local installation requirements.

This manual is written for Linux systems, but Eskapade also runs fine on `MacOS <mac_os.html>`_ systems.


Eskapade with Docker
--------------------

There is a Docker image available to try out Eskapade without concerns about the installation process of all necessary packages.
The instructions below show how one can use a locally checked-out version of Eskapade in combination with this Docker image.

This combination is a very convenient way of actively working with and/or developing code in Eskapade.


Installing Eskapade
___________________

Make sure you have installed the latest version of the public Eskapade repository, with the command:

.. code-block:: bash

  git clone git@github.com:KaveIO/Eskapade.git eskapade


Installing Docker
_________________

Docker is a great tool to develop and automate the deployment of applications inside software containers.
To install Docker, go `docker.com <https://www.docker.com/>`_ and follow the installation instructions.


Pulling in the Eskapade environment
___________________________________

To pull in the environment for running Eskapade (but excluding the Eskapade repository itself), type in a shell:

.. code-block:: bash

  docker pull kave/eskapade-env:0.7

Downloading this docker image can take a minute or two.


Running Eskapade with Docker
____________________________

To start up the Eskapade docker environment, with access to the Eskapade repository, do:

.. code-block:: bash

  docker run -it --name es-ktb -p 8888:8888 -v /path/to/your/local/eskapade:/opt/eskapade kave/eskapade-env:0.6 bash

This command will start up a bash shell in the docker ``kave/eskapade-env:0.7`` image, and opens port 8888.
The Eskapade setup file will be automatically sourced.

The option ``-v /path/to/your/local/eskapade:/opt/eskapade`` mounts the local directory ``/path/to/your/local/eskapade``
(containing your local Eskapade repository) under ``/opt/eskapade`` in the docker container.
You can now edit the files in this directory, either locally or in the (docker) bash shell, and any updates
to these files will be kept after exiting docker.

This combination is a great way of using and developing Eskapade code.

E.g. one can now do:

.. code-block:: bash

  cd /opt/eskapade

and run any Eskapade code. See `Tutorial section <tutorial.html>`_ for examples.


Exit the (docker) bash shell with:

.. code-block:: bash

  exit

See section `After you exit Docker`_ (right below) for cleaning up obsolete docker processes.

Consider adding a permanent alias to your local ``~/.bashrc`` or ``~/.bash_profile`` file:

.. code-block:: bash

  alias eskapade_docker='docker run -it --name es-ktb -p 8888:8888 -v /path/to/local/eskapade:/opt/eskapade kave/eskapade-env:0.7 bash'

So the next time, in a fresh shell, you can simply run the command ``eskapade_docker``.


Starting Jupyter notebook
_________________________

To run the Jupyter notebook on port 8888 from the docker environment:

.. code-block:: bash

  cd /opt/eskapade
  jupy &

And press enter twice to return to the shell prompt.

The command ``jupy &`` starts up Jupyter notebook in the background on port 8888 and pipes the output to the log file ``nohup.out``.

In your local browser then go to address::

  localhost:8888/

And you will see the familiar Jupyter environment.

E.g. you can now do ``import eskapade`` (shift-enter) to get access to the Eskapade library.

Be sure to run ``jupy &`` from a directory that is mounted in the docker container, such as ``/opt/eskapade``.
In this way any notebook(s) you create are kept after you exit the docker run.


After you exit Docker
_____________________

Every time you want to have a clean Docker environment, run the following commands::

  # --- 1. remove all exited docker processes
  docker ps -a | grep Exited | awk '{print "docker stop "$1 "; docker rm "$1}' | sh

  # --- 2. remove all failed docker image builts
  docker images | grep "<none>" | awk '{print "docker rmi "$3}' | sh

  # --- 3. remove dangling volume mounts
  docker volume ls -qf dangling=true | awk '{print "docker volume rm "$1}' | sh

To automate this, we advise you put these commands in an executable ``docker_cleanup.sh`` script.


Eskapade on a virtual machine
-----------------------------

.. include:: ../../vagrant/README.rst

Eskapade on your own machine
----------------------------

The repository is hosted on github, clone it to your machine with:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git

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

Alternatively, consider installing `KaveToolbox <http://github.com/kaveio/KaveToolbox>`_ version 3.6 or higher,
which can be found `here <http://github.com/kaveio/kavetoolbox>`_.To install the released version:

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

Eskapade Package Installation
_____________________________

Eskapade can be installed as any other Python package with ``easy_install`` or ``pip``:

.. code-block:: bash

  $ pip install /path/to/eskapade

**You are now ready to use Eskapade!**

After installation
__________________

You can now use Eskapade in Python with:

.. code-block:: python

  import eskapade

See the readme files in other parts of the repository for specific usage.


Installing Eskapade on MacOS
----------------------------

To install eskapade on MacOS, see our `MacOS appendix <mac_oS.html>`_.
