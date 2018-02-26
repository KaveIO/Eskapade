
To install Eskapade on macOS there are basically four steps:

  * Setting up Python 3.6
  * Setting up Apache Spark 2.x
  * Setting up ROOT 6.10.08
  * Setting up Eskapade

.. note::

  This installation guide is written for `macOS High Sierra <https://www.apple.com/lae/macos/high-sierra/>`_ with `Homebrew <https://brew.sh>`_, and `fish <https://fishshell.com>`_.


Setting up Python 3.6
---------------------

Homebrew provides Python 3.6 for macOS:

.. code-block:: bash

  $ brew install python3

To create an isolated Python installation use ``virtualenv``:

.. code-block:: bash

  $ virtualenv venv/eskapade --python=python3 --system-site-packages


Each time a new terminal is started, set up the virtual python environment:

.. code-block:: bash

  $ . ~/venv/eskapade/bin/activate.fish


Setting up ROOT 6
-----------------

Clone ROOT from the git repository:

.. code-block:: bash

  git clone http://root.cern.ch/git/root.git
  cd root
  git checkout -b v6-10-08 v6-10-08

Then compile it with the additional flags to ensure the desired functionality:

.. code-block:: bash

  $ mkdir ~/root_v06-10-08_p36m && cd ~/root_v06-10-08_p36m
  $ cmake -Dfftw3=ON -Dmathmore=ON -Dminuit2=ON -Droofit=ON -Dtmva=ON -Dsoversion=ON -Dthread=ON -Dpython3=ON -DPYTHON_EXECUTABLE=/usr/local/opt/python3/Frameworks/Python.framework/Versions/3.6/bin/python3.6m -DPYTHON_INCLUDE_DIR=/usr/local/opt/python3/Frameworks/Python.framework/Versions/3.6/include/python3.6m/ -DPYTHON_LIBRARY=/usr/local/opt/python3/Frameworks/Python.framework/Versions/3.6/lib/libpython3.6m.dylib $HOME/root
  $ cmake --build . -- -j7

PS: make sure all the flags are picked up correctly (for example, ``-Dfftw3`` requires ``fftw`` to be installed with Homebrew).

To setup the ROOT environment each time a new shell is started, set the following environment variables:

.. code-block:: bash

  set -xg ROOTSYS "$HOME/root_v06-10-08_p36m"
  set -xg PATH $ROOTSYS/bin $PATH
  set -xg LD_LIBRARY_PATH "$ROOTSYS/lib:$LD_LIBRARY_PATH"
  set -xg DYLD_LIBRARY_PATH "$ROOTSYS/lib:$DYLD_LIBRARY_PATH"
  set -xg LIBPATH "$ROOTSYS/lib:$LIBPATH"
  set -xg SHLIB_PATH "$ROOTSYS/lib:$SHLIB_PATH"
  set -xg PYTHONPATH "$ROOTSYS/lib:$PYTHONPATH"

Note that for bash shells this can be done by sourcing the script in ``root_v06-10-08_p36m/bin/thisroot.sh``.

Finally, install the Python packages for ROOT bindings:

.. code-block:: bash

  $ pip install rootpy==1.0.1 root-numpy=4.7.3


Setting up Apache Spark 2.x 
---------------------------

Apache Spark is provided through Homebrew:

.. code-block:: bash

  $ brew install apache-spark

The ``py4j`` package is needed to support access to Java objects from Python:

.. code-block:: bash

  $ pip install py4j==0.10.4

To set up the Spark environment each time a new terminal is started set:

.. code-block:: bash

  set -xg SPARK_HOME (brew --prefix apache-spark)/libexec
  set -xg SPARK_LOCAL_HOSTNAME "localhost"
  set -xg PYTHONPATH "$SPARK_HOME/python:$PYTHONPATH"


Setting up Eskapade
-------------------

The Eskapade source code can be obtained from git:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git eskapade


To set up the Eskapade environment (Python, Spark, ROOT) each time a new terminal is started, source a shell script (e.g. ``setup_eskapade.fish``) that contains set the environment variables as described above:

.. code-block:: bash

  # --- setup Python
  . ~/venv/eskapade/bin/activate.fish

  # --- setup ROOT
  set -xg ROOTSYS "${HOME}/root_v06-10-08_p36m"
  set -xg PATH $ROOTSYS/bin $PATH
  set -xg LD_LIBRARY_PATH "$ROOTSYS/lib:$LD_LIBRARY_PATH"
  set -xg DYLD_LIBRARY_PATH "$ROOTSYS/lib:$DYLD_LIBRARY_PATH"
  set -xg LIBPATH "$ROOTSYS/lib:$LIBPATH"
  set -xg SHLIB_PATH "$ROOTSYS/lib:$SHLIB_PATH"
  set -xg PYTHONPATH "$ROOTSYS/lib:$PYTHONPATH" 

  # --- setup Spark
  set -xg SPARK_HOME (brew --prefix apache-spark)/libexec
  set -xg SPARK_LOCAL_HOSTNAME "localhost"
  set -xg PYTHONPATH "$SPARK_HOME/python:$PYTHONPATH"

  # --- setup Eskapade
  cd /path/to/eskapade

Finally, install Eskapade (and it's dependencies) by simply running:

.. code-block:: bash

  $ pip install -e /path/to/eskapade
