MacOS
=====

This Eskapade installation guide is written using **MacOS Sierra**.

To install Eskapade on MacOS there are basically four challenges to overcome certain versioning issues:

  * Getting an isolated Python 3.6.x environment,
  * Getting ROOT 6.10.08 to work with Python 3.6.x (ROOT conflicts with Anaconda),
  * Getting Python packages similar to those in Anaconda,
  * Getting Spark 2.1.2 set up (the homebrew version does not cooperate).

Therefore we are building a KaveToolBox for MacOS and re-use the Vagrant code.

Setup Python 3
--------------

First install a clean Python 3.6.x (same as Anaconda):

.. code-block:: bash

  env PYTHON_CONFIGURE_OPTS="--enable-framework" pyenv install -v 3.6.3

.. note::

  Python 3.6.x is incompatible with Spark 2.1.0

Then we need to create an isolated Python environment which we call 'eskapade':

.. code-block:: bash

  brew install pyenv-virtualenv
  eval "$(pyenv init -)"
  eval "$(pyenv virtualenv-init -)"
  pyenv virtualenv 3.6.3 eskapade
  pyenv activate eskapade

Installing ROOT 6
-----------------

Clone ROOT from the git repository:

.. code-block:: bash

  git clone http://root.cern.ch/git/root.git
  cd root
  git tag -l
  git checkout -b v6-10-08 v6-10-08
  mkdir ~/build
  cd ~/build

Next we compile it with some additional flags to ensure it has the desired functionality.
Note the CXX stuff (this is important):

.. code-block:: bash

  cmake -DCMAKE_INSTALL_PREFIX="../root" -Dfail-on-missing=ON -Dcxx11=ON-Dshared=ON -Dsoversion=ON \
  -Dthread=ON -Dfortran=ON -Dpython3=ON -Dcling=ON -Dx11=ON -Dssl=ON -Dxml=ON -Dfftw3=ON -Dbuiltin_fftw3=OFF \
  -Dmathmore=ON -Dminuit2=ON -Droofit=ON -Dtmva=ON -Dopengl=ON -Dgviz=ON -Dalien=OFF -Dbonjour=OFF -Dcastor=OFF \
  -Dchirp=OFF -Ddavix=OFF -Ddcache=OFF -Dfitsio=OFF -Dgfal=OFF -Dhdfs=OFF -Dkrb5=OFF -Dldap=OFF -Dmonalisa=OFF \
  -Dmysql=OFF -Dodbc=OFF -Doracle=OFF -Dpgsql=OFF -Dpythia6=OFF -Dpythia8=OFF -Dsqlite=OFF -Drfio=OFF -Dxrootd=OFF \
  -DPYTHON_EXECUTABLE="/Users/$USER/.pyenv/versions/3.6.3/bin/python" \
  -DPYTHON_INCLUDE_DIR="/Users/$USER/.pyenv/versions/3.6.3/include/python3.6m" \
  -DPYTHON_LIBRARY="/Users/$USER/.pyenv/versions/3.6.3/lib/libpython3.6m.dylib" \
  -Dbuiltin_tbb=ON
  -DCMAKE_CXX_COMPILER=/usr/bin/clang++
  ../root


.. code-block:: bash

  cmake --build . --target install -- -j4

When this is done, we need to setup the ROOTSYS environment, so we can install some additional Python packages:

.. code-block:: bash

  source ~/root/bin/thisroot.sh

And finally install some Python packages for ROOT bindings:

.. code-block:: bash

  pip install rootpy root-numpy root_pandas


Setting up Spark 2.1.2
----------------------

Now download Spark from apache, extract it, and compile it:

.. code-block:: bash

  wget  "http://archive.apache.org/dist/spark/spark-2.1.2/spark-2.1.2.tgz"
  tar -xzf "spark-2.1.2.tgz"
  cd spark-2.1.2
  mvn -DskipTests clean package

Ensure it has the py4j package:

.. code-block:: bash

  pip install py4j

Add docker containers to hosts
------------------------------

Add the following aliases to the localhost line in /etc/hosts, so it looks like::

  127.0.0.1	localhost es-service es-mongo es-proxy

This will ensure you can reach the docker containers via the port forwards from the container to the docker host
(i.e. localhost).

Cleaning the environment
------------------------

Everytime you want to have a clean Eskapade environment run the following::

  # --- setup Python
  pyenv activate eskapade

  # --- setup ROOT
  source ~/root/bin/thisroot.sh

  # --- setup Spark
  export SPARK_HOME=$HOME/spark-2.1.2
  export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
  export PYSPARK_SUBMIT_ARGS="--master local[4] --num-executors 1 --executor-cores 4 --executor-memory 4g pyspark-shell"

  # --- setup Eskapade
  cd ~/git/gitlab-nl/decision-engine
  pip install .
