Setting up Eskapade environment for MacOS
=========================================

This installation guide is written using **MacOS Sierra**.

To install eskapade on MacOS there are basically 3 challenges to overcome certain versioning issues:
  * Getting an isolated Python 3.5.2 environment,
  * Getting ROOT 6.08/06 to work with Python 3.5.2 (ROOT conflicts with Anaconda),
  * Getting Python packages similar to those in Anaconda,
  * Getting SPARK 2.1.0 set up (the homebrew version does not cooperate).

Therefore we are building a KaveToolBox for MacOS and re-use the Vagrant code.

Setup Python 3
--------------

First install a clean Python 3.5.2 (same as Anaconda):

.. code-block:: bash

  env PYTHON_CONFIGURE_OPTS="--enable-framework" pyenv install -v 3.5.2

.. note::

  Using homebrew will give Python 3.6.x which is incompatible with Spark 2.1.0

Then we need to create an isolated Python environment which we call 'eskapade':

.. code-block:: bash

  brew install virtualenv
  pyenv-sh-virtualenvwrapper
  source /usr/local/bin/virtualenvwrapper.sh
  mkvirtualenv -p .pyenv/versions/3.5.2/bin/python eskapade
  workon eskapade

Installing ROOT6
----------------

Clone ROOT from the git repository::

  git clone http://root.cern.ch/git/root.git
  cd root
  git tag -l
  git checkout -b v6-08-06 v6-08-06
  mkdir ~/root
  cd ~/root

Next, apply some patches to make it work properly with Python 3.5. Luckily that was already sorted
out by the Eskapade team for the Vagrant/Docker installation:

.. code-block:: bash

  for patchfile in $(ls ${ESKAPADE}/vagrant/dev/root/patches/\*.patch); do \
    patch -p1 -i "${patchfile}" \
  done

Next we compile it with some additional flags to ensure it has the desired functionality.
Note the CXX stuff (this is important):

.. code-block:: bash

  cmake /Users/$USER/git/root/. \
  -DPYTHON_EXECUTABLE=/Users/$USER/.pyenv/versions/3.5.2/bin/python \
  -DPYTHON_INCLUDE_DIR=/Users/$USER/.pyenv/versions/3.5.2/include/python3.5m \
  -DPYTHON_LIBRARY=/Users/$USER/.pyenv/versions/3.5.2/lib/libpython3.5m.dylib \
  -Dfail-on-missing=ON \
  -Dcxx14=ON -Droot7=ON -Dshared=ON -Dsoversion=ON -Dthread=ON -Dfortran=ON -Dpython=ON -Dcling=ON -Dx11=ON -Dssl=ON \
  -Dxml=ON -Dfftw3=ON -Dbuiltin_fftw3=OFF -Dmathmore=ON -Dminuit2=ON -Droofit=ON -Dtmva=ON -Dopengl=ON -Dgviz=ON \
  -Dalien=OFF -Dbonjour=OFF -Dcastor=OFF -Dchirp=OFF -Ddavix=OFF -Ddcache=OFF -Dfitsio=OFF -Dgfal=OFF -Dhdfs=OFF \
  -Dkrb5=OFF -Dldap=OFF -Dmonalisa=OFF -Dmysql=OFF -Dodbc=OFF -Doracle=OFF -Dpgsql=OFF -Dpythia6=OFF -Dpythia8=OFF \
  -Dsqlite=OFF -Drfio=OFF -Dxrootd=OFF \
  cmake --build . -- -j4

When this is done, we need to setup the ROOTSYS environment, so we can install some additional Python packages:

.. code-block:: bash

  source ~/root/bin/thisroot.sh

And finally install some Python packages for ROOT bindings:

.. code-block:: bash

  pip install rootpy root-numpy root_pandas


Getting Python packages
-----------------------

Then we want to install the same packages as there are in a proper KaveToolBox environment to avoid version conflicts
and random issues. We can use a requirements file, obtained through a ``pip freeze`` on the Vagrant/Docker installation
that works.

To install the requirements run:

.. code-block:: bash

  pip install -r requirements.txt

Input for the requirements.txt file is the following::

  alabaster==0.7.8
  amqp==2.1.4
  appdirs==1.4.3
  appnope==0.1.0
  argcomplete==1.0.0
  arrow==0.10.0
  astroid==1.4.9
  astropy==1.2.1
  autopep8==1.3.1
  Babel==2.3.3
  backports.shutil-get-terminal-size==1.0.0
  beautifulsoup4==4.4.1
  billiard==3.5.0.2
  binaryornot==0.4.0
  bitarray==0.8.1
  blaze==0.10.1
  bokeh==0.12.0
  boto==2.40.0
  Bottleneck==1.1.0
  branca==0.2.0
  bson==0.4.6
  cairocffi==0.8.0
  CairoSVG==2.0.2
  celery==4.0.2
  cffi==1.6.0
  chardet==2.3.0
  cheroot==5.4.0
  CherryPy==10.2.1
  chest==0.2.3
  click==6.6
  cloudpickle==0.2.1
  clyent==1.2.1
  colorama==0.3.7
  configobj==5.0.6
  contextlib2==0.5.3
  cookiecutter==1.5.1
  coverage==4.3.4
  cryptography==1.4
  cssselect==1.0.1
  cycler==0.10.0
  Cython==0.24
  cytoolz==0.8.0
  dask==0.10.0
  datashape==0.5.2
  decorator==4.0.10
  Delorean==0.6.0
  descartes==1.1.0
  dill==0.2.5
  Django==1.10.5
  django-filter==1.0.2
  djangorestframework==3.6.2
  docutils==0.12
  entrypoints==0.2.2
  et-xmlfile==1.0.1
  fastcache==1.0.2
  Flask==0.11.1
  Flask-Cors==2.1.2
  folium==0.3.0
  future==0.16.0
  gevent==1.1.1
  gnureadline==6.3.3
  greenlet==0.4.10
  h5py==2.6.0
  HeapDict==1.0.0
  humanize==0.5.1
  idna==2.1
  imagesize==0.7.1
  ipykernel==4.3.1
  ipython==4.2.0
  ipython-genutils==0.1.0
  ipywidgets==4.1.1
  isort==4.2.5
  itsdangerous==0.24
  JayDeBeApi==1.1.1
  jdcal==1.2
  jedi==0.9.0
  Jinja2==2.8
  jinja2-time==0.2.0
  JPype1==0.6.2
  jsonschema==2.5.1
  jupyter==1.0.0
  jupyter-client==4.3.0
  jupyter-console==4.1.1
  jupyter-core==4.1.0
  kombu==4.0.2
  lazy-object-proxy==1.2.2
  locket==0.2.0
  lxml==3.6.0
  Markdown==2.6.8
  MarkupSafe==0.23
  matplotlib==1.5.1
  mccabe==0.6.1
  mistune==0.7.2
  mock==2.0.0
  modernize==0.5
  mpld3==0.3
  mpmath==0.19
  multipledispatch==0.4.8
  names==0.3.0
  nbconvert==4.2.0
  nbformat==4.0.1
  nbpresent==3.0.0
  networkx==1.11
  nltk==3.2.1
  nose==1.3.7
  notebook==4.2.1
  numpy==1.11.1
  odo==0.5.0
  packaging==16.8
  pandas==0.18.1
  patsy==0.4.1
  pbr==2.0.0
  pexpect==4.0.1
  pickleshare==0.7.2
  Pillow==3.2.0
  portend==1.8
  poyo==0.4.1
  prompt-toolkit==1.0.14
  psutil==4.3.0
  ptyprocess==0.5.1
  py4j==0.10.4
  pyasn1==0.1.9
  pycodestyle==2.3.1
  pycparser==2.14
  Pygments==2.1.3
  pymongo==3.4.0
  pyparsing==2.1.4
  python-dateutil==2.5.3
  pytz==2016.4
  PyYAML==3.11
  pyzmq==15.2.0
  qtconsole==4.2.1
  requests==2.13.0
  root-numpy==4.7.2
  root-pandas==0.1.1
  rootpy==0.9.0
  scikit-learn==0.18.1
  scipy==0.19.0
  seaborn==0.7.1
  simplegeneric==0.8.1
  six==1.10.0
  sklearn==0.0
  snowballstemmer==1.2.1
  sortedcontainers==1.5.7
  Sphinx==1.5.3
  sphinx-rtd-theme==0.2.4
  SQLAlchemy==1.0.13
  statsmodels==0.8.0
  tabulate==0.7.7
  tempora==1.6.1
  terminado==0.6
  tinycss==0.4
  toolz==0.8.0
  tornado==4.3
  traitlets==4.2.1
  tzlocal==1.3
  vine==1.1.3
  wcwidth==0.1.7
  Werkzeug==0.11.10
  whichcraft==0.4.0
  wrapt==1.10.10


Setting up SPARK 2.1.0
----------------------

Now download SPARK from apache, extract it, and compile it:

.. code-block:: bash

  wget  "http://archive.apache.org/dist/spark/spark-2.1.0/spark-2.1.0.tgz"
  tar -xzf "spark-2.1.0.tgz"
  cd spark-2.1.0
  mvn -DskipTests clean package

Ensure it has the py4j package:

.. code-block:: bash

  pip install py4j

Add docker containers to hosts
------------------------------

Add the following aliases to the localhost line in /etc/hosts, so it looks like::

  127.0.0.1	localhost es-service es-mongo es-jboss es-proxy

This will ensure you can reach the docker containers via the port forwards from the container to the docker host
(i.e. localhost).

Cleaning the environment
------------------------

Everytime you want to have a clean Eskapade environment run the following::

  # --- setup PYTHON
  source /usr/local/bin/virtualenvwrapper.sh
  workon eskapade

  # --- setup ROOT
  source ~/root/bin/thisroot.sh

  # --- setup SPARK
  export SPARK_HOME=$HOME/spark-2.1.0
  export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
  export PYSPARK_SUBMIT_ARGS="--master local[4] --num-executors 1 --executor-cores 4 --executor-memory 4g pyspark-shell"

  # --- setup Eskapade
  cd ~/git/gitlab-nl/decision-engine
  source ./eskapade/setup.sh
  source ./analyticsengine/setup.sh


To automate this you can put it in a 'setup_eskapade.sh' script, but at the time of writing we have not done this yet.