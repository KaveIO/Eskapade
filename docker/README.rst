Consistent environments for Eskapade development and use can be created with docker containers. Containers are created and managed by `Docker <https://www.docker.com/>`_. An Eskapade container contains a pre-installed Eskapade setup and runs out-of-the-box.  It is possible to mount your customised Eskapade code for development inside the container.

The instructions below show how one can use a locally checked-out version of Eskapade in combination with this Docker image. This combination is a very convenient way of actively working with and/or developing code in Eskapade.

By default, Eskapade code is executed as ``root`` user in the container. It is possible, however, to run with reduced user privileges inside containers through user
mapping with the Docker host, as decribed in the last section.


Required software
_________________

Docker installation instructions can be found here: `<https://docs.docker.com/install/>`_.

Eskapade source code is optional (for development purposes), you can check it out locally with the command:

.. code-block:: bash

  git clone git@github.com:KaveIO/Eskapade.git eskapade



Getting Eskapade docker images
______________________________

From DockerHub
::::::::::::::

The official Eskapade docker image is provided on `DockerHub <https://hub.docker.com/r/kave/eskapade-env/>`_.

.. code:: bash

  $  docker pull kave/eskapade-env:0.7 

This will download the ``kave/eskapade-env:0.7`` image locally.
Downloading this docker image can take a minute or two.



Building from scratch
:::::::::::::::::::::

To build the docker image from scratch using the Eskapade source code, do:

.. code:: bash

  $  cd eskapade/docker/eskapade-env && sh create_docker.sh

This will produce the ``kave/eskapade-env:0.7`` image locally.


Spinning up docker containers
_____________________________

Out-of-the-box
::::::::::::::

From this image, containers with the Eskapade environment set up, can be run out-of-the-box:

.. code:: bash

  $  docker run -p 8888:8888 -it kave/eskapade-env:0.7 

Where port 8888 is forwarded to the docker host to make Jupyter notebook available (below).

E.g. one can now do:

.. code-block:: bash

  cd /opt/eskapade
  eskapade_run --help

and run any Eskapade code. See the Tutorial section for examples.

Exit the (docker) bash shell with:

.. code-block:: bash

  exit

See section `After you exit Docker`_ (below) for cleaning up obsolete docker processes.


Mounting source code
::::::::::::::::::::

.. code:: bash

  $  docker run -v <ESKAPADE>:/opt/eskapade -p 8888:8888 -it kave/eskapade-env:0.7 

Where ``<ESKAPADE>`` specifies the path of the Eskapade source code on the docker host, and where ``/opt/eskapade`` is the location of the Eskapade source code inside the container.

NOTE: in case you mount a clean installation of the Eskapade source code, you have to (re-)build the libraries by executing:

.. code:: bash

  $ pip install -e /opt/eskapade


Running as non-root user
________________________

For increased security in a production environment, it is recommended to run Eskapade code inside the container as non-root user. The ``Dockerfile`` in the ``eskapade-user`` directory provides an additional user-mapping layer to the ``eskapade-env`` image: it creates a ``esdev`` user that has its own virtual Python environment with Eskapade installed. The mapping of user id's between Docker host and container ensure that proper permissions are propogated when writing/reading to the mounted volume with Eskapade code.

To obtain a centrally produced Eskapade image, use:

.. code:: bash

  $ docker pull kave/eskapade-usr:0.7

Or build the Eskapade docker image with ``esdev`` user installation, from scratch:

.. code:: bash

  $  cd docker/eskapade-usr && docker build -t kave/eskapade-usr:0.7 .

This will produce the ``kave/eskapade-usr:0.7`` image.

From this image, containers with the Eskapade environment set up, can be run out-of-the-box:

.. code:: bash

  $ docker run -e HOST_USER_ID=$(id -u) -e HOST_USER_GID=$(id -g) -p 8888:8888 -it kave/eskapade-usr:0.7

The first time you run this command it will likely take some time. The ``HOST_USER_ID`` and ``HOST_USER_GID`` environment
variables are used to dynamically map user- and group id's between the host and Docker container, ensuring proper read/write permissions.


Remapping the user id
:::::::::::::::::::::

To prevent the remapping of user and group id from happening the next time you boot up the image, open another shell:

.. code:: bash

  $ docker ps

Copy the top CONTAINER-ID string, matching the running instance of the ``kave/eskapade-usr:0.7`` image, and then paste it:

.. code:: bash

  $ docker commit CONTAINER-ID kave/eskapade-usr:0.7

Next time when you run:

.. code:: bash

  $ docker run -e HOST_USER_ID=$(id -u) -e HOST_USER_GID=$(id -g) -p 8888:8888 -it kave/eskapade-usr:0.7

the remapping of user and group id should no longer happen.


Mounting source code
::::::::::::::::::::

Containers with the user-specific Eskapade environment setup can be run out-of-the-box, and with your own mounted (customised) source code, using:

.. code:: bash

  $  docker run -e HOST_USER_ID=$(id -u) -e HOST_USER_GID=$(id -g) -v <ESKAPADE>:/home/esdev/eskapade -p 8888:8888 -it kave/eskapade-usr:0.7

Where ``<ESKAPADE>`` specifies the path of the Eskapade source code.

NOTE: in case you mount a clean installation of the Eskapade source code, you have to (re-)build the libraries by executing:

.. code:: bash

  $ pip install -e /home/esdev/eskapade

This combination is a great way of using and developing Eskapade code.

Consider adding a permanent alias to your local ``~/.bashrc`` or ``~/.bash_profile`` file:

.. code-block:: bash

  alias eskapade_docker='docker run -e HOST_USER_ID=$(id -u) -e HOST_USER_GID=$(id -g) -v <ESKAPADE>:/home/esdev/eskapade -p 8888:8888 -it kave/eskapade-usr:0.7'

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
In case you get asked for a password, take a look at the tail end of the file ``nohup.out``, where you will see the exact url address that you need to go to.

E.g. you can now do ``import eskapade`` (shift-enter) to get access to the Eskapade library.

Be sure to run ``jupy &`` from a directory that is mounted in the docker container, such as ``/opt/eskapade``.
In this way any notebook(s) you create are kept after you exit the docker run.


After you exit Docker
_____________________

Every time you want to have a clean Docker environment, run the following commands:

.. code-block:: bash

  # --- 1. remove all exited docker processes
  docker ps -a | grep Exited | awk '{print "docker stop "$1 "; docker rm "$1}' | sh

  # --- 2. remove all failed docker image builts
  docker images | grep "<none>" | awk '{print "docker rmi "$3}' | sh

  # --- 3. remove dangling volume mounts
  docker volume ls -qf dangling=true | awk '{print "docker volume rm "$1}' | sh

To automate this, we advise you put these commands in an executable ``docker_cleanup.sh`` script.
