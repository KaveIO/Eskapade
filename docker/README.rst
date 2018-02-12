Introduction
------------

Consistent environments for Eskapade development and use can be created with docker containers. Containers are created and managed by `Docker <https://www.docker.com/>`_. A containers contains a pre-installed Eskapade setup and runs out-of-the-box.  It is possible to mount your customised Eskapade code for development inside the container. By default, Eskapade code is executed as ``root`` user in the container. It is possible, however, to run with reduced user privileges inside containers through user mapping with the Docker host, as decribed in the last section.


Required software
-----------------

Docker installation instructions can be found here: `<https://docs.docker.com/install/>`_.

Eskapade source code is optional (for development purposes) and can be obtained from `<https://github.com/KaveIO/Eskapade>`_.


Getting Eskapade docker images
------------------------------

DockerHub
:::::::::

The official Eskapade docker image is provided on `DockerHub <https://hub.docker.com/r/kave/eskapade-env/>`_.

.. code:: bash

  $  docker pull kave/eskapade-env:0.7 

This will download the ``kave/eskapade-env:0.7`` image locally.

Building from scratch
:::::::::::::::::::::

To build the docker image from scratch using the Eskapade source code, do:

.. code:: bash

  $  cd eskapade/docker/eskapade-env && sh create_docker.sh

This will produce the ``kave/eskapade-env:0.7`` image.


Spinning up docker containers
-----------------------------

Out-of-the-box
::::::::::::::

From this image, containers with the Eskapade environment set up, can be run out-of-the-box:

.. code:: bash

  $  docker run -p 8888:8888 -it kave/eskapade-env:0.7 

Where port 8888 is forwarded to the docker host to make jupyter notebook available.

Mounting source code
::::::::::::::::::::

.. code:: bash

  $  docker run -v <ESKAPADE>:/opt/eskapade -p 8888:8888 -it kave/eskapade-env:0.7 

Where ``<ESKAPADE>`` specifies the path of the Eskapade source code on the docker host, and where ``/opt/eskapade`` is the location of the Eskapade source code inside the container.

NOTE: in case you mount a clean installation of the Eskapade source code, you have to (re-)build libraries by executing:

.. code:: bash

  $ pip install -e /opt/eskapade


Running as non-root user
------------------------

For increased security in a production environment, it is recommended to run Eskapade code inside the container as non-root. The ``Dockerfile`` in the ``eskapade-user`` directory provides an additional user-mapping layer to the ``eskapade-env`` image: it creates a ``esdev`` user that has its own virtual Python environment with Eskapade installed. The mapping of user id's between Docker host and container ensure that proper permissions are propogated when writing/reading to the mounted volume with Eskapade code.

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
variables are used to dynamically map user & group id's between Docker host and container ensuring proper read/write permissions.


Remapping the user id permanently
:::::::::::::::::::::::::::::::::

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

Containers with the user-specific Eskapade environment setup, can be run out-of-the-box, and with your own mounted (customised) source code using:

.. code:: bash

  $  docker run -e HOST_USER_ID=$(id -u) -e HOST_USER_GID=$(id -g) -v <ESKAPADE>:/home/esdev/eskapade -p 8888:8888 -it kave/eskapade-usr:0.7

Where ``<ESKAPADE>`` specifies the path of the Eskapade source code.

NOTE: in case you mount a clean installation of the Eskapade source code, you have to (re-)build the libraries by executing:

.. code:: bash

  $ pip install -e /home/esdev/eskapade
