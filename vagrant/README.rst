
Consistent environments for Eskapade development and use are created with virtual machines.  Machines are created and
managed by `Vagrant <https://www.vagrantup.com/>`_, with `VirtualBox <https://www.virtualbox.org/>`_ as a provider.


Getting started
_______________

Required software
:::::::::::::::::

To build and run Eskapade boxes, Vagrant and VirtualBox are required. Vagrant can be downloaded at
https://www.vagrantup.com/downloads.html. For Ubuntu (and other `Debian <https://www.debian.org/>`_-based systems),
download the Vagrant Debian package, move to where you want to install this and run this command to install: 

.. code:: bash

  dpkg -i vagrant_<VERSION>_<ARCHITECTURE>.deb

where the version and architecture are in the file name.

VirtualBox can be downloaded and installed by following the instructions on
https://www.virtualbox.org/wiki/Downloads.
Make sure you install the latest version from the VirtualBox repository instead of an older version from the
repositories of your system.

Repository
:::::::::::

The repository, which contains the code to build your virtual environment, is hosted on github. 
Clone it to your machine with:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade.git

All code below is executed in the root of this repository, unless otherwise stated. 

Virtual environment
::::::::::::::::::::

The environment box contains `Ubuntu <https://www.ubuntu.com/>`_ and the
`KAVE Toolbox <https://github.com/KaveIO/AmbariKave/wiki/Detailed-Guides#kavetoolbox>`_, including `Anaconda
<https://www.continuum.io/>`_, `Spark <https://spark.apache.org/>`_, and `ROOT <https://root.cern.ch/>`_.  

You can either build this box yourself, or download the image.
In order to build it yourself, run the following commands.
It might take a while to build (up to two hours!)

.. code:: bash

  cd vagrant/dev
  vagrant up

Alternatively, in order to download and import its image, do:

.. code:: bash

  cd vagrant/dev_image
  vagrant box add eskapade-dev.json
  vagrant up

Eskapade users and developers log in as the user ``esdev`` on ``localhost``, by default on port 2222:

.. code:: bash

  ssh -p 2222 esdev@localhost 

This user can log in with the password ``esdev``. 

By default  the local directory ``/path/to/your/local/eskapade`` is mounted
(containing your local Eskapade repository) under ``/opt/eskapade`` in your vagrant machine.
You can now edit the files in this directory, either locally or in the (vagrant) bash shell, and any updates
to these files will be kept after exiting the virtual machine.

**You are now ready to use Eskapade!** 

The next time...
________________

Simply do:

.. code:: bash

  cd vagrant/dev
  vagrant up

or:

.. code:: bash

  cd vagrant/dev_image
  vagrant up

depending on whether you built vagrant yourself or downloaded the image.

Then you can access it via ssh (password ``esdev``):

.. code:: bash

  ssh -p 2222 esdev@localhost 
 

Easy log-in
___________

To make logging in easier, the key pair ``vagrant/dev/ssh/esdev_id_rsa.pub``, ``vagrant/dev/ssh/esdev_id_rsa`` can be used,
and an example SSH configuration is provided in ``vagrant/dev/ssh/config``.  Put these files in your ``~/.ssh/``:

.. code:: bash

  cp vagrant/dev/ssh/* ~/.ssh/

You can then log in using the command:

.. code:: bash

  ssh esdevbox


Vagrant boxes
_____________

Boxes are built and started with the command ``vagrant up`` in the directory of the ``Vagrantfile`` describing the box.
A box can be restarted by executing ``vagrant reload``.  The virtual machines are administered by the ``vagrant`` user,
which logs in by running ``vagrant ssh`` in the directory of the ``Vagrantfile``. The ``vagrant`` user has root access
to the system by password-less ``sudo``.


Starting Jupyter notebook
_________________________

To run the Jupyter notebook on port 8888 from the vagrant machine:

.. code-block:: bash

  cd /opt/eskapade
  jupy &

And press enter twice to return to the shell prompt.

The command ``jupy &`` starts up Jupyter notebook in the background on port 8888 and pipes the output to the log file ``nohup.out``.

In your local browser then go to address:

  localhost:8888/

And you will see the familiar Jupyter environment.

E.g. you can now do ``import eskapade`` (shift-enter) to get access to the Eskapade library.

Be sure to run ``jupy &`` from a directory that is mounted in the vagrant machine, such as ``/opt/eskapade``.
In this way any notebook(s) you create are kept after you exit the docker run.