Virtual machines
================

Consistent environments for Eskapade development and use are created with virtual machines.  Machines are created and
managed by `Vagrant <https://www.vagrantup.com/>`_, with `VirtualBox <https://www.virtualbox.org/>`_ as a provider.

Setup
-----

To build and run Eskapade boxes, Vagrant and VirtualBox are required.  Vagrant can be downloaded at
https://www.vagrantup.com/downloads.html.  For Ubuntu (and other `Debian <https://www.debian.org/>`_-based systems),
download the Vagrant Debian package and install by running

.. code:: bash

  dpkg -i vagrant_<VERSION>_<ARCHITECTURE>.deb

VirtualBox can be downloaded and installed by following the instructions on
https://www.virtualbox.org/wiki/Downloads.
Make sure you install the latest version from the VirtualBox repository instead of an older version from the
repositories of your system.

Boxes are built and started with the command ``vagrant up`` in the directory of the ``Vagrantfile`` describing the box.
A box can be restarted by executing ``vagrant reload``.  The virtual machines are administered by the ``vagrant`` user,
which logs in by running ``vagrant ssh`` in the directory of the ``Vagrantfile``. The ``vagrant`` user has root access
to the system by password-less ``sudo``.

Development box
---------------

An environment for standard Eskapade development is provided by a box with `Ubuntu <https://www.ubuntu.com/>`_ and the
`KAVE Toolbox <https://github.com/KaveIO/AmbariKave/wiki/Detailed-Guides#kavetoolbox>`_, including `Anaconda
<https://www.continuum.io/>`_, `Spark <https://spark.apache.org/>`_, and `ROOT <https://root.cern.ch/>`_.  This box is
built by running

.. code:: bash

  cd vagrant/dev
  vagrant up

Instead of building the development box, it can also be created by downloading and importing its image:

.. code:: bash

  cd vagrant/dev_image
  vagrant box add eskapade-dev.json
  vagrant up

Eskapade users and developers log in as the user ``esdev`` on ``localhost``, by default on port 2222.  An example SSH
configuration is provided in ``vagrant/dev/ssh/config``.  This user can log in with the password ``esdev``, but in
general, the key ``vagrant/dev/ssh/esdev_id_rsa`` will be used.  SSH access is configured by doing

.. code:: bash

  cp vagrant/dev/ssh/esdev_id_rsa ~/.ssh/
  cat vagrant/dev/ssh/config >> ~/.ssh/config
