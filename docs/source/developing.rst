===========================
Developing and Contributing
===========================

Preliminaries
-------------

Working on Eskapade
-------------------
You have some cool feature and/or algorithm you want to add to Eskapade. How do you go about it?

First clone Eskapade.

.. code-block:: bash

  git clone https://github.com/KaveIO/Eskapade.git eskapade

then

.. code-block:: bash

  pip install -e eskapade

this will install Eskapade in editable mode, which will allow you to edit the code and run it as
you would with a normal installation of eskapade.

To make sure that everything works try executing eskapade without any arguments, e.g.

.. code-block:: bash

  eskapade_run

or you could just execute the tests using either the eskapade test runner, e.g.

.. code-block:: bash

  cd eskapade
  eskapade_trial .

or

.. code-block:: bash

  cd eskapade
  python setup.py test

That's it.

Contributing
------------

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any
other method with the owners of this repository before making a change. You can find the contact information on the
`index <index.html>`_ page.

Note that when contributing that all tests should succeed.

Tips and Tricks
---------------

- Enable auto reload in ipython:

.. code-block:: python

  %load_ext autoreload

this will reload modules before executing any user code.

