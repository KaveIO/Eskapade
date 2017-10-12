Tutorial 4: creating a new analysis project
-------------------------------------------

Now that we have covered how to make a link, macro, and notebook we can create a new analysis project.
To generate a new project type the following:

.. code-block::  bash

  $ eskapade_bootstrap --project_root_dir ./yourproject -m yourmacro -l YourLink --n yournotebook yourpackage

The script will create a Python package called ``yourpackage`` in the path specified in the ``--project_root_dir`` argument.
The arguments ``-m``, ``-l``, and ``-n`` are optional, if not specified the default values are used.

The generated project has the following structure::

   |-yourproject
      |-yourpackage
         |-links
            |-__init__.py
            |-yourlink.py
         |-__init__.py
         |-yourmacro.py
         |-yournotebook.ipynb
      |-setup.py

The project contains a link module called ``yourlink.py`` under ``links`` directory,
a macro ``yourmacro.py``, and a Jupyter notebook ``yournotebook.ipynb`` with required dependencies.
To add more of each to the project you can use the commands ``generate_link``, ``generate_macro``, and ``generate_notebook``
like it was done before.

The script also generates ``setup.py`` file and the package can be installed as any other pip package.

Let's try to debug the project interactively within a Jupyter notebook.
First, go to your project directory and install the package in an editable mode:

.. code-block:: bash

  $ cd yourproject
  $ pip install -e .

As you can see in the output, installation checks if ``eskapade`` and its requirements are installed.

If the installation was successful, run Jupyter and open ``yournotebook.ipynb`` in ``yourpackage`` directory:

.. code-block::  bash

  $ jupyter notebook

As you see in the code the notebook runs ``yourmacro.py``:

.. code-block:: python

  settings['macro'] = '$ESKAPADE/yourproject/yourpackage/yourmacro.py'

Now run the cells in the notebook and check if the macro runs properly.
