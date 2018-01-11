"""Project: Eskapade - A python-based package for data analysis.

Created: 2016/11/08

Description:
    Utility functions to collect Eskapade python modules
    e.g. functions to get correct Eskapade file paths and env variables

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import os
import sys

import matplotlib

from eskapade.logger import Logger

ENV_VARS = dict(spark_args='PYSPARK_SUBMIT_ARGS',
                docker='DE_DOCKER', display='DISPLAY')
ARCHIVE_FILE = 'es_python_modules.egg'

logger = Logger()


def set_matplotlib_backend(backend=None, batch=None, silent=True):
    """Set Matplotlib backend.

    :param str backend: backend to set
    :param bool batch: require backend to be non-interactive
    :param bool silent: do not raise exception if backend cannot be set
    :raises: RuntimeError
    """
    # determine if batch mode is required
    display = get_env_var('display')
    run_batch = bool(batch) or display is None or not display.startswith(':') or not display[1].isdigit()
    if run_batch:
        matplotlib.interactive(False)

    # check if batch mode can be set if it is requested
    if not batch and batch is not None and run_batch:
        if not silent:
            raise RuntimeError('Interactive Matplotlib mode requested, but no display found.')
        logger.warning('Matplotlib cannot be used interactively; no display found.')

    # get Matplotlib backends
    curr_backend = matplotlib.get_backend().lower()
    ni_backends = [nib.lower() for nib in matplotlib.rcsetup.non_interactive_bk]

    # determine backend to be set
    if not backend:
        # try to use current backend
        backend = curr_backend if not run_batch or curr_backend in ni_backends else ni_backends[0]
    backend = str(backend).lower()

    # check if backend is compatible with mode
    if run_batch and backend not in ni_backends:
        if not silent:
            raise RuntimeError('Non-interactive Matplotlib backend required, but "{!s}" requested.'.format(backend))
        logger.warning(
            'Set Matplotlib backend to "{actual}"; non-interactive backend required, but "{requested}" requested.',
            actual=ni_backends[0], requested=backend)
        backend = ni_backends[0]

    # check if backend has to change
    if backend == curr_backend:
        return

    # check if backend can still be set
    if 'matplotlib.pyplot' in sys.modules:
        if not silent:
            raise RuntimeError('cannot set Matplotlib backend: pyplot module already loaded.')
        return

    # set backend
    matplotlib.use(backend)


def get_env_var(key):
    """Retrieve Eskapade-specific environment variables.

    :param str key: Eskapade-specific key to variable
    :returns: environment variable value
    :rtype: str
    """
    var_name = ENV_VARS[key]
    return os.environ.get(var_name)


def collect_python_modules():
    """Collect Eskapade Python modules."""
    import pathlib
    from pkg_resources import resource_filename
    from zipfile import PyZipFile

    import eskapade

    package_dir = resource_filename(eskapade.__name__, '')
    lib_path = pathlib.Path(package_dir).joinpath('lib')
    lib_path.mkdir(exist_ok=True)
    archive_path = str(lib_path.joinpath(ARCHIVE_FILE))

    archive_file = PyZipFile(archive_path, 'w')
    logger.info('Adding Python modules to egg archive {path}.'.format(path=archive_path))
    archive_file.writepy(package_dir)
    archive_file.close()
    return archive_path
