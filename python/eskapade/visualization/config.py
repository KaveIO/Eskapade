"""Project: Eskapade - A python-based package for data analysis.

Created: 2016/11/08

Description:
    Utility function to set matplotlib backend

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import os
import sys

from escore.utils import check_interactive_backend
from eskapade.logger import Logger

logger = Logger()

def set_matplotlib_backend(backend=None, batch=None, silent=True):
    """Set Matplotlib backend.

    :param str backend: backend to set
    :param bool batch: require backend to be non-interactive
    :param bool silent: do not raise exception if backend cannot be set
    :raises: RuntimeError
    """
    from eskapade import process_manager, ConfigObject
    settings = process_manager.service(ConfigObject)

    # determine if we think batch mode is required
    run_interactive = check_interactive_backend()

    # priority: 1) function arg, 2) settings, 3) check_interactive_backend()
    if (batch is not None) and isinstance(batch, bool): 
        run_batch = batch
    elif 'batchMode' in settings:
        # batchMode in settings is initialized to: not check_interactive_backend(),
        # but may be overwritten by user
        run_batch = settings.get('batchMode')
    else:
        run_batch = not run_interactive

    # check if interactive mode actually can be used, if it is requested
    if (not run_batch) and (not run_interactive):
        if not silent:
            raise RuntimeError('Interactive Matplotlib mode requested, but no display found.')
        logger.warning('Matplotlib cannot be used interactively; no display found.')

    import matplotlib
    if run_batch:
        matplotlib.interactive(False)

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
            'Set Matplotlib backend to "{0:s}"; non-interactive backend required, but "{1:s}" requested.'.format(ni_backends[0], backend))
        backend = ni_backends[0]

    # check if backend has to change
    if backend == curr_backend:
        return

    # check if backend can still be set
    if 'matplotlib.pyplot' in sys.modules:
        if not silent:
            raise RuntimeError('Cannot set Matplotlib backend: pyplot module already loaded.')
        else:
            logger.warning('Cannot set Matplotlib backend: pyplot module already loaded.')
        return

    # set backend
    matplotlib.use(backend)
