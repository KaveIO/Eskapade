"""Project: Eskapade - A python-based package for data analysis.

Created: 2016/11/08

Description:
    Functions for running and resetting Eskapade machinery

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import sys

import eskapade.utils
from eskapade.core.process_manager import process_manager
from eskapade.core.process_services import ConfigObject
from eskapade.logger import Logger

logger = Logger()


def reset_eskapade(skip_config=False):
    """Reset Eskapade objects.

    :param bool skip_config: skip reset of configuration object
    """
    settings = process_manager.service(ConfigObject)
    process_manager.reset()
    if skip_config:
        process_manager.service(settings)


def eskapade_run(settings=None):
    """Run Eskapade.

    This function is called in the script eskapade_run when run
    from the cmd line.  The working principle of Eskapade is to run chains of
    custom code chunks (so-called links).

    Each chain should have a specific purpose, for example pre-processing
    incoming data, booking and/or training predictive algorithms, validating
    these predictive algorithms, evaluating the algorithms.

    By using this principle, links can be easily reused in future projects.

    :param ConfigObject settings: analysis settings
    :return: status of the execution
    :rtype: StatusCode
    """
    # get config object from process manager
    if settings:
        # use supplied settings as config service in process manager
        process_manager.remove_service(ConfigObject, silent=True)
        process_manager.service(settings)
    settings = process_manager.service(ConfigObject)

    def message(msg):
        width = 80
        fence = '*'
        logger.info(fence * width)
        logger.info('{begin}{msg:>{fill}}{end:>{e_fill}}'.format(begin=fence,
                                                                 msg=msg,
                                                                 fill=(width + len(msg)) // 2,
                                                                 end=fence,
                                                                 e_fill=(width - len(msg) - 1) // 2))
        logger.info(fence * width)

    message('Welcome to Eskapade!')

    # check for batch mode
    if settings.get('batchMode'):
        # set non-interactive Matplotlib backend before plotting tools are imported
        eskapade.utils.set_matplotlib_backend(batch=True, silent=False)

    # execute configuration macro, this sets up the order of the chains and links.
    if not settings['macro']:
        raise RuntimeError('macro is not set')
    process_manager.execute_macro(settings['macro'])

    if 'ROOT.RooFit' in sys.modules:
        # initialize logging for RooFit
        from eskapade.root_analysis.roofit_utils import set_rf_log_level
        set_rf_log_level(settings['logLevel'])

    # check analysis name
    if not settings['analysisName']:
        raise RuntimeError('analysis name is not set')

    # standard execution from now on
    status = process_manager.run()

    message('Leaving Eskapade. Bye!')

    return status
