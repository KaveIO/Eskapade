# **************************************************************************************
# * Project: Eskapade - A python-based package for data analysis                       *
# * Created : 2015-09-16                                                               *
# *                                                                                    *
# * Description:                                                                       *
# *      Collection of eskapade entry points                                           *
# *                                                                                    *
# * Authors:                                                                           *
# *      KPMG Big Data team, Amstelveen, The Netherlands                               *
# *                                                                                    *
# * Redistribution and use in source and binary forms, with or without                 *
# * modification, are permitted according to the terms listed in the file              *
# * LICENSE.                                                                           *
# **************************************************************************************


def eskapade_ignite():
    print('Boom!')


def eskapade_run():
    """Run Eskapade

    Top-level entry point for an Eskapade run started from the
    command line.  Arguments specified by the user are parsed and
    converted to settings in the configuration object.  Optionally, an
    interactive IPython session is started when the run is finished.
    """
    import logging
    import IPython
    import pandas as pd

    from eskapade import core, ProcessManager, ConfigObject, DataStore
    from eskapade.core.run_utils import create_arg_parser

    log = logging.getLogger(__name__)

    # create parser for command-line arguments
    parser = create_arg_parser()
    user_args = parser.parse_args()

    # create config object for settings
    if not user_args.unpickle_config:
        # create new config
        settings = ConfigObject()
    else:
        # read previously persisted settings if pickled file is specified
        conf_path = user_args.config_files.pop(0)
        settings = ConfigObject.import_from_file(conf_path)
    del user_args.unpickle_config

    # set configuration macros
    settings.add_macros(user_args.config_files)

    # set user options
    settings.set_user_opts(user_args)

    # run Eskapade
    core.execution.run_eskapade(settings)

    # start interpreter if requested (--interactive on command line)
    if settings.get('interactive'):
        # create process manager, config object, and data store
        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        # set Pandas display options
        pd.set_option('display.width', 120)
        pd.set_option('display.max_columns', 50)

        # start interactive session
        log.info("Continuing interactive session ... press Ctrl+d to exit.\n")
        IPython.embed()


def eskapade_trial():
    """Run Eskapade tests.

    We will keep this here until we've completed switch to pytest or nose and tox.
    We could also keep it, but I don't like the fact that packages etc. are
    hard coded. Gotta come up with
    a better solution.
    """
    import unittest
    import argparse
    import logging

    log = logging.getLogger(__name__)

    # parse arguments
    parser = argparse.ArgumentParser('eskapade_trial')
    parser.add_argument('start_dir', nargs='?', help='Folder in which to search for tests')
    parser.add_argument('type',
                        nargs='?',
                        choices=('unit', 'integration'),
                        default='unit',
                        help='Type of test to run (default "unit")')
    args = parser.parse_args()

    log.info('Running {arg} tests\n'.format(arg=args.type))

    # create test suite
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    tests_mods = loader.discover(args.start_dir)
    log.info("Going to run {n_tc} test cases".format(n_tc=tests_mods.countTestCases()))
    suite.addTests(tests_mods)
    # run tests
    unittest.TextTestRunner(verbosity=4).run(suite)
