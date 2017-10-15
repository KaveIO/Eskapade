# **************************************************************************************
# * Project: Eskapade - A python-based package for data analysis                       *
# * Created : 2017-08-08                                                               *
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

from eskapade import _bootstrap as bootstrap
from eskapade.logger import LogLevel, Logger, global_log_publisher, ConsoleHandler, ConsoleErrHandler

publisher = global_log_publisher
publisher.log_level = LogLevel.INFO
publisher.add_handler(ConsoleHandler())
publisher.add_handler(ConsoleErrHandler())

logger = Logger(__name__)


def eskapade_ignite():
    """Log info message."""
    logger.info('Boom!')


def eskapade_run():
    """Run Eskapade.

    Top-level entry point for an Eskapade run started from the
    command line.  Arguments specified by the user are parsed and
    converted to settings in the configuration object.  Optionally, an
    interactive IPython session is started when the run is finished.
    """
    import IPython
    import pandas as pd

    from eskapade import process_manager
    from eskapade import core, ConfigObject, DataStore
    from eskapade.core.run_utils import create_arg_parser

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
        # set Pandas display options
        pd.set_option('display.width', 120)
        pd.set_option('display.max_columns', 50)

        # start interactive session
        ds = process_manager.service(DataStore)
        logger.info("Continuing interactive session ... press Ctrl+d to exit.\n")
        IPython.embed()


def eskapade_trial():
    """Run Eskapade tests.

    We will keep this here until we've completed switch to pytest or nose and tox.
    We could also keep it, but I don't like the fact that packages etc. are
    hard coded. Gotta come up with
    a better solution.
    """
    import sys
    import pytest

    # ['--pylint'] +
    # -r xs shows extra info on skips and xfails.
    default_options = ['-rxs']
    args = sys.argv[1:] + default_options
    sys.exit(pytest.main(args))


def _add_project_dir_argument(parser, arg_name, dir_type):
    """Add a project directory argument to a parser.

    :param parser: argparse
    :param arg_name: argument's name
    :param dir_type: type of the directory to be created
    """
    import os

    parser.add_argument('--{arg_name}'.format(arg_name=arg_name),
                        nargs='?',
                        default=os.getcwd(),
                        help='The analysis project {dir_type} directory. Default is: {default}.'
                        .format(dir_type=dir_type, default=os.getcwd()), )


def eskapade_generate_link():
    """Generate Eskapade link.

    By default does not create init file.
    """
    import argparse

    parser = argparse.ArgumentParser('eskapade_generate_link',
                                     description='Generate Eskapade link.')
    parser.add_argument('name',
                        help='The name of the link to generate.', )
    _add_project_dir_argument(parser, 'dir', 'links')
    parser.add_argument('--create_init',
                        nargs='?',
                        default=False,
                        type=bool,
                        help='Whether to create __init__.py file or no. Default is: False.', )
    args = parser.parse_args()

    if args.name == 'Link':
        raise AttributeError('Link is reserved by Eskapade. Please, choose different name for the link.')

    path = bootstrap.get_absolute_path(args.dir)

    bootstrap.create_dir(path)
    bootstrap.generate_link(path, args.name, args.create_init)


def eskapade_generate_macro():
    """Generate Eskapade macro."""
    import argparse

    parser = argparse.ArgumentParser('eskapade_generate_macro',
                                     description='Generate Eskapade macro.')
    parser.add_argument('name',
                        help='The name of the macro to generate.', )
    _add_project_dir_argument(parser, 'dir', 'macros')
    args = parser.parse_args()

    path = bootstrap.get_absolute_path(args.dir)

    bootstrap.create_dir(path)
    bootstrap.generate_macro(path, args.name, False)


def eskapade_generate_notebook():
    """Generate Eskapade notebook."""
    import argparse

    parser = argparse.ArgumentParser('eskapade_generate_notebook',
                                     description='Generate Eskapade notebook.')
    parser.add_argument('name',
                        help='The name of the notebook to generate.', )
    _add_project_dir_argument(parser, 'dir', 'notebooks')
    args = parser.parse_args()

    path = bootstrap.get_absolute_path(args.dir)

    bootstrap.create_dir(path)
    bootstrap.generate_notebook(notebook_dir=path, notebook_name=args.name)


def eskapade_bootstrap():
    """Generate Eskapade project structure."""
    import argparse

    parser = argparse.ArgumentParser('eskapade_bootstrap',
                                     description='Generate Eskapade project structure.',
                                     epilog='Please note, existing files with the same names will be rewritten.')
    parser.add_argument('project_name',
                        help='The name of the project to generate.', )
    _add_project_dir_argument(parser, 'project_root_dir', 'root')
    parser.add_argument('--macro_name', '-m',
                        nargs='?',
                        help='The name of the macro to generate. Default is: macro.',
                        default='macro', )
    parser.add_argument('--link_name', '-l',
                        nargs='?',
                        help='The name of the link to generate. Default is: ExampleLink.',
                        default='ExampleLink', )
    parser.add_argument('--notebook_name', '-n',
                        nargs='?',
                        help='The name of the notebook to generate. Default is: notebook.',
                        default='notebook', )
    args = parser.parse_args()

    if args.link_name == 'Link':
        raise AttributeError('Link is reserved by Eskapade. Please, choose different name for the link.')
    if args.project_name.lower() == 'eskapade':
        raise AttributeError('eskapade is reserved by Eskapade. Please, choose different name for the project.')

    project_dir = bootstrap.get_absolute_path(args.project_root_dir) + '/' + args.project_name
    link_dir = project_dir + '/links'
    marco_path = project_dir + '/' + args.macro_name + '.py'

    # create the directories
    bootstrap.create_dir(link_dir)
    bootstrap.generate_link(link_dir=link_dir, link_name=args.link_name, is_create_init=True)
    bootstrap.generate_macro(macro_dir=project_dir, macro_name=args.macro_name,
                             link_module=args.project_name, link_name=args.link_name, is_create_init=True)
    bootstrap.generate_notebook(notebook_dir=project_dir, notebook_name=args.notebook_name, macro_path=marco_path)
    bootstrap.generate_setup(root_dir=args.project_root_dir, project_name=args.project_name)
