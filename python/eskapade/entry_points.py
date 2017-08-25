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

import logging

logging.basicConfig(
    format='%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s',
    level=logging.INFO,
)


def eskapade_ignite():
    print('Boom!')


def eskapade_run():
    """Run Eskapade

    Top-level entry point for an Eskapade run started from the
    command line.  Arguments specified by the user are parsed and
    converted to settings in the configuration object.  Optionally, an
    interactive IPython session is started when the run is finished.
    """
    import IPython
    import pandas as pd

    from eskapade import core, process_manager, ConfigObject, DataStore
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
        # get process manager, config object, and data store
        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # set Pandas display options
        pd.set_option('display.width', 120)
        pd.set_option('display.max_columns', 50)

        # start interactive session
        logging.info("Continuing interactive session ... press Ctrl+d to exit.\n")
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


def eskapade_generate_link():
    """Generate Eskapde link.

    :return:
    """

    import argparse
    import os
    import sys
    import datetime

    # Do not modify the indentation of template!
    template = """
# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : {link_name!s}
# * Created: {date_generated!s}
# * Description:                                                                   *
# *      Algorithm to do...(fill in one-liner here)                                *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************
/

from eskapade import process_manager
from eskapade import ConfigObject
from eskapade import Link
from eskapade import DataStore
from eskapade import StatusCode

class {link_name!s}(Link):
    \"\"\"Defines the content of link {link_name!s}\"\"\"

    def __init__(self, **kwargs):
        \"\"\"Initialize {link_name!s} instance

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        \"\"\"

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', {link_name!s}))

        # Process and register keyword arguments.  All arguments are popped from
        # kwargs and added as attributes of the link.  The values provided here
        # are defaults.
        self._process_kwargs(kwargs, read_key=None, store_key=None)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off line above, and on two lines below if you wish to keep these
        # extra kwargs.
        #self.kwargs = kwargs

    def initialize(self):
        \"\"\"Initialize {link_name!s}

        :returns: status code of initialization
        :rtype: StatusCode
        \"\"\"

        return StatusCode.Success

    def execute(self):
        \"\"\"Execute {link_name!s}

        :returns: status code of execution
        :rtype: StatusCode
        \"\"\"

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # --- your algorithm code goes here

        self.log().debug('Now executing link: %s', self.name)

        return StatusCode.Success

    def finalize(self):
        \"\"\"Finalize {link_name!s}

        :returns: status code of finalization
        :rtype: StatusCode
        \"\"\"

        # --- any code to finalize the link follows here

        return StatusCode.Success

"""

    parser = argparse.ArgumentParser('eskapade_generate_link')
    parser.add_argument('link_name',
                        help='The name of the link to generate.',
                        )
    parser.add_argument('--links_root_dir',
                        nargs='?',
                        default=os.getcwd(),
                        help='The analysis project root directory. Default is: ' + os.getcwd(),
                        )
    args = parser.parse_args()

    # First expand ~ if present.
    # Second take care of any . or ..
    links_dir = os.path.abspath(os.path.expanduser(args.links_root_dir)) + '/links'

    status = 0

    try:
        logging.info('Creating links directory {dir!s}'.format(dir=links_dir))
        os.makedirs(links_dir, exist_ok=True)
    except PermissionError as e:
        logging.error('Failed to create links directory {dir!s}! error={err!s}'
                      .format(dir=links_dir, err=e.strerror))
        status = e.errno

    if status == 0:

        try:
            logging.info('Creating __init__.py in links directory {dir!s}'.format(dir=links_dir))
            fp = open(links_dir + '/__init__.py', 'w')
        except PermissionError:
            logging.error('Failed to create __init__.py in links directory {dir!s}! error={err!s}'
                          .format(dir=links_dir, err=e.strerror))
            status = e.errno
        else:
            with fp:
                fp.write('# Created by Eskapade on {date!s}'.format(date=datetime.date.today()))

    if status == 0:

        try:
            logging.info('Creating {link_name!s}.py links directory {dir!s}'
                         .format(link_name=args.link_name, dir=links_dir))
            fp = open(links_dir + '/{link_name!s}.py'.format(link_name=args.link_name), 'w')
        except PermissionError:
            logging.error('Failed to create {link_name!s}.py in links directory {dir!s}! error={err!s}'
                          .format(link_name=args.link_name, dir=links_dir, err=e.strerror))
            status = e.errno
        else:
            with fp:
                fp.write(template.format(link_name=args.link_name, date_generated=datetime.date.today()))

    return sys.exit(status)
