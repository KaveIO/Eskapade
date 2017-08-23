# ********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                 *
# * Created: 2017/08/23                                                          *
# * Description:                                                                 *
# *     Collection of helper functions to get fixtures, i.e. test data,          *
# *     ROOT/RooFit libs, and tutorials. These are mostly used by the            *
# *     (integration) tests.                                                     *
# *                                                                              *
# * Authors:                                                                     *
# *      KPMG Big Data team, Amstelveen, The Netherlands                         *
# *                                                                              *
# * Redistribution and use in source and binary forms, with or without           *
# * modification, are permitted according to the terms listed in the file        *
# * LICENSE.                                                                     *
# ********************************************************************************

import pathlib

from pkg_resources import resource_filename

import eskapade

# Fixtures
_FIXTURE = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'data')).glob('*')}

# C/C++ libraries that are shipped with eskapade.
_LIBS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'lib')).glob('*.so')}

# Tutorials that are shipped with eskapade.
_TUTORIALS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'tutorials')).glob('*.py')}


def fixture(name: str) -> str:
    """ Return the full path filename of a fixture data set.


    :param name: The name of the fixture.
    :return: The full path filename of the fixture data set.
    :rtype: str
    :raises ValueError: If the fixture cannot be found.
    """

    full_path = _FIXTURE.get(name, None)

    if full_path and full_path.exists():
        return str(full_path)

    raise ValueError('Could not find fixture "{fixture!s}"! Does it exist?')


def lib(name: str) -> str:
    """ Return the full path filename of a library.

    :param name: The name of the library.
    :return: The full path filename of the library.
    :rtype: str
    :raises ValueError: If the library cannot be found.
    """

    full_path = _LIBS.get(name, None)

    if full_path and full_path.exists():
        return str(full_path)

    raise ValueError('Could not find library "{name!s}"! Does it exist?'.format(name=name))


def tutorial(name: str) -> str:
    """Return the full path filename of a tutorial.
    
    :param name: The name of the tutorial.
    :return: The full path filename of the tutorial.
    :rtype: str
    :raises ValueError: If the template cannot be found.
    """

    full_path = _TUTORIALS.get(name, None)

    if full_path and full_path.exists():
        return str(full_path)

    raise ValueError('Could not find tutorial "{name!s}"! Does it exist?'.format(name=name))
