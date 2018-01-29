"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/08/23

Description:
    Collection of helper functions to get fixtures, i.e. test data,
    ROOT/RooFit libs, and tutorials. These are mostly used by the
    (integration) tests.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pathlib
import sys

from pkg_resources import resource_filename

import eskapade

# Fixtures
_FIXTURE = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'data')).glob('*')}

# C/C++ libraries that are shipped with eskapade.
if sys.platform == 'darwin':
    _LIBS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'lib')).glob('*.dylib')}
else:
    _LIBS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'lib')).glob('*.so')}

# Tutorials that are shipped with eskapade.
_TUTORIALS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'tutorials')).glob('*.py')}

# Templates that are shipped with eskapade.
_TEMPLATES = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'templates')).glob('*')}

# Resource types
_RESOURCES = {
    'fixture': _FIXTURE,
    'library': _LIBS,
    'tutorial': _TUTORIALS,
    'template': _TEMPLATES,
}


def _resource(resource_type, name: str) -> str:
    """Return the full path filename of a resource.

    :param resource_type The type of the resource.
    :param name: The name of the resource.
    :return: The full path filename of the fixture data set.
    :rtype: str
    :raises FileNotFoundError: If the resource cannot be found.
    """
    full_path = _RESOURCES[resource_type].get(name, None)

    if full_path and full_path.exists():
        return str(full_path)

    raise FileNotFoundError('Could not find {resource_type} "{name!s}"! Does it exist?'
                            .format(resource_type=resource_type, name=name))


def fixture(name: str) -> str:
    """Return the full path filename of a fixture data set.

    :param name: The name of the fixture.
    :return: The full path filename of the fixture data set.
    :rtype: str
    :raises FileNotFoundError: If the fixture cannot be found.
    """
    return _resource('fixture', name)


def lib(name: str) -> str:
    """Return the full path filename of a library.

    :param name: The name of the library.
    :return: The full path filename of the library.
    :rtype: str
    :raises FileNotFoundError: If the library cannot be found.
    """
    return _resource('library', name)


def tutorial(name: str) -> str:
    """Return the full path filename of a tutorial.

    :param name: The name of the tutorial.
    :return: The full path filename of the tutorial.
    :rtype: str
    :raises FileNotFoundError: If the tutorial cannot be found.
    """
    return _resource('tutorial', name)


def template(name: str) -> str:
    """Return the full path filename of a tutorial.

    :param name: The name of the template.
    :return: The full path filename of the tutorial.
    :rtype: str
    :raises FileNotFoundError: If the template cannot be found.
    """
    return _resource('template', name)
