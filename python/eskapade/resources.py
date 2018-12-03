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
_TUTORIALS = {_.name if _.parent.name == 'tutorials' else _.parent.name + '/' + _.name:
              _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'tutorials')).glob('**/*.py')}

# Templates that are shipped with eskapade.
_TEMPLATES = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'templates')).glob('*')}

# Configs that are shipped with eskapade.
_CONFIGS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'config')).glob('**/*')}

# Notebooks that are shipped with eskapade.
_NOTEBOOKS = {_.name: _ for _ in pathlib.Path(resource_filename(eskapade.__name__, 'notebooks')).glob('*.ipynb')}

# Resource types
_RESOURCES = {
    'fixture': _FIXTURE,
    'library': _LIBS,
    'tutorial': _TUTORIALS,
    'template': _TEMPLATES,
    'config': _CONFIGS,
    'notebook': _NOTEBOOKS
}


def _resource(resource_type, name: str) -> str:
    """Return the full path filename of a resource.

    :param str resource_type: The type of the resource.
    :param str  name: The name of the resource.
    :returns: The full path filename of the fixture data set.
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

    :param str name: The name of the fixture.
    :returns: The full path filename of the fixture data set.
    :rtype: str
    :raises FileNotFoundError: If the fixture cannot be found.
    """
    return _resource('fixture', name)


def lib(name: str) -> str:
    """Return the full path filename of a library.

    :param str name: The name of the library.
    :returns: The full path filename of the library.
    :rtype: str
    :raises FileNotFoundError: If the library cannot be found.
    """
    return _resource('library', name)


def tutorial(name: str) -> str:
    """Return the full path filename of a tutorial.

    :param str name: The name of the tutorial.
    :returns: The full path filename of the tutorial.
    :rtype: str
    :raises FileNotFoundError: If the tutorial cannot be found.
    """
    return _resource('tutorial', name)


def template(name: str) -> str:
    """Return the full path filename of a tutorial.

    :param str name: The name of the template.
    :returns: The full path filename of the tutorial.
    :rtype: str
    :raises FileNotFoundError: If the template cannot be found.
    """
    return _resource('template', name)


def config(name: str) -> str:
    """Return the absolute path of a config.

    :param str name: The name of the config.
    :returns: The absolute path of the config.
    :raises FileNotFoundError: If the config cannot be found.
    """
    return _resource('config', name)


def notebook(name: str) -> str:
    """Return the absolute path of a notebook.

    :param str name: The name of the notebook.
    :returns: The absolute path of the notebook.
    :raises FileNotFoundError: If the notebook cannot be found.
    """
    return _resource('notebook', name)
