# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2017/02/27                                                            *
# * Description:                                                                   *
# *      Base class and core implementations of run-process services               *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
import pickle
import re
from collections import defaultdict
from typing import Any

import eskapade.utils
from eskapade.core import persistence
from eskapade.core.definitions import CONFIG_DEFAULTS
from eskapade.core.definitions import CONFIG_OPTS_SETTERS
from eskapade.core.definitions import CONFIG_VARS
from eskapade.core.definitions import USER_OPTS
from eskapade.core.exceptions import UnknownSetting
from eskapade.mixins import LoggingMixin


class ProcessServiceMeta(type):
    """Meta class for process-services base class"""

    def __str__(self):
        """Get printable specification of service"""
        return '{0:s}.{1:s}'.format(self.__module__, self.__name__)

    @property
    def persist(self):
        """Flag to indicate if service can be persisted"""

        return self._persist


class ProcessService(LoggingMixin, metaclass=ProcessServiceMeta):
    """Base class for process services"""

    _persist = False

    def __init__(self):
        """Initialize service instance"""

        pass

    def __str__(self):
        """Get printable specification of service instance"""

        return '{0:s} ({1:s})'.format(str(type(self)), hex(id(self)))

    @classmethod
    def create(cls):
        """Create an instance of this service

        :returns: service instance
        :rtype: ProcessService
        """

        # create instance and make sure the service is initialized
        inst = cls()
        ProcessService.__init__(inst)
        return inst

    def finish(self):
        """Finish current processes

        This function can be implemented by a process-service implementation to
        finish running processes and clean up to prepare for a reset of the
        process manager.  This would typically involve deleting large objects
        and closing files and database connections.
        """

        pass

    @classmethod
    def import_from_file(cls, file_path):
        """Import service instance from a Pickle file

        :param str file_path: path of Pickle file
        :returns: imported service instance
        :rtype: ProcessService
        :raises: RuntimeError, TypeError
        """

        # check if service can be persisted
        if cls.persist:
            cls.log().debug('Importing service instance of "%s" from file "%s"', str(cls), file_path)
        else:
            cls.log().debug('Not importing service "%s"', str(cls))
            return None

        # check specified file path
        if not os.path.isfile(file_path):
            cls.log().critical('Specified path for importing "%s" instance is not a file: "%s"', str(cls), file_path)
            raise RuntimeError('invalid file path specified for importing process service')

        try:
            # try to open file and import instance
            with open(file_path, 'rb') as inst_file:
                inst = pickle.load(inst_file)
        except Exception as exc:
            # re-raise exeption if import failed
            cls.log().warning('Failed to import service instance of "%s" from file "%s"', str(cls), file_path)
            raise exc

        # check type of instance
        if not isinstance(inst, cls):
            cls.log().critical('Expected to import "%s" instance, got object of type "%s"',
                               str(cls), type(inst).__name__)
            raise TypeError('incorrect type for imported service object')

        return inst

    def persist_in_file(self, file_path):
        """Persist service instance in Pickle file

        :param str file_path: path of Pickle file
        """

        # check if service can be persisted
        if type(self).persist:
            self.log().debug('Persisting service instance "%s" in file "%s"', str(self), file_path)
        else:
            self.log().debug('Not persisting service "%s"', str(type(self)))
            return

        try:
            # try to persist
            with open(file_path, 'wb') as inst_file:
                pickle.dump(self, inst_file)
        except Exception as exc:
            # give warning if persisting failed
            self.log().warning('Failed to persist service instance "%s" in file "%s":', str(self), file_path)
            self.log().warning('Caught exception "%s"', str(exc))


class ConfigObject(ProcessService):
    """Configuration settings for Eskapade

    The ConfigObject is a dictionary meant for containing global settings of
    Eskapade.  Settings are set in the configuration macro of an
    analysis, or on the command line.

    The ConfigObject is a dictionary meant only for storing global settings
    of Eskapade.  In general, it is accessed through the process
    manager.

    Example usage:

    >>> # first set logging output level.
    >>> import logging
    >>> logging.basicConfig(level=logging.DEBUG)

    Obtain the ConfigObject from any location as follows:

    >>> from eskapade import ProcessManager, ConfigObject
    >>> proc_mgr = ProcessManager()
    >>> settings = proc_mgr.settings
    >>> proc_mgr.settings = settings
    >>> settings = proc_mgr.service(ConfigObject)

    One can treat the ConfigObject as any other dictionary:

    >>> settings['foo'] = 'bar'
    >>> foo = settings['foo']

    Write the ConfigObject to a pickle file with:

    >>> settings.persist_in_file(file_path)

    And reload from the pickle file with:

    >>> settings = ConfigObject.import_from_file(file_path)

    A ConfigObject pickle file can be read in by Eskapade with the
    command line option (-u).
    """

    _persist = True

    def __init__(self):
        """Initialize ConfigObject instance"""
        self.__settings = defaultdict()

        # Initialize self with default values.
        for section in CONFIG_VARS.values():
            for config in section:
                self.__settings[config] = CONFIG_DEFAULTS.get(config)

        # initialize batch-mode setting with display variable from environment
        display = eskapade.utils.get_env_var('display')
        self.__settings['batchMode'] = display is None or not re.search(':\d', display)

        # initialize file I/O paths with repository directories with repo root from environment
        self.__settings['esRoot'] = eskapade.utils.get_dir_path('es_root')

    def __repr__(self):
        return repr(self.__settings)

    def __getitem__(self, setting: str) -> Any:
        """Get value of setting by name.

        :param setting: The setting to get.
        :return: The value of setting.
        :raise: UnknownSetting if it does not exist.
        """
        if setting in self.__settings:
            return self.__settings[setting]

        raise UnknownSetting('Unknown setting {setting}!'.format(setting=setting))

    def __setitem__(self, setting: str, value: Any) -> None:
        """Set the value of a setting.

        Note this overrides the current value a the setting.

        :param setting:
        :param value:
        :return: None
        """
        self.__settings[setting] = value

    def __contains__(self, setting):
        """Is a setting in settings?

        :param setting: The setting to check for.
        :return: True if setting is present else False.
        """
        return setting in self.__settings

    def get(self, setting: str, default: Any = None) -> object:
        """Get value of setting. If it does not exists return the default value.

        :param setting: The setting to get.
        :param default: The default value of the setting.
        :return: The value of the setting or None if it does not exist.
        """
        try:
            return self.__getitem__(setting)
        except UnknownSetting:
            return default

    def __copy__(self):
        """Perform a shallow copy of self.

        :return:
        """
        clone = ConfigObject()
        clone.__settings = self.__settings.copy()

        return clone

    def copy(self):
        """Perform a shallow copy of self.

        :return:
        """
        return self.__copy__()

    def io_base_dirs(self) -> dict:
        """Get configured base directories

        :returns: base directories
        :rtype: dict
        """

        return dict([(key + '_dir', self.__getitem__(key + 'Dir'))
                     for key in ['results', 'data', 'macros', 'templates']])

    def io_conf(self):
        """Get I/O configuration

        The I/O configuration contains storage locations and basic analysis
        info.

        :return: I/O configuration
        :rtype: IoConfig
        """

        return persistence.IoConfig(analysis_name=self['analysisName'],
                                    analysis_version=self['version'],
                                    **self.io_base_dirs())

    def Print(self):
        """Print a summary of the settings"""

        # print standard settings
        self.log().info('Run configuration')
        for sec, sec_keys in CONFIG_VARS.items():
            if not sec_keys:
                continue
            self.log().info('  {}:'.format(sec))
            max_key_len = max(len(k) for k in sec_keys)
            for key in sec_keys:
                self.log().info('    {{0:<{:d}s}}  {{1:s}}'.format(max_key_len).format(key, str(self.get(key))))

        # print additional custom settings
        add_keys = sorted(set(self.__settings.keys()) - set(o for s in CONFIG_VARS.values() for o in s))
        if add_keys:
            self.log().info('  custom:')
            max_key_len = max(len(k) for k in add_keys)
        for key in add_keys:
            self.log().info('    {{0:<{:d}s}}  {{1:s}}'.format(max_key_len).format(key, str(self.get(key))))

    def add_macros(self, macro_paths):
        """Add configuration macros for Eskapade run"""

        # convert input to list if single path is specified
        if isinstance(macro_paths, str):
            macro_paths = [macro_paths]

        # loop over specified file paths
        # FIXME: add all specified macros instead of only the first one
        for path in macro_paths:
            self['macro'] = path
            break

    def set_user_opts(self, parsed_args):
        """Set options specified by user on command line

        :param argparse.Namespace parsed_args: parsed user arguments
        """

        # loop over arguments
        args = vars(parsed_args)
        known_opts = set(opt for sec_opts in USER_OPTS.values() for opt in sec_opts)
        for opt_key in args.keys():
            # only process known config options
            if opt_key not in known_opts:
                continue

            # call setter function for this user option
            CONFIG_OPTS_SETTERS[opt_key](opt_key, self, args)


class DataStore(ProcessService, dict):
    """Store for transient data sets and related objects

    The data store is a dictionary meant for storing transient data sets or
    any other objects.  Links can take one or several data sets as input,
    transform them or use them as input for a model, and store the output
    back again in the datastore, to be picked up again by any following link.

    Example usage:

    >>> # first set logging output level.
    >>> import logging
    >>> logging.basicConfig(level=logging.DEBUG)

    Obtain the global datastore from any location as follows:

    >>> from eskapade import ProcessManager, DataStore
    >>> proc_mgr = ProcessManager()
    >>> ds = proc_mgr.service(DataStore)

    One can treat the datastore as any other dict:

    >>> ds['a'] = 1
    >>> ds['b'] = 2
    >>> ds['0'] = 3
    >>> a = ds['a']

    Write the datastore to a pickle file with:

    >>> ds.persist_in_file(file_path)

    And reload from the pickle file with:

    >>> ds = DataStore.import_from_file(file_path)
    """

    _persist = True

    def Print(self):
        """Print a summary the data store contents"""

        self.log().info('Summary of data store ({:d} objects)'.format(len(self)))
        if not self:
            return

        max_key_len = max(len(k) for k in self.keys())
        for key in sorted(self.keys()):
            self.log().info('  {{0:<{:d}s}}  <{{1:s}}.{{2:s}} at {{3:x}}>'.
                            format(max_key_len).format(key,
                                                       type(self[key]).__module__,
                                                       type(self[key]).__name__,
                                                       id(self[key])))
