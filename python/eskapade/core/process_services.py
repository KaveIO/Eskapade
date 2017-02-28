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

import logging
import os
import pickle

from . import project_utils, persistence, definitions
from .mixins import LoggingMixin


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


class ConfigObject(ProcessService, dict):
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

        # version, analysisName, and configuration file define an entire analysis
        # version and analysisName are used for bookkeeping purposes (e.g. directory structure)
        self['version'] = 0
        self['analysisName'] = ''
        # file path of the configuration macro
        self['macro'] = ''
        # display mode
        self['batchMode'] = True
        # logging level used throughout Eskapade run
        self['logLevel'] = logging.INFO
        # seed for random generator
        self['seed'] = 0
        # base directories for results, macros, data
        self['esRoot'] = project_utils.get_dir_path('es_root')
        self['resultsDir'] = self['esRoot'] + ('/' if self['esRoot'] else '') + 'results'
        self['dataDir'] = self['esRoot'] + ('/' if self['esRoot'] else '') + 'data'
        self['macrosDir'] = self['esRoot'] + ('/' if self['esRoot'] else '') + 'tutorials'
        self['templatesDir'] = self['esRoot'] + ('/' if self['esRoot'] else '') + 'templates'
        # mongo collections
        self['all_mongo_collections'] = None

    def io_base_dirs(self):
        """Get configured base directories

        :returns: base directories
        :rtype: dict
        """
        return dict([(key + '_dir', self[key + 'Dir']) for key in ['results', 'data', 'macros', 'templates']])

    def io_conf(self):
        """Get I/O configuration

        The I/O configuration contains storage locations and basic analysis
        info.

        :return: I/O configuration
        :rtype: IoConfig
        """
        return persistence.IoConfig(analysis_name=self['analysisName'], analysis_version=self['version'],
                                    **self.io_base_dirs())

    def Print(self):
        """Print a summary of the settings"""

        self.log().info("*---------------------------------------------------*")
        self.log().info("     Configuration")
        self.log().info("*---------------------------------------------------*")
        self.log().info("analysis name:        %s", self['analysisName'])
        self.log().info("version:              %s", self['version'])
        self.log().info("configuration file:   %s", self['macro'])
        self.log().info("logging level:        %s", definitions.LOG_LEVELS[self['logLevel']])
        self.log().info("results dir:          %s", self['resultsDir'])
        self.log().info("random seed:          %s", self['seed'])
        self.log().info("objects in dict: %d", len(self))
        for key in sorted(self.keys()):
            self.log().info("    %s: %s", str(key), str(self[key]))
        self.log().info("*---------------------------------------------------*")


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
        """Print a summary of the contents of the data store"""

        self.log().info("*-------------------------------------------------*")
        self.log().info("     Summary of DataStore")
        self.log().info("*-------------------------------------------------*")
        self.log().info("Objects in dict: %d", len(list(self.keys())))
        for key in sorted(self.keys()):
            self.log().info("    key name: %s  %s" % (key, type(self[key])))
        self.log().info("*-------------------------------------------------*")
