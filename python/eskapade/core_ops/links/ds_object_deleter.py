# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : DsObjectDeleter                                                       *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to do...(fill in here)                                          *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import process_manager
from eskapade import StatusCode
from eskapade import DataStore
from eskapade import Link
from eskapade import ConfigObject


class DsObjectDeleter(Link):
    """Delete objects from data store

    Delete objects from the DataStore by the key they are under, or keeps
    only the data by the specified keys.
    """

    def __init__(self, **kwargs):
        """Initialize DsObjectDeleter instance

        :param str name: name of link
        :param list deletionKeys: keys to clear. Overwrites clearAll to false.
        :param list deletionClasses: delete object(s) by class type.
        :param lsst keepOnly: keys to keep. Overwrites clearAll to false.
        :param bool clearAll: clear all key-value pairs in the datastore. Default is true.
        """

        Link.__init__(self, kwargs.pop('name', 'DsObjectDeleter'))

        # process keyword arguments
        self._process_kwargs(kwargs, deletionKeys=[], deletionClasses=[], keepOnly=[], clearAll=True)
        self.check_extra_kwargs(kwargs)

        return

    def initialize(self):
        """Initialize DsObjectDeleter"""

        # Overwrites clearAll to false if individual keys are set.
        if len(self.deletionKeys):
            self.clearAll = False
        if len(self.deletionClasses):
            self.clearAll = False
        if len(self.keepOnly):
            self.clearAll = False

        return StatusCode.Success

    def execute(self):
        """Execute DsObjectDeleter"""

        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # used in code testing only
        if settings.get('TESTING'):
            self.log().warning('Running in TESTING mode. NOT clearing datastore for testing purposes.')
            return StatusCode.Success

        # delete specific items
        for key in self.deletionKeys:
            if key in ds:
                self.log().debug('Now deleting datastore object with key "%s"', key)
                del ds[key]

        # delete specific class types
        for cls in self.deletionClasses:
            for key in ds:
                if isinstance(ds[key], cls):
                    self.log().debug('Now deleting datastore object with key "%s"', key)
                    del ds[key]

        # delete all but specific items
        if len(self.keepOnly):
            keys = list(ds.keys())
            for key in keys:
                if key not in self.keepOnly:
                    self.log().debug('Now deleting datastore object with key "%s"', key)
                    del ds[key]

        # delete all items in datastore
        if self.clearAll:
            keys = list(ds.keys())
            for key in keys:
                self.log().debug('Now deleting datastore object with key "%s"', key)
                del ds[key]

        return StatusCode.Success
