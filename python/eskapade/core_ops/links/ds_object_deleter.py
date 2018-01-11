"""Project: Eskapade - A python-based package for data analysis.

Class: DsObjectDeleter

Created: 2016/11/08

Description:
    Algorithm to delete objects from the datastore.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode


class DsObjectDeleter(Link):
    """Delete objects from the datastore.

    Delete objects from the DataStore by the key they are under, or keeps
    only the data by the specified keys.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param list deletion_keys: keys to clear. Overwrites clear_all to false.
        :param list deletion_classes: delete object(s) by class type.
        :param lsst keep_only: keys to keep. Overwrites clear_all to false.
        :param bool clear_all: clear all key-value pairs in the datastore. Default is true.
        """
        Link.__init__(self, kwargs.pop('name', 'DsObjectDeleter'))

        # process keyword arguments
        self._process_kwargs(kwargs, deletion_keys=[], deletion_classes=[], keep_only=[], clear_all=True)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # Overwrites clear_all to false if individual keys are set.
        if len(self.deletion_keys):
            self.clear_all = False
        if len(self.deletion_classes):
            self.clear_all = False
        if len(self.keep_only):
            self.clear_all = False

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        settings = process_manager.service(ConfigObject)
        ds = process_manager.service(DataStore)

        # used in code testing only
        if settings.get('TESTING'):
            self.logger.warning('Running in TESTING mode. NOT clearing datastore for testing purposes.')
            return StatusCode.Success

        # delete specific items
        for key in self.deletion_keys:
            if key in ds:
                self.logger.debug('Now deleting datastore object with key "{key}".', key=key)
                del ds[key]

        # delete specific class types
        for cls in self.deletion_classes:
            for key in ds:
                if isinstance(ds[key], cls):
                    self.logger.debug('Now deleting datastore object with key "{key}".', key=key)
                    del ds[key]

        # delete all but specific items
        if len(self.keep_only):
            keys = list(ds.keys())
            for key in keys:
                if key not in self.keep_only:
                    self.logger.debug('Now deleting datastore object with key "{key}".', key=key)
                    del ds[key]

        # delete all items in datastore
        if self.clear_all:
            keys = list(ds.keys())
            for key in keys:
                self.logger.debug('Now deleting datastore object with key "{key}".', key=key)
                del ds[key]

        return StatusCode.Success
