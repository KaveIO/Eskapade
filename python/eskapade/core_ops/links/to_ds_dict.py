"""Project: Eskapade - A python-based package for data analysis.

Class: ToDsDict

Created: 2016/11/08

Description:
    Algorithm to store one object in the DataStore dict during run time.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class ToDsDict(Link):
    """Stores one object in the DataStore dict during run time."""

    def __init__(self, **kwargs):
        """Link to store one external object in the DataStore dict during run time.

        :param str name: name of link
        :param str store_key: key of object to store in data store
        :param obj: object to store
        :param bool force: overwrite if already present in datastore. default is false. (optional)
        :param bool at_initialize: store at initialize of link. Default is false.
        :param bool at_execute: store at execute of link. Default is true.
        :param bool copydict: if true and obj is a dict, copy all key value pairs into datastore. Default is false.
        """
        Link.__init__(self, kwargs.pop('name', 'ToDsDict'))

        # process keyword arguments
        self._process_kwargs(kwargs,
                             store_key=None,
                             obj=None,
                             at_initialize=False,
                             at_execute=True,
                             force=False,
                             copydict=False)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        # perform basic checks.
        if self.obj is None:
            raise RuntimeError('object "{}" to store is of type None'.format(self.store_key))
        # storage key needs to be set in nearly all cases
        if not (self.copydict and isinstance(self.obj, dict)):
            if not (isinstance(self.store_key, str) and self.store_key):
                raise RuntimeError('object storage key has not been set')

        ds = process_manager.service(DataStore)
        return StatusCode.Success if not self.at_initialize else self.do_storage(ds)

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        return StatusCode.Success if not self.at_execute else self.do_storage(ds)

    def do_storage(self, ds):
        """Perform storage in datastore.

        Function makes a distinction been dicts and any other object.
        """
        # if dict and copydict==true, store all individual items
        if self.copydict and isinstance(self.obj, dict):
            stats = [(self.store(ds, v, k, force=self.force)).value for k, v in self.obj.items()]
            return StatusCode(max(stats))

        # default: store obj under store_key
        return self.store(ds, self.obj, force=self.force)
