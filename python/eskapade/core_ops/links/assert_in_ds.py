"""Project: Eskapade - A python-based package for data analysis.

Class: AssertInDs

Created: 2016/11/08

Description:
    Algorithm that asserts that items exists in the datastore

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


class AssertInDs(Link):
    """Asserts that specified item(s) exists in the datastore."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        Store the configuration of link AssertInDs

        :param str name: name of link
        :param lst keySet: list of keys to check
        """
        Link.__init__(self, kwargs.pop('name', 'AssertInDs'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, keySet=[])
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute the link."""
        ds = process_manager.service(DataStore)

        for key in self.keySet:
            assert key in ds, 'Key {} not in DataStore.'.format(key)

        return StatusCode.Success
