"""Project: Eskapade - A python-based package for data analysis.

Class: ImportDataStore

Created: 2018-03-17

Description:
    Algorithm to import datastore from external pickle file

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import os
from eskapade import process_manager, DataStore, Link, StatusCode


class ImportDataStore(Link):

    """Link to import datastore from external pickle file.

    Import can happen at initialize() or execute(). Default is initialize() 
    """

    def __init__(self, **kwargs):
        """Initialize an instance of the datastore importer link.

        :param str name: name of link
        :param str path: path of the datastore pickle file to import
        :param bool import_at_initialize: if false, perform datastore import at execute. Default is true, at initialize. 
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'ImportDataStore'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, path='', import_at_initialize=True)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        # check input arguments
        self.check_arg_types(path=str)
        self.check_arg_vals('path')

        if not os.path.exists(self.path):
            self.logger.fatal('File path "{path}" does not exist.', path=self.path)
            raise AssertionError('Unable to find input file.')

        if self.import_at_initialize:
            self.import_and_update_datastore()
        return StatusCode.Success

    def import_and_update_datastore(self):
        """Import and update the datastore
        """
        # loading external datastore
        ext_store = DataStore.import_from_file(self.path)
        if not isinstance(ext_store, DataStore):
            self.logger.fatal('Object in file "{path}" not of type DataStore.', path=self.path)
            raise AssertionError('Input object not of type DataStore.')
        # overwriting existing datastore
        ds = process_manager.service(DataStore)
        ds.clear()
        ds.update(ext_store)

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        if not self.import_at_initialize:
            self.import_and_update_datastore()        
        return StatusCode.Success
