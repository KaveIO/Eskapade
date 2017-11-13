"""Project: Eskapade - A python-based package for data analysis.

Class: LinePrinter

Created: 2017/02/21

Description:
    Simple algorithm to pick up lines and reprint them.

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


class LinePrinter(Link):
    """LinePrinter picks up lines from the datastore and prints them."""

    def __init__(self, **kwargs):
        """Set up the configuration of link LinePrinter.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'LinePrinter'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, read_key=None)

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""
        assert isinstance(self.read_key, str) and len(self.read_key), 'read_key not set properly.'

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        No output is printed except for lines that are passed on,
        such that the output lines can be picked up again by another parser.
        """
        ds = process_manager.service(DataStore)

        # just print the lines!
        lines = ds[self.read_key]
        assert isinstance(lines, list) and len(lines), 'lines is not a (filled) list.'

        for line in lines:
            print(line)

        return StatusCode.Success
