"""Project: Eskapade - A python-based package for data analysis.

Class: EventLooper

Created: 2016/11/08

Description:
    EventLooper algorithm processes input lines and reprints them,
    e.g. to use with map/reduce

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import copy
import sys

from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class EventLooper(Link):
    """Event looper algorithm processes input lines and reprints or stores them.

    Input lines are taken from sys.stdin, processed, and printed on screen.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str filename: file name where the strings are located (txt or similar). Default is None. (optional)
        :param str store_key: key to collect in datastore. If set lines are collected. (optional)
        :param list line_processor_set: list of functions to apply to input lines. (optional)
        :param bool sort: if true, sort lines before storage (optional)
        :param bool unique: if true, keep only unique lines before storage (optional),
        :param list skip_line_beginning_with: skip line if it starts with any of the list. input is list of strings.
            Default is ['#'] (optional)
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'EventLooper'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs,
                             filename=None,
                             store_key=None,
                             line_processor_set=[],
                             sort=False,
                             unique=False,
                             skip_line_beginning_with=['#'])

        # process keyword arguments
        self.check_extra_kwargs(kwargs)

        # default line stream to pick up lines is set to sys.stdin below
        # input stream and possible input file
        self._f = None
        self._linestream = None

        # collect lines for storage
        self._collect = False

    def initialize(self):
        """Perform basic checks of configured attributes."""
        if self.store_key is not None:
            assert isinstance(self.store_key, str) and self.store_key, 'output key not set.'
            self._collect = True

        # default line stream is set to sys.stdin
        self._linestream = sys.stdin

        # try to open input file, if provided.
        # if successful, switch linestream to file.
        if self.filename is not None:
            assert isinstance(self.filename, str) and len(self.filename), 'input file name not set properly.'
            try:
                self._f = open(self.filename, "r")
            except IOError:
                raise Exception('Cannot open file {}.'.format(self.filename))
            # successful, so switch linestream to file.
            self._linestream = self._f

        return StatusCode.Success

    def execute(self):
        """Process all incoming lines.

        No output is printed except for lines that are passed on,
        such that the output lines can be picked up again by another parser.
        """
        lines = []

        # default line stream is set to sys.stdin
        # print or collect (processed) lines
        for line in self._linestream:
            line = line.strip()
            # skip empty and comment lines
            if not line or any(line.startswith(c) for c in self.skip_line_beginning_with):
                continue
            myline = copy.deepcopy(line)
            for func in self.line_processor_set:
                myline = func(myline)
            if not self._collect:
                print(myline)
            else:
                lines.append(myline)

        if not self._collect:
            return StatusCode.Success

        # perform basic operations before storage, if desired:
        # sorting and unique set.
        if self.sort:
            lines = sorted(lines)
        if self.unique:
            lines = list(set(lines))

        ds = process_manager.service(DataStore)
        ds[self.store_key] = lines
        ds['n_' + self.store_key] = len(lines)

        return StatusCode.Success

    def finalize(self):
        """Close open file if present."""
        if self._f is not None:
            self._f.close()

        return StatusCode.Success
