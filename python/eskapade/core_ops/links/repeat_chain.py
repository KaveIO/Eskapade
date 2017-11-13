"""Project: Eskapade - A python-based package for data analysis.

Class: RepeatChain

Created: 2016/11/08

Description:
    Algorithm that sends "repeat this chain" signal to processManager,
    until ready.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, Link, StatusCode


class RepeatChain(Link):
    """Algorithm that sends signal to processManager to repeat the current chain."""

    def __init__(self, **kwargs):
        """Link that sends signal to processManager to repeat the current chain.

        Sents a RepeatChain deenums.StatusCode signal.

        :param str name: name of link
        :param list listen_to: repeat this chain if given key is present in ConfigObject and set to true.
            E.g. this key is set by readtods link when looping over files.
        :param int maxcount: repeat this chain until max count has been reacher. Default is -1 (off).
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'RepeatChain'))

        # process keyword arguments
        self._process_kwargs(kwargs, maxcount=-1, listen_to=[])
        self.check_extra_kwargs(kwargs)

        self._counter = 0

    def initialize(self):
        """Initialize the link."""
        if isinstance(self.listen_to, list):
            pass
        elif isinstance(self.listen_to, str) and self.listen_to:
            self.listen_to = [self.listen_to]
        else:
            raise Exception('listen_to key of incorrect type.')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        settings = process_manager.service(ConfigObject)

        # search for listen_to key in ConfigObject. if present and true, send signal to repeat current chain.
        for l in self.listen_to:
            if l in settings and settings[l]:
                self.logger.debug('The repeater count is: {n:d}.', n=self._counter)
                self._counter += 1
                return StatusCode.RepeatChain

        # repeat this chain until counter reaches specified maxcount value..
        while self._counter < self.maxcount:
            self.logger.debug('The repeater count is: {n:d}. Max count is: {max:d}', n=self._counter, max=self.maxcount)
            self._counter += 1
            return StatusCode.RepeatChain

        return StatusCode.Success
