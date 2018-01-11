"""Project: Eskapade - A python-based package for data analysis.

Class: HelloWorld

Created: 2017/01/31

Description:
    Algorithm to do print Hello {}!

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import Link
from eskapade import StatusCode


class HelloWorld(Link):
    """Defines the content of link HelloWorld."""

    def __init__(self, **kwargs):
        """Store the configuration of link HelloWorld.

        :param str name: name assigned to the link
        :param str hello: name to print in Hello World! Defaults to 'World'
        :param int repeat: repeat print statement N times. Default is 1
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'HelloWorld'))

        # process keyword arguments
        self._process_kwargs(kwargs, hello='World', repeat=1)

        # check residual kwargs.
        # (turn line off if you wish to keep these to pass on.)
        self.check_extra_kwargs(kwargs)
        # self.kwargs = kwargs

    def execute(self):
        """Execute the link."""
        for _ in range(self.repeat):
            self.logger.info('Hello {hello}', hello=self.hello)

        return StatusCode.Success
