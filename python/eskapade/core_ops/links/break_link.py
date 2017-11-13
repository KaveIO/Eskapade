"""Project: Eskapade - A python-based package for data analysis.

Class: Break

Created: 2017/02/26

Description:
    Algorithm to send break signal to process manager and halt execution

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import Link
from eskapade import StatusCode


class Break(Link):
    """Halt execution.

    Link sends failure signal and halts execution of process manager.  Break
    the execution of the processManager at a specific location by simply
    adding this link at any location in a chain.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'Break'))

        # check residual kwargs. exit if any present
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute the link."""
        # halt the execution of process_manager by sending a failure signal
        self.logger.info('Now sending break signal to halt execution!')
        return StatusCode.Failure
