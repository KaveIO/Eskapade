"""Project: Eskapade - A python-based package for data analysis.

Class: IPythonEmbed

Created: 2017/02/26

Description:
    Link that starts up an ipython console during execution for debugging.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from IPython import embed

from eskapade import Link
from eskapade import StatusCode


class IPythonEmbed(Link):
    """Link to start up ipython console.

    Start up an ipython console by simply adding this link at any location in a chain.
    """

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'IPythonEmbed'))

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute the link."""
        self.logger.info("Starting interactive session ... press Ctrl+d to exit.\n")
        # this function calls the interactive ipython session
        # in this session ds, settings, and process_manager are available
        embed()

        return StatusCode.Success
