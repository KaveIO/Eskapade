"""Project: Eskapade - A python-based package for data analysis.

Class: DsApply

Created: 2018-06-30

Description:
    Simple link to execute functions, to which datastore has been passed.

    Helps in the development of links.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, ConfigObject, DataStore, Link, StatusCode


class DsApply(Link):

    """Simple link to execute functions to which datastore is passed."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param list apply: list of functions to execute at execute(), to which datastore is passed
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'DsApply'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, apply=[])

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        ds = process_manager.service(DataStore)

        # --- pass ds to list of functions, to execute bits of code
        #     by doing this here, the objects previously created can be picked up.
        for func in self.apply:
            self.logger.debug('Now executing function: {function}.', function=func.__name__)
            func(ds)

        return StatusCode.Success
