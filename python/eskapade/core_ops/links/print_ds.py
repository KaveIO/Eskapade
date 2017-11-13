"""Project: Eskapade - A python-based package for data analysis.

Class: PrintDs

Created: 2016/11/08

Description:
    Algorithm to print the content of the datastore.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade import process_manager, Link, DataStore, StatusCode


class PrintDs(Link):
    """Print the content of the datastore."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param list keys: keys of items to print explicitly.
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'PrintDs'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, keys=[])

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute the link.

        Print overview of the datastore in current state.
        """
        ds = process_manager.service(DataStore)
        ds.Print()

        if not self.keys:
            return StatusCode.Success
        self.logger.info("*-------------------------------------------------*")
        for k in sorted(self.keys):
            if k not in ds:
                self.logger.warning('Datastore does not contain key "{key}". Skipping.', key=k)
                continue
            item = ds[k]
            line = "  {key} = {value}".format(key=k, value=type(item) if not hasattr(item, '__str__') else str(item))
            self.logger.info(line)
        self.logger.info("*-------------------------------------------------*")
        return StatusCode.Success
