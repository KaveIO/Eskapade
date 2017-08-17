# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : PrintDs                                                        *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to do...(fill in here)                                          *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import process_manager
from eskapade import StatusCode
from eskapade import DataStore
from eskapade import Link


class PrintDs(Link):
    """
    Print the content of the datastore
    """

    def __init__(self, **kwargs):
        """
        Print overview of the datastore in current state

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

        return

    def execute(self):
        """ Execute PrintDs """

        ds = process_manager.service(DataStore)
        ds.Print()

        if len(self.keys):
            self.log().info("*-------------------------------------------------*")
        for k in sorted(self.keys):
            line = "  %s = " % k
            if not k in ds:
                self.log().warning('datastore does not contain item "%s". Skipping.' % k)
                continue
            item = ds[k]
            line += type(item) if not hasattr(item, '__str__') else str(item)
            self.log().info(line)
        if len(self.keys):
            self.log().info("*-------------------------------------------------*")

        return StatusCode.Success
