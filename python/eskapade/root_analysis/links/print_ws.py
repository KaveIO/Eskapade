# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : PrintWs                                                               *
# * Created: 2017/03/27                                                            *
# * Description:                                                                   *
# *      Algorithm to print the contents of the rooworkspace
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode
from eskapade.root_analysis import RooFitManager


class PrintWs(Link):
    """Print the contents of the RooFit workspace"""

    def __init__(self, **kwargs):
        """Initialize PrintWs instance

        :param str name: name of link
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'PrintWs'))

        # check residual kwargs. exit if any present.
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute PrintWs"""

        proc_mgr = ProcessManager()
        ws = proc_mgr.service(RooFitManager).ws

        ws.Print('v')

        return StatusCode.Success
