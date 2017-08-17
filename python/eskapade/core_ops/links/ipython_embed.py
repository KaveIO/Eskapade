# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : IPythonEmbed                                                          *
# * Created: 2017/02/26                                                            *
# * Description:                                                                   *
# *      Link that starts up an ipython console during execution for debuggin.     *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from IPython import embed

from eskapade import ConfigObject
from eskapade import DataStore
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class IPythonEmbed(Link):
    """Link to start up ipython console

    Start up an ipython console by simply adding this link at any location in a chain.    
    """

    def __init__(self, **kwargs):
        """Init of link IPythonEmbed

        :param str name: name of link
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'IPythonEmbed'))

        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)

    def execute(self):
        """Execute IPythonEmbed"""

        proc_mgr = process_manager
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        self.log().info("Starting interactive session ... press Ctrl+d to exit.\n")
        # this function calls the interactive ipython session
        # in this session ds, settings, and proc_mgr are available
        embed()

        return StatusCode.Success
