# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : LINKTEMPLATE                                                          *
# * Created: DATE                                                                  *
# * Description:                                                                   *
# *      Algorithm to do...(fill in one-liner here)                                *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode


class LINKTEMPLATE(Link):
    """Defines the content of link LINKTEMPLATE"""

    def __init__(self, **kwargs):
        """Store and do basic check on the attributes of link LINKTEMPLATE

        :param str name: name of link
        :param str readKey: key of input data to read from data store
        :param str storeKey: key of output data to store in data store
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'LINKTEMPLATE'))

        # process and register all relevant kwargs. kwargs are added as attributes of the link.
        # second arg is default value for an attribute. key is popped from kwargs.
        self._process_kwargs(kwargs, readKey=None, storeKey=None)

        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)
        # turn off line above, and on two lines below, if you wish to keep these extra kwargs.
        #import copy
        #self.kwargs = copy.deepcopy(kwargs)

    def initialize(self):
        """Initialize and (further) check the assigned attributes of LINKTEMPLATE"""

        ## --- any initialization code for this link follows here


        return StatusCode.Success

    def execute(self):
        """Execute LINKTEMPLATE"""

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        ## --- your algorithm code goes here

        self.log().info('Now executing link: %s' % self.name)


        return StatusCode.Success

    def finalize(self):
        """Finalize LINKTEMPLATE"""

        ## --- any code to finalize the link follows here


        return StatusCode.Success

