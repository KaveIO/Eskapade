# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : LINKTEMPLATE
# * Created: DATE
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
        """Initialize LINKTEMPLATE instance

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'LINKTEMPLATE'))

        # Process and register keyword arguments.  All arguments are popped from
        # kwargs and added as attributes of the link.  The values provided here
        # are defaults.
        self._process_kwargs(kwargs, read_key=None, store_key=None)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off line above, and on two lines below if you wish to keep these
        # extra kwargs.
        #self.kwargs = kwargs

    def initialize(self):
        """Initialize LINKTEMPLATE

        :returns: status code of initialization
        :rtype: StatusCode
        """

        return StatusCode.Success

    def execute(self):
        """Execute LINKTEMPLATE

        :returns: status code of execution
        :rtype: StatusCode
        """

        proc_mgr = ProcessManager()
        settings = proc_mgr.service(ConfigObject)
        ds = proc_mgr.service(DataStore)

        # --- your algorithm code goes here

        self.log().debug('Now executing link: %s', self.name)

        return StatusCode.Success

    def finalize(self):
        """Finalize LINKTEMPLATE

        :returns: status code of finalization
        :rtype: StatusCode
        """

        # --- any code to finalize the link follows here

        return StatusCode.Success
