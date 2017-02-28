# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : BreakLink                                                             *
# * Created: 2017/02/26                                                            *
# * Description:                                                                   *
# *      Algorithm to send break signal to process manager and halt execution      *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, ConfigObject, Link, DataStore, StatusCode


class BreakLink(Link):
    """Link sends failure signal and halts execution of process manager

    Break the execution of the processManager at a specific location 
    by simply adding this link at any location in a chain.
    """

    def __init__(self, **kwargs):
        """Store the configuration of link BreakLink

        :param str name: name of link
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'BreakLink'))

        # check residual kwargs. exit if any present. 
        self.check_extra_kwargs(kwargs)


    def execute(self):
        """Execute BreakLink"""

        self.log().info('Now sending break signal to halt execution!')
        # halt the execution of proc_mgr by sending a failure signal
        return StatusCode.Failure

