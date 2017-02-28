# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : HelloWorld                                                            *
# * Created: 2017/01/31                                                            *
# * Description:                                                                   *
# *      Algorithm to do print Hello %s!                                           *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ProcessManager, StatusCode, DataStore, Link, ConfigObject


class HelloWorld(Link):
    """Defines the content of link HelloWorld"""

    def __init__(self, **kwargs):
        """Store the configuration of link HelloWorld

        :param str name: name assigned to the link
        :param str hello: name to print in Hello World! Defaults to 'World'
        :param int repeat: repeat print statement N times. Default is 1
        """

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'HelloWorld'))

        # process keyword arguments
        self._process_kwargs(kwargs, hello='World', repeat=1)

        # check residual kwargs.
        # (turn line off if you wish to keep these to pass on.)
        self.check_extra_kwargs(kwargs)
        # self.kwargs = kwargs


    def execute(self):
        """Execute HelloWorld"""

        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        for i in range(self.repeat):
            self.log().info('Hello {0}'.format(self.hello))

        return StatusCode.Success

