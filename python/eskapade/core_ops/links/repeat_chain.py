# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : RepeatChain                                                           *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm that sends "repeat this chain" signal to processManager,        *
# *      until ready.                                                              *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import ConfigObject
from eskapade import Link
from eskapade import StatusCode
from eskapade import process_manager


class RepeatChain(Link):
    """Algorithm that sends signal to processManager to repeat the current chain"""

    def __init__(self, **kwargs):
        """Link that sends signal to processManager to repeat the current chain

        Sents a RepeatChain deenums.StatusCode signal.

        :param str name: name of link
        :param list listenTo: repeat this chain if given key is present in ConfigObject and set to true. E.g. this key is set by readtods link when looping over files. 
        :param int maxcount: repeat this chain until max count has been reacher. Default is -1 (off).
        """

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'RepeatChain'))

        # process keyword arguments
        self._process_kwargs(kwargs, maxcount=-1, listenTo=[])
        self.check_extra_kwargs(kwargs)

        self._counter = 0
        
    def initialize(self):
        """Initialize RepeatChain"""

        if self.listenTo is not None:
            if isinstance(self.listenTo,list): pass
            elif isinstance(self.listenTo,str) and len(self.listenTo)>0:
                self.listenTo = [self.listenTo]
            else:
                raise Exception('listenTo key of incorrect type.')
        else:
            raise Exception('listenTo key of incorrect type.')
        
        return StatusCode.Success

    def execute(self):
        """Execute RepeatChain"""

        settings = process_manager.service(ConfigObject)

        # search for listenTo key in ConfigObject. if present and true, send signal to repeat current chain.
        for l in self.listenTo:
            if l in settings and settings[l]:
                self.log().debug('The repeater count is: {:d}'.format(self._counter))
                self._counter += 1
                return StatusCode.RepeatChain
                
        # repeat this chain until counter reaches specified maxcount value..
        while self._counter < self.maxcount:
            self.log().debug('The repeater count is: {:d}. Max count is: {:d}'.format(self._counter, self.maxcount))
            self._counter += 1
            return StatusCode.RepeatChain

        return StatusCode.Success

