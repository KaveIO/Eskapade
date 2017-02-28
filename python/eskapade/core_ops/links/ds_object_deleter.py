# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : DsObjectDeleter                                                       *
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

from eskapade import ProcessManager, StatusCode, DataStore, Link, ConfigObject


class DsObjectDeleter(Link):
    """
    Deletes objects from the DataStore by the key they are under, or keeps only the data by the keys you give.
    """

    def __init__(self, **kwargs):
        """
        Store the configuration of link DsObjectDeleter

        :param str name: name of link
        :param lst deletionKeys: keys to clear. Overwrites clearAll to false.
        :param lst keepOnly: keys to keep. Overwrites clearAll to false.
        :param bool clearAll: clear all key-value pairs in the datastore. Default is true.
        """
        
        Link.__init__(self, kwargs.pop('name', 'DsObjectDeleter'))

        # process keyword arguments
        self._process_kwargs(kwargs, deletionKeys=[], keepOnly=[], clearAll=True)
        self.check_extra_kwargs(kwargs)
        
        return

    def initialize(self):
        """ Initialize DsObjectDeleter """

        # Overwrites clearAll to false if individual keys are set.
        if len(self.deletionKeys): self.clearAll = False
        if len(self.keepOnly): self.clearAll = False
        
        return StatusCode.Success

    def execute(self):
        """ Execute DsObjectDeleter """

        settings = ProcessManager().service(ConfigObject)
        ds = ProcessManager().service(DataStore)

        # used in code testing only
        if 'TESTING' in settings and settings['TESTING']==True:
            self.log().warning('Running in TESTING mode. NOT clearing datastore for testing purposes.')
            return StatusCode.Success

        # delete specific items
        for key in self.deletionKeys:
            if key in ds:
                self.log().debug('Now deleting datastore object with key <%s>', key)
                del ds[key]

        # delete all but specific items
        if len(self.keepOnly):
            keys = list(ds.keys())
            for key in keys:
                if not key in self.keepOnly:
                    self.log().debug('Now deleting datastore object with key <%s>', key)
                    del ds[key]

        # delete all items in datastore
        if self.clearAll:
            keys = list(ds.keys())
            for key in keys:
                self.log().debug('Now deleting datastore object with key <%s>', key)
                del ds[key]
                
        return StatusCode.Success

