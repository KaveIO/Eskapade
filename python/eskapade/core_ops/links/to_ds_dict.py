# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : ToDsDict                                                       *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      Algorithm to store one object in the DataStore dict during run time.      *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

from eskapade import StatusCode, ProcessManager, DataStore, Link


class ToDsDict(Link):
    """
    Stores one object in the DataStore dict during run time.
    """

    def __init__(self, **kwargs):
        """
        Link to store one external object in the DataStore dict during run time.

        :param str name: name of link
        :param str storeKey: key of object to store in data store
        :param obj: object to store
        :param bool force: overwrite if already present in datastore. default is false. (optional)
        :param bool at_initialize: store at initialize of link. Default is false.
        :param bool at_execute: store at execute of link. Default is true.
        :param bool copydict: if true and obj is a dict, copy all key value pairs into datastore. Default is false.
        """
        
        Link.__init__(self, kwargs.pop('name', 'ToDsDict'))

        # process keyword arguments
        self._process_kwargs(kwargs,
                             storeKey=None,
                             obj=None,
                             at_initialize = False,
                             at_execute = True,
                             force = False,
                             copydict = False)
        self.check_extra_kwargs(kwargs)

        return

    def initialize(self):
        """ Initialize ToDsDict """

        # perform basic checks.
        if self.obj is None:
            raise Exception('ERROR. object <%s> to store is of type None.' % self.storeKey)
        # storage key needs to be set in nearly all cases
        if not (self.copydict and isinstance(self.obj,dict)):
            assert isinstance(self.storeKey,str) and len(self.storeKey), 'ERROR. object storage key has not been set.'

        ds = ProcessManager().service(DataStore)
        return StatusCode.Success if not self.at_initialize else self.dostorage(ds)

    def execute(self):
        """ Execute ToDsDict """

        ds = ProcessManager().service(DataStore)
        return StatusCode.Success if not self.at_execute else self.dostorage(ds)

    def dostorage(self, ds):
        """ perform storage in datastore 

        Function makes a distinction been dicts and any other object.
        """
        # if dict and copydict==true, store all individual items
        if self.copydict and isinstance(self.obj,dict):
            stats = [(self.store(ds, v, k, force=self.force)).value for k,v in self.obj.items()]
            return StatusCode(max(stats))

        # default: store obj under storekey
        return self.store(ds, self.obj, force=self.force)
