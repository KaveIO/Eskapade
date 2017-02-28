# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2017/02/27                                                            *
# * Description:                                                                   *
# *      Base classes for the building blocks of an Eskapade analysis run:         *
# *      Chains and Links                                                          *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import os
from copy import deepcopy, copy

from eskapade.core.definitions import StatusCode
from eskapade.core import persistence
from eskapade.core.mixins import LoggingMixin, ArgumentsMixin


class Link(ArgumentsMixin, LoggingMixin):
    """Link base class

    A link defines the content of an algorithm.  Any actual link is derived
    from this base class.

    A link usually does three things:
    - takes data from the datastore
    - does something to it
    - writes data back

    To take from the data store there is a simple function load()
    To write to the data store there is a simple function store()

    Links are added to a chain as follows:

    >>> # add a chain first
    >>> proc_mgr = ProcessManager()
    >>> overview = proc_mgr.add_chain('Overview')
    >>>
    >>> # add a link to the chain
    >>> from eskapade import analysis
    >>> reader = analysis.ReadToDf(name='CsvReader', key='foo')
    >>> reader.path = 'foo.csv'
    >>> overview.add_link(reader)

    A link has an initialize, execute, and finalize method.  execute() is the
    central function that executes the algorithm code.  initialize() and
    finalize() are supporting functions before and afterexecute().

    * initialize():
      Initialise internal link variables, if needed

    >>> # done here by hand, but normally done in the process manager
    >>> pds.initialize()

    * execute():
      Execute the algorithm

    >>> # by hand
    >>> pds.execute()

    * finalize():
      Finalize the link, if needed

    >>> # by hand
    >>> pds.finalize()

    See the documentation of these methods, or example links for more info.

    :param name: The name of the link
    """

    def __init__(self, name):
        """Initialize link instance"""

        self.name = name
        self.prefix = ''

        # readKey is typically the name string of the object picked up from the datastore
        # readKey can be a string or list of strings
        self.readKey = None
        # storeKey is typically the name string of the object put back in the datastore
        self.storeKey = name
        # required kwargs. filled by _process_kwargs
        self._required_vars = []
        # chain is a reference to the chain that executes this link.
        self.chain = None
        # return code by load()
        self.ifInputMissing = StatusCode.Failure
        # return code by store()
        self.ifOutputExists = StatusCode.Success

        self.init_instance(name)

    def __str__(self):
        """String of the link"""

        return '%s link "%s"' % (self.__class__.__name__, self.name)

    def _process_kwargs(self, kwargs, **name_val):
        """Process the key word arguments

        :param kwargs: key word arguments to process
        :param str name_val: name of a required kw arg/setting
        """

        super(Link, self)._process_kwargs(kwargs, **name_val)
        if name_val:
            self._required_vars += list(name_val.keys())

    def summary(self):
        """Print a summary of the main settings of the link"""

        self.log().debug("Link: %s" % self.name)
        for key in sorted(self._required_vars):
            line = "  attr: %s = " % key
            if not hasattr(self, key):
                raise KeyError('%s does not contain item "%s"' % (str(self), str(key)))
            item = getattr(self, key)
            line += type(item) if not hasattr(item, '__str__') else str(item)
            self.log().debug(line)

    def __ret_data_list(self, ds, dlist):
        """Internal method used by load method"""

        dats = []
        stats = []
        for r in dlist:
            try:
                dats.append(ds[r])
                stats.append(StatusCode.Success.value)
            except KeyError:
                self.log().warn("Some input data did not exist " + str(r) + " " + self.prefix)
                stats.append(self.ifInputMissing.value)
                dats.append(None)
        return StatusCode(max(stats)), dats

    def load(self, ds, readKey=None):
        """Read all data from specified source

        readKey can either be:

        * one Link: return statuscode, [data_from_link,...]
        * A list of locations: return statuscode, [data,...]
        * A list of links with only one output location: return statuscode, [data,...]
        * A list of links with multiple output locations: return statuscode, [data,[moredata]...]
        * Any mixture of the above

        Do something logical with a statuscode if this data does not exist
        link.ifInputMissing = statuscode

        :returns: a tuple statuscode, [data in same order as readKey]
        :rtype: (StatusCode,list)
        """

        if readKey is not None:
            rk = readKey
        elif self.readKey is not None:
            rk = self.readKey
        else:
            self.log().debug("No readKey defined " + self.prefix)
            return self.ifInputMissing, []

        # Handle the case where this is only one link
        if isinstance(rk, Link):
            if rk.storeKey is None:
                self.log().warn("Link has no storeKey " + rk.name + " " + self.prefix)
                return self.ifInputMissing, [None]
            rk = rk.storeKey
        # Always treat it as a list
        if type(rk) in [str, str]:
            rk = [rk]
        # Iterate over this list
        stats = []
        dats = []
        for r in rk:
            try:
                # Handle links which appear in lists ...
                if isinstance(r, Link):
                    if r.storeKey is None:
                        self.log().warn("Link has no storeKey " + r.name + " " + self.prefix)
                        stats.append(self.ifInputMissing.value)
                        dats.append(None)
                        continue
                    r = r.storeKey
                    if type(r) not in [str, str]:
                        stati, dati = self.__ret_data_list(ds, r)
                        stats.append(stati.value)
                        dats.append(dati)
                        continue
                dats.append(ds[r])
                stats.append(StatusCode.Success.value)
            except KeyError:
                self.log().warn("Some input data did not exist " + str(r) + " " + self.prefix)
                stats.append(self.ifInputMissing.value)
                dats.append(None)
        return StatusCode(max(stats)), dats

    def __check_store_loc(self, ds, loc):
        """Check if a location exists

        If yes, return a status code, if no, print an error and return
        link.ifOutputExists

        :returns: status code if location exists
        :rtype: StatusCode
        """

        if not self.ifOutputExists.isSuccess() and loc in ds:
            self.log().error("Store key already exists, I am not overwriting " + loc + " " + self.prefix)
            return self.ifOutputExists
        return StatusCode.Success

    def store(self, ds, data, storeKey=None, force=False):
        """Store data back to datastore

        Do something logical with a statuscode if this data already exists
        link.ifOutputExists = statuscode uses self.storeKey.  If self.storeKey is
        a list of locations, I must sent a list of the same length here
        """

        if storeKey is not None:
            sk = storeKey
        elif self.storeKey is not None:
            sk = self.storeKey
        else:
            raise AttributeError("storeKey has not been set for this link, so I cannot store! " + self.prefix)

        if type(sk) not in [str, str]:
            if len(data) != len(self.storeKey):
                raise ValueError("If you want to store multiple things at once, then the length of the things"
                                 "you want to store must be the same as the length of self.storeKey")
        else:
            data = [data]
            sk = [sk]
        stats = []
        for d, k in zip(data, sk):
            stat = StatusCode.Success if force else self.__check_store_loc(ds, k)
            stats.append(stat.value)
            # If it's not a success then it's not going to be stored
            if not stat.isSuccess():
                continue
            ds[k] = d
            self.log().debug('Put object <%s> in data store with type <%s>.' % (k, type(d)))
        return StatusCode(max(stats))

    @property
    def name(self):
        return self._name if self._name else ''

    @name.setter
    def name(self, name):
        try:
            self._name = str(name)
        except:
            self.log().warning('name could not be set')

    def clone(self, prefix=""):
        """Clone into a new object

        :param str prefix: Optional new prefix for the copied algorithm
        """

        if prefix == "":
            prefix = self.prefix
        # copies all properties prior to initialize
        newLink = deepcopy(self)
        newLink.init_instance(prefix)
        return newLink

    def init_instance(self, prefix):
        """Initialize prefix settings

        :param str prefix: The prefix to use
        """

        self.prefix = prefix

    def initialize_link(self):
        """Initialize the link

        :returns: status code of initialize attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        # initialize
        self.log().debug("Now initializing link \'%s\'" % self.name)
        # print summary of main members
        self.summary()
        # run initialize function of actual link
        status = self.initialize()
        self.log().debug("Done initializing link \'%s\'" % self.name)

        return status

    def execute_link(self):
        """Execute the link

        :returns: status code of execute attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        # run execute function of actual link
        self.log().debug("Now executing link \'%s\'" % self.name)
        status = self.execute()
        self.log().debug("Done executing link \'%s\'" % self.name)

        return status

    def finalize_link(self):
        """Finalizing the link

        :returns: status code of finalize attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        # run finalize function of actual link
        self.log().debug("Now finalizing link \'%s\'" % self.name)
        status = self.finalize()
        self.log().debug("Done finalizing link \'%s\'" % self.name)

        return status

    def initialize(self):
        """Initialize the link

        This function is supposed to be overloaded by the actual link.

        :returns: status code of initialize attempt
        :rtype: StatusCode
        """
        return StatusCode.Success

    def execute(self):
        """Execute the link

        This function is supposed to be overloaded by the actual link.

        :returns: status code of execute attempt
        :rtype: StatusCode
        """
        return StatusCode.Success

    def finalize(self):
        """ Finalizing the link

        This function is supposed to be overloaded by the actual link.

        :returns: status code of finalize attempt
        :rtype: StatusCode
        """
        return StatusCode.Success


class Chain(LoggingMixin):
    """Chain of links

    A Chain object contains a collection of links with analysis code.  The
    links in a chain are executed in the order in which the links have been
    added to the chain.  Typically a chain contains all links related to one
    topic, for example 'validation of a model', or 'data preparation', or
    'data quality checks'.

    Chains are added to the processManager (PM) thusly:

    >>> proc_mgr = ProcessManager()
    >>> overview = proc_mgr.add_chain('Overview')

    And Links are added to a chain as follows:

    >>> # add a link to the chain
    >>> from eskapade import analysis
    >>> overview.add_link(analysis.ReadToDf(path='foo.csv', key='foo'))

    A Chain has an initialize, execute, and finalize method.

    * initialize():
      Initialise internal chain variables
      Then initialize the links in the chain.

    * execute():
      Execute the links in the chain

    * finalize():
      Finalize the links in the chain
      If configured, then store datastore and configuration
    """

    def __init__(self, name):
        """Initialize the new chain with certain name

        :param str name: name of the chain
        """
        self.init_instance(name)

    def clone(self, newName=""):
        """Clone chain into a new one

        :param str newName: optional new name for the chain
        """

        if newName == "":
            newName = self.name

        #  copies all properties prior to initialize
        newChn = deepcopy(self)
        newChn.init_instance(newName)

        newChn.links = deepcopy(self.links)

        for link in newChn.links:
            link.init_instance(newChn.prefix)
        return newChn

    def init_instance(self, name):
        """Initialize chain

        :param str name: name of chain
        """

        # shared method between __init__ and clone
        self.name = name
        self.prefix = 'prefix/' + self.name

        # attributes to below are OK to deepcopy
        self.prevChainName = ''
        self.links = []
        self.exitStatus = StatusCode.Undefined

    def initialize(self):
        """Initialize internal variables and links

        :returns: status code of initialization attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.log().debug("Now initializing chain \'%s\'" % self.name)

        # initialization
        for mod in self.links:
            status = mod.initialize_link()
            if status.isFailure():
                self.log().critical("Problem initializing link <%s> in chain <%s>." % (mod.name, self.name))
                return status
            elif status.isSkipChain():
                self.log().warning("Skipping chain <%s>, as requested by link <%s>." % (self.name, mod.name))
                return status

        self.log().debug("Done initializing chain \'%s\'" % self.name)

        return status

    def execute(self):
        """Initialize, execute, finalize links in the chain

        :returns: status code of execution attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.log().debug("Now executing chain \'%s\'" % self.name)

        # execution
        for mod in self.links:
            status = mod.execute_link()
            if status.isFailure():
                self.log().critical("Problem executing link <%s> in chain <%s>." % (mod.name, self.name))
                return status
            elif status.isRepeatChain():
                self.log().warning("Repeating chain <%s>, as requested by link <%s>." % (self.name, mod.name))
                return status
            elif status.isSkipChain():
                self.log().warning("Skipping chain <%s>, as requested by link <%s>." % (self.name, mod.name))
                return status

        self.log().debug("Done executing chain \'%s\'" % self.name)

        return status

    def finalize(self):
        """Finalize the chain and the links in the chain

        :returns: status code of finalization attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.log().debug("Now finalizing chain \'%s\'" % self.name)

        # finalization
        for mod in self.links:
            status = mod.finalize_link()
            if status.isFailure():
                self.log().critical("Problem finalizing link <%s> in chain <%s>." % (mod.name, self.name))
                return status

        self.log().debug("Done finalizing chain \'%s\'" % self.name)

        return status

    def add_link(self, obj, do_deepcopy=True):
        """Add link as a pre-built object

        :param obj: The link to add
        :param bool do_deepcopy: if true (default) make copy of the link
        :returns: the link just added
        :rtype: Link
        """

        if not isinstance(obj, Link):
            raise RuntimeError("add_link does not support input of type '%s'." % (type(obj)))

        # Verify that this name is not already used
        for mod in self.links:
            if mod.name == obj.name:
                raise RuntimeError("Link %s already exists in Chain %s. Please use a different name."
                                   % (obj.name, self.name))
        # Create a copy
        if do_deepcopy:
            newObj = deepcopy(obj)
        else:
            newObj = copy(obj)

        # Reset algorithm parent
        newObj.chain = self

        # Add algorithm clone to the list
        self.links.append(newObj)
        return self.links[-1]

    def get_link(self, name):
        """Find the link with the given name

        :param str name: The name of the link to search for
        :returns: the link just found
        :rtype: Link
        :raises RuntimeError: if link name not found
        """

        for mod in self.links:
            if mod.name == name:
                return mod

        raise RuntimeError("No link with name %s found" % name)

    def has_link(self, name):
        """Check if link with name exists in this chain

        :param str name: The name of the link to search for
        :returns: boolean answer
        :rtype: bool
        """
        for m in self.links:
            if m.name == name:
                return True

        return False

    def remove_links(self):
        """Remove all links in the chain"""

        for i in range(0, len(self.links)):
            self.links.pop()

    def remove_link(self, mod):
        """Remove link from this chain

        :param mod: Link of the chain to remove
        """

        if isinstance(mod, Link):
            aMod = mod
        elif isinstance(mod, str):
            aMod = self.get_link(mod)
        else:
            raise ValueError("Chain: link type %s not supported" % (type(mod)))

        try:
            self.links.remove(aMod)
        except:
            self.log().warning("Unable to remove link %s from chain %s" % (aMod.name, self.name))
