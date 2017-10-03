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

from eskapade.core.definitions import StatusCode
from eskapade.logger import Logger
from eskapade.mixins import ArgumentsMixin, TimerMixin


class Link(ArgumentsMixin, TimerMixin):

    """Link base class.

    A link defines the content of an algorithm.  Any actual link is derived
    from this base class.

    A link usually does three things:
    - takes data from the datastore
    - does something to it
    - writes data back

    To take from the data store there is a simple function load()
    To write to the data store there is a simple function store()

    Links are added to a chain as follows:

    >>> from eskapade import process_manager
    >>> # add a chain first
    >>> overview = process_manager.add_chain('Overview')
    >>>
    >>> # add a link to the chain
    >>> from eskapade import analysis
    >>> reader = analysis.ReadToDf(name='CsvReader', key='foo')
    >>> reader.path = 'foo.csv'
    >>> overview.add_link(reader)

    A link has an initialize, execute, and finalize method.  execute() is the
    central function that executes the algorithm code.  initialize() and
    finalize() are supporting functions before and after execute().

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

    logger = Logger()

    def __init__(self, name):
        """Initialize link instance."""
        # initialize timer
        TimerMixin.__init__(self)

        self.name = name
        # read_key is typically the name string of the object picked up from the datastore
        # read_key can be a string or list of strings
        self.read_key = None
        # store_key is typically the name string of the object put back in the datastore
        self.store_key = name
        # required kwargs. filled by _process_kwargs
        self._required_vars = []
        # chain is a reference to the chain that executes this link.
        self.chain = None
        # return code by load()
        self.if_input_missing = StatusCode.Failure
        # return code by store()
        self.if_output_exists = StatusCode.Success

    def __str__(self):
        """Get string representation of the link."""
        return '{} link "{}"'.format(self.__class__.__name__, self.name)

    def _process_kwargs(self, kwargs, **name_val):
        """Process the key word arguments.

        :param kwargs: key word arguments to process
        :param str name_val: name of a required kw arg/setting
        """
        super(Link, self)._process_kwargs(kwargs, **name_val)
        if name_val:
            self._required_vars += list(name_val.keys())

    def summary(self):
        """Print a summary of the main settings of the link."""
        self.logger.debug('Link: {name}', name=self.name)
        for key in sorted(self._required_vars):
            line = '  attr: {} = '.format(key)
            if not hasattr(self, key):
                raise KeyError('{!s} does not contain item "{!s}".'.format(self, key))
            item = getattr(self, key)
            line += type(item) if not hasattr(item, '__str__') else str(item)
            self.logger.debug(line)

    def __ret_data_list(self, ds, dlist):
        """Check if data exist.

        Internal method used by load method.
        """
        dats = []
        stats = []
        for r in dlist:
            try:
                dats.append(ds[r])
                stats.append(StatusCode.Success.value)
            except KeyError:
                self.logger.warn('Some input data did not exist {r!s} {name}', r=r, name=self.name)
                stats.append(self.if_input_missing.value)
                dats.append(None)
        return StatusCode(max(stats)), dats

    def load(self, ds, read_key=None):
        """Read all data from specified source.

        read_key can either be:

        * one Link: return statuscode, [data_from_link,...]
        * A list of locations: return statuscode, [data,...]
        * A list of links with only one output location: return statuscode, [data,...]
        * A list of links with multiple output locations: return statuscode, [data,[moredata]...]
        * Any mixture of the above

        Do something logical with a statuscode if this data does not exist
        link.if_input_missing = statuscode

        :returns: a tuple statuscode, [data in same order as read_key]
        :rtype: (StatusCode,list)
        """
        if read_key is not None:
            rk = read_key
        elif self.read_key is not None:
            rk = self.read_key
        else:
            self.logger.debug('No read_key defined for {name}.', name=self.name)
            return self.if_input_missing, []

        # Handle the case where this is only one link
        if isinstance(rk, Link):
            if rk.store_key is None:
                self.logger.warn('Link has no store_key {link} {name}.', link=rk.name, name=self.name)
                return self.if_input_missing, [None]
            rk = rk.store_key
        # Always treat it as a list
        if isinstance(rk, str):
            rk = [rk]
        # Iterate over this list
        stats = []
        dats = []
        for r in rk:
            try:
                # Handle links which appear in lists ...
                if isinstance(r, Link):
                    if r.store_key is None:
                        self.logger.warn('Link has no store_key {link} {name}.', link=r.name, name=self.name)
                        stats.append(self.if_input_missing.value)
                        dats.append(None)
                        continue
                    r = r.store_key
                    if not isinstance(r, str):
                        stati, dati = self.__ret_data_list(ds, r)
                        stats.append(stati.value)
                        dats.append(dati)
                        continue
                dats.append(ds[r])
                stats.append(StatusCode.Success.value)
            except KeyError:
                self.logger.warn('Some input data did not exist {link!s} {name}.', link=r, name=self.name)
                stats.append(self.if_input_missing.value)
                dats.append(None)
        return StatusCode(max(stats)), dats

    def __check_store_loc(self, ds, loc):
        """Check if a location exists.

        If yes, return a status code, if no, print an error and return
        link.if_output_exists

        :returns: status code if location exists
        :rtype: StatusCode
        """
        if not self.if_output_exists.is_success() and loc in ds:
            self.logger.error('Store key already exists, I am not overwriting {loc} {name}.', loc=loc, name=self.name)
            return self.if_output_exists
        return StatusCode.Success

    def store(self, ds, data, store_key=None, force=False):
        """Store data back to datastore.

        Do something logical with a statuscode if this data already exists
        link.if_output_exists = statuscode uses self.store_key.  If self.store_key is
        a list of locations, I must sent a list of the same length here
        """
        if store_key is not None:
            sk = store_key
        elif self.store_key is not None:
            sk = self.store_key
        else:
            raise AttributeError('store_key has not been set for this link, so I cannot store! {}.'.format(self.name))

        if not isinstance(sk, str):
            if len(data) != len(self.store_key):
                raise ValueError('If you want to store multiple things at once, then the length of the things'
                                 ' you want to store must be the same as the length of self.store_key')
        else:
            data = [data]
            sk = [sk]
        stats = []
        for d, k in zip(data, sk):
            stat = StatusCode.Success if force else self.__check_store_loc(ds, k)
            stats.append(stat.value)
            # If it's not a success then it's not going to be stored
            if not stat.is_success():
                continue
            ds[k] = d
            self.logger.debug('Put object "{key}" in data store with type "{type}".', key=k, type=type(d))
        return StatusCode(max(stats))

    @property
    def name(self):
        """Name of the link."""
        return self._name if self._name else ''

    @name.setter
    def name(self, name):
        """Set the name of the link."""
        try:
            self._name = str(name)
        except Exception:
            self.logger.warning('Name could not be set')

    def initialize_link(self):
        """Initialize the link.

        :returns: status code of initialize attempt
        :rtype: StatusCode
        """
        # initialize
        self.logger.debug('Now initializing link "{link}".', link=self.name)
        # print summary of main members
        self.summary()
        # run initialize function of actual link
        status = self.initialize()
        self.logger.debug('Done initializing link "{link}".', link=self.name)

        return status

    def execute_link(self):
        """Execute the link.

        :returns: status code of execute attempt
        :rtype: StatusCode
        """
        # run execute function of actual link
        self.logger.debug('Now executing link "{link}".', link=self.name)

        # Start the timer directly after the message.
        self.start_timer()

        status = self.execute()

        self.logger.debug('Done executing link "{link}".', link=self.name)

        # Stop the timer when the link is done
        self.stop_timer()

        return status

    def finalize_link(self):
        """Finalize the link.

        :returns: status code of finalize attempt
        :rtype: StatusCode
        """
        # run finalize function of actual link
        self.logger.debug('Now finalizing link "{link}".', link=self.name)
        status = self.finalize()

        self.logger.debug('{link}: execute() took {secs:.2f} seconds.', link=self.name, secs=self.total_time())

        self.logger.debug('Done finalizing link "{link}".', link=self.name)

        return status

    def initialize(self):
        """Initialize the link.

        This function is supposed to be overloaded by the actual link.

        :returns: status code of initialize attempt
        :rtype: StatusCode
        """
        return StatusCode.Success

    def execute(self):
        """Execute the link.

        This function is supposed to be overloaded by the actual link.

        :returns: status code of execute attempt
        :rtype: StatusCode
        """
        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        This function is supposed to be overloaded by the actual link.

        :returns: status code of finalize attempt
        :rtype: StatusCode
        """
        return StatusCode.Success


class Chain(TimerMixin):

    """Chain of links.

    A Chain object contains a collection of links with analysis code.  The
    links in a chain are executed in the order in which the links have been
    added to the chain.  Typically a chain contains all links related to one
    topic, for example 'validation of a model', or 'data preparation', or
    'data quality checks'.

    Chains are added to the process_manager (PM) thusly:

    >>> from eskapade import process_manager
    >>> overview = process_manager.add_chain('Overview')

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

    logger = Logger()

    def __init__(self, name):
        """Initialize the new chain with certain name.

        :param str name: name of the chain
        """
        # initialize timer
        TimerMixin.__init__(self)

        self.name = name
        self.prevChainName = ''
        self.links = []
        self.exitStatus = StatusCode.Undefined

    def initialize(self):
        """Initialize internal variables and links.

        :returns: status code of initialization attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.logger.debug('Now initializing chain "{name}".', name=self.name)

        # Start the timer directly after the initialize message.
        self.start_timer()

        # initialization
        for mod in self.links:
            status = mod.initialize_link()
            if status.is_failure():
                self.logger.fatal('Problem initializing link "{link}" in chain "{chain}".',
                                  link=mod.name, chain=self.name)
                return status
            elif status.is_skip_chain():
                self.logger.warning('Skipping chain "{chain}", as requested by link "{link}".',
                                    chain=self.name, link=mod.name)
                return status

        self.logger.debug('Done initializing chain "{name}".', name=self.name)

        return status

    def execute(self):
        """Initialize, execute, finalize links in the chain.

        :returns: status code of execution attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.logger.debug('Now executing chain "{name}".', name=self.name)

        # execution
        for mod in self.links:
            status = mod.execute_link()
            if status.is_failure():
                self.logger.fatal('Problem executing link "{link}" in chain "{chain}".',
                                  link=mod.name, chain=self.name)
                return status
            elif status.is_repeat_chain():
                self.logger.warning('Repeating chain "{chain}", as requested by link "{link}".',
                                    chain=self.name, link=mod.name)
                return status
            elif status.is_skip_chain():
                self.logger.warning('Skipping chain "{chain}", as requested by link "{link}".',
                                    chain=self.name, link=mod.name)
                return status

        self.logger.debug('Done executing chain "{chain}"', chain=self.name)

        return status

    def finalize(self):
        """Finalize the chain and the links in the chain.

        :returns: status code of finalization attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.logger.debug('Now finalizing chain "{chain}".', chain=self.name)

        # finalization
        for mod in self.links:
            status = mod.finalize_link()
            if status.is_failure():
                self.logger.fatal('Problem finalizing link "{link}" in chain "{chain}".',
                                  link=mod.name, chain=self.name)
                return status

        self.logger.debug('Done finalizing chain "{chain}".', chain=self.name)

        # Stop the timer when the chain is done and print.
        total_time = self.stop_timer()
        self.logger.debug('{chain}: total runtime: {secs:.2f} seconds.', chain=self.name, secs=total_time)

        return status

    def add_link(self, obj):
        """Add link as a pre-built object.

        :param obj: The link to add
        :returns: the link just added
        :rtype: Link
        """
        if not isinstance(obj, Link):
            raise RuntimeError('add_link does not support input of type "{}".'.format(type(obj)))

        # Verify that this name is not already used
        for mod in self.links:
            if mod.name == obj.name:
                raise RuntimeError('Link "{}" already exists in Chain "{}"; please use a different name.'
                                   .format(obj.name, self.name))

        # Reset algorithm parent
        obj.chain = self

        # Add algorithm to the list
        self.links.append(obj)
        return self.links[-1]

    def get_link(self, name):
        """Find the link with the given name.

        :param str name: The name of the link to search for
        :returns: the link just found
        :rtype: Link
        :raises RuntimeError: if link name not found
        """
        for mod in self.links:
            if mod.name == name:
                return mod

        raise RuntimeError('No link with name "{}" found.'.format(name))

    def has_link(self, name):
        """Check if link with name exists in this chain.

        :param str name: The name of the link to search for
        :returns: boolean answer
        :rtype: bool
        """
        for m in self.links:
            if m.name == name:
                return True

        return False

    def remove_links(self):
        """Remove all links in the chain."""
        for _ in range(0, len(self.links)):
            self.links.pop()

    def remove_link(self, mod):
        """Remove link from this chain.

        :param mod: Link of the chain to remove
        """
        if isinstance(mod, Link):
            a_mod = mod
        elif isinstance(mod, str):
            a_mod = self.get_link(mod)
        else:
            raise ValueError('Chain: link type "{}" not supported'.format(type(mod)))

        try:
            self.links.remove(a_mod)
        except ValueError:
            self.logger.warning('Unable to remove link "{link}" from chain "{chain}"',
                                link=a_mod.name, chain=self.name)
