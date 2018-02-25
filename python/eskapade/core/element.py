"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/02/27

Description:
    Base classes for the building blocks of an Eskapade analysis run:

        - Link:
        - Chain:

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from eskapade.core.definitions import StatusCode
from eskapade.core.meta import Processor, ProcessorSequence
from eskapade.core.mixin import ArgumentsMixin, TimerMixin


class Link(Processor, ArgumentsMixin, TimerMixin):
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
    >>> from eskapade import analysis
    >>>
    >>> # Create a Chain instance. Note that the chain is automatically registered with process manager.
    >>> io_chain = Chain('IO')
    >>>
    >>> # Add a link to the chain
    >>> reader = analysis.ReadToDf(name='CsvReader', key='foo')
    >>> reader.path = 'foo.csv'
    >>> io_chain.add(reader)
    >>>
    >>> # Run everything.
    >>> process_manager.run()
    """

    def __init__(self, name=None):
        """Initialize link."""
        super().__init__(name)

        # read_key is typically the name of the object picked
        # up from the data store read_key can be a string or
        # list of strings.
        self.read_key = None

        # store_key is typically the name of the object put back
        # in the data tore.
        self.store_key = None

        # required kwargs. filled by _process_kwargs
        self._required_vars = []

        # load() return code.
        self.if_input_missing = StatusCode.Failure
        # store() return code.
        self.if_output_exists = StatusCode.Success

    def _process_kwargs(self, kwargs, **name_val):
        """Process the key word arguments.

        :param kwargs: key word arguments to process
        :param str name_val: name of a required kw arg/setting
        """
        super()._process_kwargs(kwargs, **name_val)
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
                self.logger.warning('Some input data did not exist {r!s} {name}', r=r, name=self.name)
                stats.append(self.if_input_missing.value)
                dats.append(None)

        return StatusCode(max(stats)), dats

    def load(self, ds, read_key=None):
        """Read all data from specified source.

        Read_key can either be:

            * one Link: return statuscode, [data_from_link,...]
            * A list of locations: return statuscode, [data,...]
            * A list of links with only one output location: return statuscode, [data,...]
            * A list of links with multiple output locations: return statuscode, [data,[moredata]...]
            * Any mixture of the above

        Do something logical with a statuscode if this data does not exist
        link.if_input_missing = statuscode

        :return: a tuple statuscode, [data in same order as read_key]
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
                self.logger.warning('Link has no store_key {link} {name}.', link=rk.name, name=self.name)
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
                        self.logger.warning('Link has no store_key {link} {name}.', link=r.name, name=self.name)
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
                self.logger.warning('Some input data did not exist {link!s} {name}.', link=r, name=self.name)
                stats.append(self.if_input_missing.value)
                dats.append(None)

        return StatusCode(max(stats)), dats

    def __check_store_loc(self, ds, loc):
        """Check if a location exists.

        If yes, return a status code, if no, print an error and return
        link.if_output_exists

        :return: status code if location exists
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

    def initialize(self) -> StatusCode:
        """Initialize the Link.

        This method may be overridden by the user.

        :return: Status code.
        :type: StatusCode
        """
        return StatusCode.Success

    def execute(self) -> StatusCode:
        """Execute the Link.

        This method may be overridden by the user.

        :return: Status code.
        :rtype: StatusCode
        """
        return StatusCode.Success

    def finalize(self) -> StatusCode:
        """Finalize the Link.

        This method may be overridden by the user.

        :return: Status code.
        :rtype: StatusCode
        """
        return StatusCode.Success


class Chain(Processor, ProcessorSequence, TimerMixin):
    """Execution Chain.

    A Chain object contains a collection of links with analysis code. The
    links in a chain are executed in the order in which the links have been
    added to the chain.  Typically a chain contains all links related to one
    topic, for example 'validation of a model', or 'data preparation', or
    'data quality checks'.

    >>> from eskapade import process_manager
    >>> from eskapade import Chain
    >>> from eskapade import analysis
    >>>
    >>> # Create an IO chain. This is automatically registered with the process manager.
    >>> io_chain = Chain('Overview')

    And Links are added to a chain as follows:

    >>> # add a link to the chain
    >>> io_chain.add(analysis.ReadToDf(path='foo.csv', key='foo'))
    >>>
    >>> # Run everything.
    >>> process_manager.run()
    """

    def __init__(self, name, process_manager=None):
        """Initialize chain."""
        super().__init__(name)

        self.prev_chain_name = ''  # type: str
        self.enabled = True  # type: bool

        # We register ourselves with the process manager.
        # If none is specified register with the default
        # process manager.
        if process_manager is None:
            from eskapade import process_manager

        process_manager.add(self)

    def __exec(self, phase_callback) -> StatusCode:
        status = StatusCode.Success

        for _ in self:
            status = phase_callback(_)

            # A link may fail during initialization, execution, and finalization.
            if status == StatusCode.Failure:
                self.logger.fatal('Link "{link!s}" returned "{code!s}" in chain "{chain!s}"!',
                                  link=_, code=status, chain=self)
                break
            # A link may request to skip the rest of the processing during initialization and execution.
            # Why should a link decide to skip the chain it is in during execution? When an essential input collection is empty.
            elif status == StatusCode.SkipChain:
                self.logger.warning('Skipping chain "{chain!s} as requested by link "{link!s}"!',
                                    chain=self, link=_)
                break
            # A link may request to skip the rest of the execution of the chain (but do perform finalize).
            elif status == StatusCode.BreakChain:
                self.logger.warning('Breaking of exection of chain "{chain!s} as requested by link "{link!s}"!',
                                    chain=self, link=_)
                break
            # A link may request that the chain needs to be repeated during execution. When looping over input file in chunks.
            elif status == StatusCode.RepeatChain:
                self.logger.warning('Repeating chain "{chain!s}" as requested by link "{link!s}"!',
                                    chain=self, link=_)
                break
            # Default, is to log an unhandled status code from the chain.
            elif status != StatusCode.Success:
                self.logger.fatal('Unhandled StatusCode "{status!s}" from link "{link!s}" in "{chain!s}"!',
                                  status=status, link=_, chain=self)
                break

        return status

    def add(self, link: Link) -> None:
        """Add a link to the chain.

        :param link: The link to add to the chain.
        :type link: Link
        :raise TypeError: When the link is of an incompatible type.
        :raise KeyError: When a Link of the same type and name already exists.
        """
        if not issubclass(type(link), Link):
            raise TypeError('Expected (sub-class of) "Link" and not "{wrong!s}"!'.format(wrong=type(link)))

        self.logger.debug('Registering link "{link}."', link=link)

        link.parent = self
        super().add(link)

    # TODO (janos4276) This is a copy past of the Process Manager method. We could/should probably move this to
    # ProcesserSequence.
    def get(self, link_name: str) -> Link:
        """Find the link with the given name.

        :param link_name: Find a link with the given name.
        :type link_name: str
        :return: The chain.
        :rtype: Chain
        :raise ValueError: When the given chain name cannot be found.
        """
        iterator = iter(self)
        link = next(iterator, None)
        while link and link.name != link_name:
            link = next(iterator, None)

        if link is None:
            raise ValueError('Found no chain with name "{name}"!'.format(name=link_name))

        return link

    def discard(self, link: Link) -> None:
        """Remove a link from the chain.

        :param link:
        :type link: Link
        :raise KeyError: When the processor does not exist.
        """
        if not issubclass(type(link), Link):
            raise TypeError('Expected (sub-class of) "Link" not "{wrong!s}"!'.format(wrong=type(link)))

        super().discard(link)
        link.parent = None

    def initialize(self) -> StatusCode:
        """Initialize chain and links.

        :return: Initialization status code.
        :rtype: StatusCode
        """
        self.logger.debug('Initializing chain "{chain!s}".', chain=self)

        self.start_timer()

        status = self.__exec(Processor._initialize)

        if status == StatusCode.Success:
            self.logger.debug('Successfully initialized chain "{chain!s}".', chain=self)

        return status

    def execute(self) -> StatusCode:
        """Execute links in chain.

        :return: Execution status code.
        :rtype: StatusCode
        """
        self.logger.debug('Executing chain "{chain!s}".', chain=self)

        status = self.__exec(Processor._execute)

        if status == StatusCode.Success:
            self.logger.debug('Successfully executed chain "{chain!s}".', chain=self)

        return status

    def finalize(self) -> StatusCode:
        """Finalize links and chain.

        :return: Finalization status code.
        :rtype StatusCode:
        """
        self.logger.debug('Finalizing chain "{chain!s}".', chain=self)

        status = self.__exec(Processor._finalize)

        if status == StatusCode.Success:
            total_time = self.stop_timer()
            self.logger.debug('Successfully finalized chain "{chain!s}".', chain=self)
            self.logger.debug('Chain "{chain!s}" took {sec:.2f} seconds to complete.',
                              chain=self, sec=total_time)

        return status

    def clear(self):
        """Clear the chain."""
        self.parent = None
        super().clear()

    @property
    def n_links(self) -> int:
        """Return the number of links in the chain.

        :return: The number of links in the chain.
        :rtype: int
        """
        return len(self)
