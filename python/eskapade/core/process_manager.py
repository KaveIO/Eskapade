"""Project: Eskapade - A python-based package for data analysis.

Class: ProcessManager

Created: 2016/11/08

Description:
    The ProcessManager class is the heart of Eskapade.
    It performs initialization, execution, and finalization of
    analysis chains.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import glob
import importlib
import os

from eskapade.core import persistence
from eskapade.core.definitions import StatusCode
from eskapade.core.element import Chain
from eskapade.core.meta import Processor, ProcessorSequence
from eskapade.core.mixin import TimerMixin
from eskapade.core.process_services import ConfigObject, ProcessService


class ProcessManager(Processor, ProcessorSequence, TimerMixin):
    """Eskapade run process manager.

    ProcessManager is the event processing loop of Eskapade.
    It initializes, executes, and finalizes the analysis chains.
    There is, under normal circumstances, only one ProcessManager
    instance.

    Here's an simple but illustrative analysis example:

    >>> from eskapade import process_manager, Chain, Link, StatusCode
    >>>
    >>> # A chain automatically registers itself with process_manager.
    >>> one_plus_one_chain = Chain('one_plus_one')
    >>>
    >>> class OnePlusOne(Link):
    >>>     def execute(self):
    >>>         self.logger.info('one plus one = {result}', result=(1+1))
    >>>         return StatusCode.Success
    >>>
    >>> one_plus_one_chain.add(link=OnePlusOne())
    >>>
    >>> two_plus_two_chain = Chain('two_plus_two')
    >>>
    >>> class TwoPlusTwo(Link):
    >>>     def execute(self):
    >>>         self.logger.info('two plus two = {result}', result=(2+2))
    >>>         return StatusCode.Success
    >>>
    >>> two_plus_two_chain.add(TwoPlusTwo())
    >>>
    >>> process_manager.run()

    Ideally the user will not need to interact directly with the process manager.
    The magic is taken care of by the eskapade_run entry point.
    """

    def __init__(self):
        """Initialize ProcessManager instance."""
        super().__init__(ProcessManager.__name__)

        self.prev_chain_name = ''
        self._services = {}

    def service(self, service_spec):
        """Get or register process service.

        :param service_spec: class (instance) to register
        :type service_spec: ProcessServiceMeta or ProcessService
        :return: registered instance
        :rtype: ProcessService
        """
        # get service class and register if an instance of class was specified
        cls = service_spec
        if isinstance(service_spec, ProcessService):
            # service instance specified; get its class
            cls = type(service_spec)

            # if already registered, check if specified instance is the registered instance
            if service_spec is not self._services.get(cls, service_spec):
                raise ValueError('Specified service is not the instance that was registered earlier.')

            # register service if not registered yet
            if cls not in self._services:
                self._services[cls] = service_spec

        # create and register service if it is not registered yet
        if cls not in self._services:
            # check if service class is derived from ProcessService
            if not isinstance(cls, type) or not issubclass(cls, ProcessService):
                raise TypeError('Specified service type does not derive from ProcessService.')

            # create and register instance
            self._services[cls] = cls.create()

        # return registered instance
        return self._services[cls]

    def get_services(self):
        """Get set of registered process-service classes.

        :return: service set
        :rtype: set
        """
        return set(self._services)

    def get_service_tree(self):
        """Create tree of registered process-service classes.

        :return: service tree
        :rtype: dict
        """
        # build service tree
        service_tree = {}
        for service_cls in self.get_services():
            # add path of service to tree
            service_path = service_tree
            for comp in service_cls.__module__.split('.'):
                if comp not in service_path:
                    service_path[comp] = {'-services-': set()}
                service_path = service_path[comp]

            # add service to path
            service_path['-services-'].add(service_cls)

        return service_tree

    def remove_service(self, service_cls, silent=False):
        """Remove specified process service.

        :param service_cls: service to remove
        :type service_cls: ProcessServiceMeta
        :param bool silent: don't complain if service is not registered
        """
        # check if specified service is registered
        if service_cls not in self._services:
            if not silent:
                self.logger.fatal('No such service registered: "{cls!s}".', cls=service_cls)
                raise KeyError('Service to be removed not registered.')
            return

        # finish running and remove service
        self.logger.debug('Removing process service "{service!s}".', service=self._services[service_cls])
        self._services[service_cls].finish()
        del self._services[service_cls]

    def remove_all_services(self):
        """Remove all registered process services."""
        # finish running and remove all services
        self.logger.debug('Removing all process services ({n:d})', n=len(self._services))
        for service in self._services.values():
            service.finish()
        self._services.clear()

    def import_services(self, io_conf, chain=None, force=None, no_force=None):
        """Import process services from files.

        :param dict io_conf: I/O config as returned by ConfigObject.io_conf
        :param str chain: name of chain for which data was persisted
        :param force: force import if service already registered
        :type force: bool or list
        :param list no_force: do not force import of services in this list
        """
        no_force = no_force or []

        # parse I/O config
        io_conf = ConfigObject.IoConfig(**io_conf)

        # get services for which import may be forced
        force_set = set()
        if force:
            try:
                # check if an iterable of forced services was provided
                force_set.update(force)
            except TypeError:
                # force all services if "force" was provided, but is not iterable
                force_set.update(self.get_services())
        force_set -= set(no_force)

        # parse specified chain
        if chain:
            # prepend underscore for output directory
            chain = '_{}'.format(chain)
        else:
            # use data from latest chain if not specified
            chain = 'latest'

        # get list of persisted files
        base_path = persistence.io_dir('proc_service_data', io_conf)
        service_paths = glob.glob('{0:s}/{1:s}/*.pkl'.format(base_path, chain))
        self.logger.debug('Importing process services from "{path}/{chain}" (found {n:d} files).',
                          path=base_path, chain=chain, n=len(service_paths))

        # read and register services
        for path in service_paths:
            try:
                # try to import service module
                cls_spec = os.path.splitext(os.path.basename(path))[0].split('.')
                mod = importlib.import_module('.'.join(cls_spec[:-1]))
                cls = getattr(mod, cls_spec[-1])
            except Exception as exc:
                # unable to import module
                self.logger.error('Unable to import process-service module for path "{path}".', path=path)
                raise exc

            # check if service is already registered
            if cls in self.get_services():
                if cls in force_set:
                    # remove old service instance if import is forced
                    self.remove_service(cls)
                else:
                    # skip import if not forced
                    self.logger.debug('Service "{cls!s}" already registered; skipping import of "{path}"',
                                      cls=cls, path=path)
                    continue

            # read service instance from file
            inst = cls.import_from_file(path)
            if inst:
                self.service(inst)

    def persist_services(self, io_conf, chain=None):
        """Persist process services in files.

        :param dict io_conf: I/O config as returned by ConfigObject.io_conf
        :param str chain: name of chain for which data is persisted
        """
        # parse I/O config
        io_conf = ConfigObject.IoConfig(**io_conf)

        # parse specified chain
        if chain:
            # prepend underscore for output directory
            chain = '_{}'.format(chain)
        else:
            # use default directory if chain not specified
            chain = 'default'

        # get chain path and set link of latest data
        base_path = persistence.io_dir('proc_service_data', io_conf)
        chain_path = '{0:s}/{1:s}'.format(base_path, chain)
        persistence.create_dir(chain_path)
        self.logger.debug('Persisting process services in "{path}".', path=chain_path)
        try:
            # remove old symlink
            os.remove('{}/latest'.format(base_path))
        except OSError:
            pass
        try:
            # create new symlink
            os.symlink(chain, '{}/latest'.format(base_path))
        except OSError as exc:
            self.logger.fatal('Unable to create symlink to latest version of services: <{path}/latest>.',
                              path=base_path)
            raise exc

        # remove old data
        service_paths = glob.glob('{}/*.pkl'.format(chain_path))
        try:
            for path in service_paths:
                os.remove(path)
        except Exception as exc:
            self.logger.fatal('Unable to remove previously persisted process services.')
            raise exc

        # persist services
        for cls in self.get_services():
            self.service(cls).persist_in_file('{0:s}/{1!s}.pkl'.format(chain_path, cls))

    def execute_macro(self, filename, copyfile=True):
        """Execute an input python configuration file.

        A copy of the configuration file is stored for bookkeeping purposes.

        :param str filename: the path of the python configuration file
        :param bool copyfile: back up the macro for bookkeeping purposes
        :raise Exception: if input configuration file cannot be found
        """
        if not os.path.isfile(filename):
            raise Exception(
                'ERROR. Configuration macro \'{}\' not found.'.format(filename))
        exec(compile(open(filename).read(), filename, 'exec'))

        # make copy of macro for bookkeeping purposes
        settings = self.service(ConfigObject)
        if not settings.get('doNotStoreResults') and copyfile:
            import shutil
            shutil.copy(filename, persistence.io_dir('results_config'))

    def add(self, chain: Chain) -> None:
        """Add a chain to the process manager.

        :param chain: The chain to add to the process manager.
        :type chain: Chain
        :raise TypeError: When the chain is of an incompatible type.
        :raise KeyError: When a chain of the same type and name already exists.
        """
        if not issubclass(type(chain), Chain):
            raise TypeError('Expected (sub-class of) "Chain" and not "{wrong!s}"!'.format(wrong=type(chain)))

        self.logger.debug('Registering chain "{chain}."', chain=chain)

        chain.parent = self
        super().add(chain)

    def get(self, chain_name: str) -> Chain:
        """Find the chain with the given name.

        :param chain_name: Find a chain with the given name.
        :type chain_name: str
        :return: The chain.
        :rtype: Chain
        :raise ValueError: When the given chain name cannot be found.
        """
        iterator = iter(self)
        chain = next(iterator, None)
        while chain and chain.name != chain_name:
            chain = next(iterator, None)

        if chain is None:
            raise ValueError('Found no chain with name "{name}"!'.format(name=chain_name))

        return chain

    def clear(self):
        """"Clear/remove all chains."""
        # Clear chains first.
        [_.clear() for _ in self]

        # Clear self.
        super().clear()

    def __disable(self, chain_name: str, after: bool = False) -> Chain:
        """Disable chains before/after a chain with name chain_name.

        :return: The chain with name chain_name.
        :rtype: Chain
        """
        try:
            iterator = iter(self) if not after else reversed(self)
            chain = next(iterator, None)  # type: Chain
            tmp = self.get(chain_name)
        except ValueError as e:
            raise e

        while chain and chain != tmp:
            chain.enabled = False
            chain = next(iterator, None)

        return tmp

    def initialize(self):
        """Initialize the process manager.

        Initializes the process manager by configuring its chains.
        After initialization the configuration is printed.

        :return: status code of initialize attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.logger.info('Initializing process manager.')

        # Start the timer directly after the initialize message.
        self.start_timer()

        # For each chain, set name of prev chain in loop.
        # This is needed to pick up the correct process services in case
        # we start the chain-loop at different starting chain.
        # First chain has empty previous-chain name.

        prev_chain_name = ''
        for c in self:
            self.logger.debug('Configuring chain "{name}".', name=c.name)
            if prev_chain_name:
                c.prev_chain_name = prev_chain_name
            prev_chain_name = c.name

        # Disable chains that do not need to be executed.
        settings = self.service(ConfigObject)
        begin_chain = settings.get('beginWithChain', None)
        if begin_chain:
            chain = self.__disable(begin_chain)

            # Import services from previous chain persisted in a previous run.
            try:
                self.import_services(io_conf=settings.io_conf(), chain=chain.prev_chain_name, force=False)
            except Exception as exc:
                self.logger.error('Unable to import services persisted for "{chain}":',
                                  chain=chain.prev_chain_name)
                self.logger.error('Caught exception: "{exc}".', exc=exc)
                return StatusCode.Failure

        # This pretty much the same as above, except that we do it in reverse.
        end_chain = settings.get('endWithChain', None)
        if end_chain:
            self.__disable(end_chain, True)

        # Print the run configuration
        self.summary()
        settings.Print()

        self.logger.debug('Done initializing process manager.')

        return status

    def __exec(self, chain):
        """Execute a particular chain.

        Execution of a chain comprises:

        * Initialize Chain:
            - Instantiates internal variables
            - Initialize each Link
        * Execute Chain:
            - Execute each Link
        * Finalize Chain:
            - Finalize each Link
            - If setting is true, export datastore and configurations for
              each intermediate chain

        :param chain: The chain to execute
        :returns: status code of execution attempt
        :rtype: StatusCode
        """
        #  first initialize
        status = chain.initialize()
        if status.is_failure():
            return status
        elif status.is_skip_chain():
            self.prev_chain_name = chain.name
            return status

        # execute() of a chain can be called to be repeated.
        # Note: by default this is not done. i.e. chains are only executed once
        status = StatusCode.RepeatChain
        while status.is_repeat_chain():
            self.logger.debug('Executing chain={chain}', chain=chain.name)
            status = chain.execute()
        if status.is_failure():
            return status
        elif status.is_break_chain():
            # skip rest of chain execution, go to finalize
            pass
        elif status.is_skip_chain():
            self.prev_chain_name = chain.name
            return status

        # finalize.
        status = chain.finalize()
        if status.is_failure():
            return status

        # After execution of chain prev_chain_name is set here so
        # the stores will not be imported from file in
        # but retrieved from memory in the execution of next chain.
        self.prev_chain_name = chain.name

        return status

    def execute(self):
        """Execute all chains in order.

        :return: status code of execution attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.logger.info('Executing process manager.')

        settings = self.service(ConfigObject)

        # execute chains
        last_chain = None
        persist_results = settings.get('storeResultsEachChain')

        for chain in self:
            if chain.enabled:
                # execute chain and check exit status
                status = self.__exec(chain)
                chain.exitStatus = status
                if status == StatusCode.Failure:
                    break

                # check if we need to persist process services
                if settings.get('doNotStoreResults'):
                    # never persist anything
                    continue

                persist_results = persist_results or (settings.get('storeResultsOneChain') == chain.name)
                if not persist_results:
                    last_chain = chain
                    continue

                # persist process services with the output of this chain
                self.persist_services(io_conf=settings.io_conf(), chain=chain.name)
                last_chain = None

        # TODO (janos4276) I don't like this. We need to rethink this.
        # We need to find a better way of persisting or specifying options
        # for persistence. Maybe persistence should not be part of execute?
        # Maybe we should do it in finalize?
        if last_chain:
            # persist process services with the output of the last executed chain
            self.persist_services(io_conf=settings.io_conf(), chain=last_chain.name)

        self.logger.debug('Done executing process manager.')

        return status

    def finalize(self):
        """Finalize the process manager manager.

        :returns: status code of finalize attempt
        :rtype: StatusCode
        """
        self.logger.info('Finalizing process manager.')

        # Stop the timer when the Process Manager is done and print.
        total_time = self.stop_timer()
        self.logger.info('Total runtime: {time:.2f} seconds', time=total_time)

        self.logger.debug('Done finalizing process manager.')

        return StatusCode.Success

    def summary(self):
        """Print process-manager summary.

        Print a summary of the chains, links, and some analysis settings
        defined in this configuration.
        """
        self.logger.info('ProcessManager:')
        self.logger.info('Number of registered services: {n:d}', n=len(self._services))
        self.print_services()
        self.logger.info('Number of registered chains: {n:d}', n=self.n_chains)
        self.print_chains()

    def print_services(self):
        """Print registered process services."""
        def _print_level(level, prefix, depth):
            """Print services and get next level in service-tree."""
            # print service names on this level
            indent = ' ' * 2 * depth
            for service_name in sorted(cls.__name__ for cls in level.pop('-services-', [])):
                self.logger.debug('{prefix:s}{indent:s}+ {service:s}', prefix=prefix, indent=indent,
                                  service=service_name)

            # print next levels
            for lev_path in sorted(level):
                self.logger.debug('{prefix:s}{indent:s}- {level:s}', prefix=prefix, indent=indent, level=lev_path)
                _print_level(level[lev_path], prefix, depth + 1)

        # print service tree
        service_tree = self.get_service_tree()
        self.logger.debug('  Registered process services')
        _print_level(service_tree, '    ', 0)

    def print_chains(self):
        """Print all chains defined in the manager."""
        self.logger.debug('  Chains to be executed')
        for chain in self:
            self.logger.debug('    Chain: {name} Enabled: {enabled}', name=chain.name, enabled=chain.enabled)
            for link in chain:
                self.logger.debug('      Link: {name}', name=link.name)

    def reset(self):
        """Reset the process manager.

        Resetting comprises removing the chains and closing any open
        connections/sessions.
        """
        # remove process services
        self.remove_all_services()
        # remove chains
        self.clear()
        # Why do we delete the attributes?
        # delete attributes
        [delattr(self, _) for _ in list(vars(self).keys())]
        self.__init__()

    def run(self) -> StatusCode:
        """Run process manager.

        :return: Status code of run execution.
        :rtype: StatusCode
        """
        # Initialize
        status = self.initialize()

        # Execute
        profiler = None
        if status == StatusCode.Success:
            settings = self.service(ConfigObject)
            profile_code = settings.get('doCodeProfiling', False)
            if profile_code:
                from cProfile import Profile
                profiler = Profile()
                profiler.enable()

            status = self.execute()

            if profiler:
                profiler.disable()

        # Finalize
        if status == StatusCode.Success:
            status = process_manager.finalize()
            # Profiling output
            if profiler:
                import io
                import pstats
                profile_output = io.StringIO()
                profile_stats = pstats.Stats(profiler, stream=profile_output).sort_stats('time')
                profile_stats.print_stats()
                self.logger.info('Profiling Statistics (sorted by time):\n{stats}', stats=profile_output.getvalue())

        return status

    @property
    def n_chains(self) -> int:
        """Return the number of chains in the process manager.

        :return: The number of links in the chain.
        :rtype: int
        """
        return len(self)


process_manager = ProcessManager()
