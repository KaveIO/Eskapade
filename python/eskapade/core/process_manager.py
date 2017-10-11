# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Class  : ProcessManager                                                        *
# * Created: 2016/11/08                                                            *
# * Description:                                                                   *
# *      The processManager singleton class forms the core of Eskapade.            *
# *      It performs initialization, execution, and finalizing of the              *
# *      configured chains.                                                        *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************

import glob
import importlib
import os

from eskapade.core import persistence
from eskapade.core.meta import Singleton
from eskapade.core.definitions import StatusCode
from eskapade.core.process_services import ConfigObject
from eskapade.core.process_services import ProcessService
from eskapade.core.element import Chain
from eskapade.logger import Logger
from eskapade.mixins import TimerMixin


class _ProcessManager(TimerMixin, metaclass=Singleton):

    """Eskapade run-process manager.

    The processManager singleton class forms the core of Eskapade.  It
    performs initialization, execution, and finalizing of the configured
    chains.  Chains are added to the process manager (PM) thus:

    >>> from eskapade import process_manager
    >>>
    >>>
    >>> data_chain = Chain('Data')
    >>>
    >>> process_manager.add_chain('Data')
    >>>
    >>> process_manager.add_chain('MyOverview')
    >>>
    >>> process_manager.run()

    The function:

    >>> process_manager.execute_all()

    executes all chains. This function is called by the eskapade_run
    (the executable script of this project).  The chains are executed in
    the order in which they have been added to the PM.  One can begin and
    end the execution at chains specified in the configuration.

    The eskapade_run (the main of this project) script does the
    following:

    * Imports and instantiates processManager
    * Parses and process the cmd line arguments
    * Executes the python configuration macro -> chains and links are defined in the PM
    * Executes the PM

    To be precise, process_manager.execute_all() does the following:

    * initialize():
        For each Chain, set name of previous Chain. Needed to pick up
        correct versions of persisted service instances.
    * execute_all():
        For each added Chain:

        - Initialize Chain: instantiates internal variables, and initialize each Link
        - Execute Chain: execute each Link
        - Finalize Chain: finalize each Link; if setting is true export
          datastore and configurations for each intermediate chain
    * finalize():
        Finalizes execution
    """

    logger = Logger()

    def __init__(self):
        """Initialize process-manager singleton instance.

        The init function takes no arguments.  Chains are added with
        the "add_chain" method.
        """
        TimerMixin.__init__(self)

        self.prevChainName = ''
        self.chains = []
        self._services = {}

    def service(self, serv_spec):
        """Get or register process service.

        :param serv_spec: class (instance) to register
        :type serv_spec: ProcessServiceMeta or ProcessService
        :returns: registered instance
        :rtype: ProcessService
        """
        # get service class and register if an instance of class was specified
        cls = serv_spec
        if isinstance(serv_spec, ProcessService):
            # service instance specified; get its class
            cls = type(serv_spec)

            # if already registered, check if specified instance is the registered instance
            if serv_spec is not self._services.get(cls, serv_spec):
                raise ValueError('specified service is not the instance that was registered earlier')

            # register service if not registered yet
            if cls not in self._services:
                self._services[cls] = serv_spec

        # create and register service if it is not registered yet
        if cls not in self._services:
            # check if service class is derived from ProcessService
            if not isinstance(cls, type) or not issubclass(cls, ProcessService):
                raise TypeError('specified service type does not derive from ProcessService')

            # create and register instance
            self._services[cls] = cls.create()

        # return registered instance
        return self._services[cls]

    def get_services(self):
        """Get set of registered process-service classes.

        :returns: service set
        :rtype: set
        """
        return set(self._services)

    def get_service_tree(self):
        """Create tree of registered process-service classes.

        :returns: service tree
        :rtype: dict
        """
        # build service tree
        serv_tree = {}
        for serv_cls in self.get_services():
            # add path of service to tree
            serv_path = serv_tree
            for comp in serv_cls.__module__.split('.'):
                if comp not in serv_path:
                    serv_path[comp] = {'-services-': set()}
                serv_path = serv_path[comp]

            # add service to path
            serv_path['-services-'].add(serv_cls)

        return serv_tree

    def remove_service(self, serv_cls, silent=False):
        """Remove specified process service.

        :param serv_cls: service to remove
        :type serv_cls: ProcessServiceMeta
        :param bool silent: don't complain if service is not registered
        """
        # check if specified service is registered
        if serv_cls not in self._services:
            if not silent:
                self.logger.fatal('No such service registered: "{cls!s}".', cls=serv_cls)
                raise KeyError('Service to be removed not registered.')
            return

        # finish running and remove service
        self.logger.debug('Removing process service "{service!s}".', service=self._services[serv_cls])
        self._services[serv_cls].finish()
        del self._services[serv_cls]

    def remove_all_services(self):
        """Remove all registered process services."""
        # finish running and remove all services
        self.logger.debug('Removing all process services ({n:d})', n=len(self._services))
        for serv in self._services.values():
            serv.finish()
        self._services.clear()

    @staticmethod
    def check_io_config(io_conf):
        """Check I/O config and set name/version if not specified.

        :param dict io_conf: I/O config to check
        """
        io_conf = persistence.IoConfig(**io_conf)
        if not io_conf['analysis_name']:
            io_conf['analysis_name'] = 'default'
        if not io_conf['analysis_version']:
            io_conf['analysis_version'] = '0'
        return io_conf

    def import_services(self, io_conf, chain=None, force=None, no_force=[]):
        """Import process services from files.

        :param dict io_conf: I/O config as returned by ConfigObject.io_conf
        :param str chain: name of chain for which data was persisted
        :param force: force import if service already registered
        :type force: bool or list
        :param list no_force: do not force import of services in this list
        """
        # parse I/O config
        io_conf = self.check_io_config(io_conf)

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
        serv_paths = glob.glob('{0:s}/{1:s}/*.pkl'.format(base_path, chain))
        self.logger.debug('Importing process services from "{path}/{chain}" (found {n:d} files).',
                          path=base_path, chain=chain, n=len(serv_paths))

        # read and register services
        for path in serv_paths:
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
        io_conf = self.check_io_config(io_conf)

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
        serv_paths = glob.glob('{}/*.pkl'.format(chain_path))
        try:
            for path in serv_paths:
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
        :raises Exception: if input configuration file cannot be found
        """
        if not os.path.isfile(filename):
            raise Exception(
                'ERROR. Configuration macro \'{}\' not found.'.format(filename))
        exec(compile(open(filename).read(), filename, 'exec'))

        # make copy of macro for bookkeeping purposes
        settings = self.service(ConfigObject)
        if not settings.get('doNotStoreResults') and copyfile:
            import shutil
            shutil.copy(filename, persistence.io_dir('results_config', settings.io_conf()))

    def add_chain(self, input_chain):
        """Add a chain to the process manager.

        Add a chain to be run by the process manager.  A check is
        performed that a chain with this name does not already
        exist.

        :param input_chain: (name of) the chain to be added
        :type input_chain: str or Chain
        :raises RuntimeError: if chain with same name already exists
        :returns: the chain that has been added
        :rtype: Chain
        """
        # first check specified chain name
        if isinstance(input_chain, Chain):
            new_name = input_chain.name
        elif isinstance(input_chain, str):
            new_name = input_chain
        else:
            self.logger.fatal('Specifying chain by type "{type}" not supported.', type=type(input_chain).__name__)
            raise NotImplementedError('Unsupported input type for add_chain function of process manager.')
        if any(c.name == new_name for c in self.chains):
            self.logger.fatal('Chain "{name}" already exists; please use a different name.', name=new_name)
            raise RuntimeError('Tried to add chain with existing name to process manager.')

        # add new chain
        self.logger.debug('Booking new chain "{name}".', name=new_name)
        self.chains.append(input_chain if isinstance(input_chain, Chain) else Chain(new_name))

        return self.chains[-1]

    def get_chain_idx(self, name):
        """Find index of the chain with given name.

        :param str name: The name of the chain to search for
        :returns: the index of the chain
        :rtype: int
        :raises Exception: if chain name not found
        """
        for (idx, chain) in enumerate(self.chains):
            if chain.name == name:
                return idx

        raise Exception('No chain with name "{}" found.'.format(name))

    def get_chain(self, name):
        """Find the chain with the given name.

        :param str name: The name to search for
        :returns: the found chain
        :rtype: Chain
        :raises RuntimeError: if chain name not found
        """
        for chain in self.chains:
            if chain.name == name:
                return chain

        raise RuntimeError('No chain with name "{}" found.'.format(name))

    def has_chain(self, name):
        """Check if chain exists for this name.

        :param str name: the name of the chain to check
        :returns: boolian of search result
        :rtype: bool
        """
        for c in self.chains:
            if c.name == name:
                return True

        return False

    def remove_chains(self) -> None:
        """Remove all configured chains."""
        # Remove links from each chain.
        [_.clear() for _ in self.chains]

        # Remove all chains.
        self.chains.clear()

    def remove_chain(self, name: str) -> None:
        """Remove chain with specified name.

        Remove specified chain. If chain is not found, print warning.

        :param str name: Name of the chain to remove
        """
        chain_to_pop = None
        for c_i, c in enumerate(self.chains):
            if c.name == name:
                chain_to_pop = c_i
                break

        if chain_to_pop is not None:
            chain = self.chains.pop(chain_to_pop)
            chain.clear()
        else:
            self.logger.warning('Trying to remove non-existing chain {chain} or there are no chains!', chain=name)

    def initialize(self):
        """Initialize the process manager.

        Initializes the process manager by configuring its chains.
        After initialization the configuration is printed.

        :returns: status code of initialize attempt
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
        for chain in self.chains:
            self.logger.debug('Configuring chain "{name}".', name=chain.name)
            if not chain.prevChainName:
                chain.prevChainName = prev_chain_name
            prev_chain_name = chain.name

        # End by print the status of the processManager and the configuration
        # object.
        self.Print()
        self.service(ConfigObject).Print()

        self.logger.debug('Done initializing process manager.')

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

    def Print(self):
        """Print process-manager summary.

        Print a summary of the chains, links, and some analysis settings
        defined in this configuration.
        """
        settings = self.service(ConfigObject)

        self.logger.info('Summary of process manager')
        if settings.get('beginWithChain'):
            self.logger.info('  Starting from chain: "{name}"', name=settings['beginWithChain'])
        if settings.get('endWithChain'):
            self.logger.info('  Ending with chain:   "{name}"', name=settings['endWithChain'])

        self.logger.info('  Number of registered services: {n:d}', n=len(self._services))
        self.print_services()

        self.logger.info('  Number of registered chains: {n:d}', n=len(self.chains))
        self.print_chains()

    def print_services(self):
        """Print registered process services."""
        def _print_level(level, prefix, depth):
            """Print services and get next level in service-tree."""
            # print service names on this level
            indent = ' ' * 2 * depth
            for serv_name in sorted(cls.__name__ for cls in level.pop('-services-', [])):
                self.logger.debug('{prefix:s}{indent:s}+ {service:s}', prefix=prefix, indent=indent, service=serv_name)

            # print next levels
            for lev_path in sorted(level):
                self.logger.debug('{prefix:s}{indent:s}- {level:s}', prefix=prefix, indent=indent, level=lev_path)
                _print_level(level[lev_path], prefix, depth + 1)

        # print service tree
        serv_tree = self.get_service_tree()
        self.logger.debug('  Registered process services')
        _print_level(serv_tree, '    ', 0)

    def print_chains(self):
        """Print all chains defined in the manager."""
        settings = self.service(ConfigObject)
        self.logger.debug('  Chains to be executed')

        begin = self.get_chain_idx(settings['beginWithChain']) if settings.get('beginWithChain') else 0
        end = (self.get_chain_idx(settings['endWithChain']) + 1) if settings.get('endWithChain') else len(self.chains)
        for chain in self.chains[begin:end]:
            self.logger.debug('    Chain: {name}', name=chain.name)
            for link in chain.links:
                self.logger.debug('      Link: {name}', name=link.name)

    def execute_all(self):
        """Execute all chains in order.

        :returns: status code of execution attempt
        :rtype: StatusCode
        """
        status = StatusCode.Success

        self.logger.info('Executing process manager.')

        settings = self.service(ConfigObject)

        # determine which chains need to be run
        begin = self.get_chain_idx(settings['beginWithChain']) if settings.get('beginWithChain') else 0
        end = (self.get_chain_idx(settings['endWithChain']) + 1) if settings.get('endWithChain') else len(self.chains)

        if begin > 0:
            # import services from previous chain, persisted in a previous run
            try:
                self.import_services(io_conf=settings.io_conf(), chain=self.chains[begin].prevChainName, force=False)
            except Exception as exc:
                self.logger.error('Unable to import services persisted for "{chain}":',
                                  chain=self.chains[begin].prevChainName)
                self.logger.error('Caught exception: "{exc!s}".', exc=exc)
                return StatusCode.Failure

        # execute chains
        for chain in self.chains[begin:end]:
            # execute chain and check exit status
            status = self.execute(chain)
            chain.exitStatus = status
            if status.is_failure():
                return status

            # check if we need to persist process services
            if settings.get('doNotStoreResults'):
                # never persist anything
                continue
            if not (settings.get('storeResultsEachChain') or chain == self.chains[end - 1] or
                    settings.get('storeResultsOneChain') == chain.name):
                # do not persist the output of this chain
                continue

            # persist process services with the output of this chain
            self.persist_services(io_conf=settings.io_conf(), chain=chain.name)

        self.logger.debug('Done executing process manager.')

        return status

    def execute(self, chain):
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
            self.prevChainName = chain.name
            return status

        # execute() of a chain can be called to be repeated.
        # Note: by default this is not done. i.e. chains are only executed once
        status = StatusCode.RepeatChain
        while status.is_repeat_chain():
            status = chain.execute()
        if status.is_failure():
            return status
        elif status.is_skip_chain():
            self.prevChainName = chain.name
            return status

        # finalize.
        status = chain.finalize()
        if status.is_failure():
            return status

        # After execution of chain prevChainName is set here so the stores will not be imported from file in
        # but retrieved from memory in the execution of next chain.
        self.prevChainName = chain.name

        return status

    def reset(self):
        """Reset the process manager.

        Resetting comprises removing the chains and closing any open
        connections/sessions.
        """
        # remove process services
        self.remove_all_services()

        # remove chains
        self.remove_chains()

        # delete attributes
        for key in list(vars(self).keys()):
            delattr(self, key)

        # re-initialize
        self.__init__()


process_manager = _ProcessManager()
