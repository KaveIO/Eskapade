# **********************************************************************************
# * Project: Eskapade - A python-based package for data analysis                   *
# * Created: 2017/02/27                                                            *
# * Description:                                                                   *
# *      Definitions used in Eskapade runs:                                        *
# *        * logging levels                                                        *
# *        * return-status codes                                                   *
# *        * default configuration variables                                       *
# *        * user options                                                          *
# *                                                                                *
# * Authors:                                                                       *
# *      KPMG Big Data team, Amstelveen, The Netherlands                           *
# *                                                                                *
# * Redistribution and use in source and binary forms, with or without             *
# * modification, are permitted according to the terms listed in the file          *
# * LICENSE.                                                                       *
# **********************************************************************************


from enum import Enum
import logging
import collections
import ast

# dummy logging level to turn off logging.
logging.OFF = 60

LOG_LEVELS = {
    'NOTSET': logging.NOTSET,
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'FATAL': logging.FATAL,
    'CRITICAL': logging.CRITICAL,
    'OFF': logging.OFF,
    logging.DEBUG: 'DEBUG',
    logging.INFO: 'INFO',
    logging.WARNING: 'WARNING',
    logging.ERROR: 'ERROR',
    logging.FATAL: 'FATAL',
    logging.CRITICAL: 'CRITICAL',
    logging.NOTSET: 'NOTSET',
    logging.OFF: 'OFF'
}


class StatusCode(Enum):
    """Return-status codes for Eskapade run

    A StatusCode object is returned after each initialize, execute, and
    finalize function call of links, chains, and the process manager.
    """

    # all okay
    Success = 1
    # not okay, but can continue
    Recoverable = 2
    # skip this chain
    SkipChain = 3
    # failure means quit
    Failure = 4
    # repeat this chain
    RepeatChain = 5
    # undefined = default status
    Undefined = 6

    def isSuccess(self):
        """Check if status is "Success"

        :rtype: bool
        """
        return StatusCode.Success == self

    def isRecoverable(self):
        """Check if status is "Recoverable"

        :rtype: bool
        """
        return StatusCode.Recoverable == self

    def isSkipChain(self):
        """Check if status is "SkipChain"

        :rtype: bool
        """
        return StatusCode.SkipChain == self

    def isRepeatChain(self):
        """Check if status is "RepeatChain"

        :rtype: bool
        """
        return StatusCode.RepeatChain == self

    def isFailure(self):
        """Check if status is "Failure"

        :rtype: bool
        """
        return StatusCode.Failure == self

    def isUndefined(self):
        """Check if status is "Undefined"

        :rtype: bool
        """
        return StatusCode.Undefined == self


class RandomSeeds:
    """Container for seeds of random generators

    Seeds are stored as key-value pairs and are accessed with getitem and
    setitem methods.  A default seed can be accessed with the key "default".
    The default seed is also returned if no seed is set for the specified
    key.

    >>> seeds = RandomSeeds(default=999, foo=42, bar=13)
    >>> seeds['NumPy'] = 100
    >>> np.random.seed(seeds['NumPy'])
    >>> print(seeds['nosuchseed'])
    999
    """

    def __init__(self, **kwargs):
        """Initialize RandomSeeds instance

        Values of the specified keyword arguments must be integers, which are
        set as seed values for the corresponding key.
        """

        # initialize attributes
        self._seeds = {}
        self._default = 1

        # set specified seeds
        for key, seed in kwargs.items():
            self[key] = seed

    def __getitem__(self, key):
        """Return seed for specified lowercase-string key"""

        return self._seeds.get(str(key).strip().lower(), self._default)

    def __setitem__(self, key, seed):
        """Set integer seed for specified lowercase-string key"""

        # parse key and seed
        key = str(key).strip().lower()
        try:
            seed = int(seed)
        except:
            raise TypeError('specified seed for key "{0:s}" is not an integer: "{1:s}"'.format(key, str(seed)))

        # check if this is the default key
        if key == 'default':
            self._default = seed
        else:
            self._seeds[key] = seed

    def __str__(self):
        seed_str = ', '.join('{0:s}: {1:d}'.format(*kv) for kv in self._seeds.items())
        return '{{default: {0:d} | {1:s}}}'.format(self._default, seed_str)


# configuration variables
CONFIG_VARS = collections.OrderedDict()
CONFIG_VARS['run'] = ['analysisName', 'version', 'macro', 'batchMode', 'interactive', 'logLevel', 'logFormat',
                      'doCodeProfiling']
CONFIG_VARS['chains'] = ['beginWithChain', 'endWithChain', 'storeResultsEachChain', 'storeResultsOneChain',
                         'doNotStoreResults']
CONFIG_VARS['file_io'] = ['esRoot', 'resultsDir', 'dataDir', 'macrosDir', 'templatesDir']
CONFIG_VARS['config'] = ['sparkCfgFile']
CONFIG_VARS['db_io'] = ['all_mongo_collections']
CONFIG_VARS['rand_gen'] = ['seeds']
CONFIG_TYPES = dict(version=int, batchMode=bool, interactive=bool, storeResultsEachChain=bool, doNotStoreResults=bool,
                    all_mongo_collections=list)
CONFIG_DEFAULTS = dict(version=0, batchMode=True, interactive=False, logLevel=logging.INFO,
                       logFormat='%(asctime)s %(levelname)s [%(module)s]: %(message)s',
                       doCodeProfiling=None, storeResultsEachChain=False, doNotStoreResults=False, esRoot='',
                       resultsDir='results', dataDir='data', macrosDir='tutorials', templatesDir='templates',
                       sparkCfgFile='spark.cfg', seeds=RandomSeeds())

# user options in command-line arguments
USER_OPTS = collections.OrderedDict()
USER_OPTS['run'] = ['analysis_name', 'analysis_version', 'batch_mode', 'interactive', 'log_level', 'log_format',
                    'unpickle_config', 'profile', 'conf_var']
USER_OPTS['chains'] = ['begin_with', 'end_with', 'single_chain', 'store_all', 'store_one', 'store_none']
USER_OPTS['file_io'] = ['results_dir', 'data_dir', 'macros_dir', 'templates_dir']
USER_OPTS['config'] = ['spark_cfg_file']
USER_OPTS['rand_gen'] = ['seed']
USER_OPTS_SHORT = dict(analysis_name='n', analysis_version='v', interactive='i', log_level='L', conf_var='c',
                       begin_with='b', end_with='e', single_chain='s')
USER_OPTS_KWARGS = dict(analysis_name=dict(help='set name of analysis in run',
                                           metavar='NAME'),
                        analysis_version=dict(help='set version of analysis version in run',
                                              type=int,
                                              metavar='VERSION'),
                        batch_mode=dict(help='run in batch mode (no X Windows)',
                                        action='store_true'),
                        interactive=dict(help='start IPython shell after run',
                                         action='store_true'),
                        log_level=dict(help='set logging level',
                                       choices=['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'FATAL', 'OFF'],
                                       metavar='{NOTSET,DEBUG,INFO,WARNING,ERROR,FATAL,OFF}'),
                        log_format=dict(help='set log-message format',
                                        metavar='FORMAT'),
                        unpickle_config=dict(help='interpret first CONFIG_FILE as path to pickled settings',
                                             action='store_true'),
                        profile=dict(help='run Python profiler, sort output by specified column',
                                     choices=['stdname', 'nfl', 'pcalls', 'file', 'calls', 'time', 'line',
                                              'cumulative', 'module', 'name'],
                                     metavar='{stdname,nfl,pcalls,file,calls,time,line,cumulative,module,name}'),
                        conf_var=dict(help='set configuration variable',
                                      action='append',
                                      metavar='KEY=VALUE'),
                        begin_with=dict(help='begin execution with chain CHAIN_NAME',
                                        metavar='CHAIN_NAME'),
                        end_with=dict(help='end execution with chain CHAIN_NAME',
                                      metavar='CHAIN_NAME'),
                        single_chain=dict(help='only execute chain CHAIN_NAME',
                                          metavar='CHAIN_NAME'),
                        store_all=dict(help='store run-process services after every chain',
                                       action='store_true'),
                        store_one=dict(help='store run-process services after chain CHAIN_NAME',
                                       metavar='CHAIN_NAME'),
                        store_none=dict(help='do not store run-process services',
                                        action='store_true'),
                        results_dir=dict(help='set directory path for results output',
                                         metavar='RESULTS_DIR'),
                        data_dir=dict(help='set directory path for data',
                                      metavar='DATA_DIR'),
                        macros_dir=dict(help='set directory path for macros',
                                        metavar='MACROS_DIR'),
                        templates_dir=dict(help='set directory path for template files',
                                           metavar='TEMPLATES_DIR'),
                        spark_cfg_file=dict(help='set path of Spark configuration file',
                                            metavar='SPARK_CONFIG_FILE'),
                        seed=dict(help='set seed for random-number generation',
                                  action='append',
                                  metavar='KEY=SEED'))
USER_OPTS_CONF_KEYS = dict(analysis_name='analysisName', analysis_version='analysisVersion', batch_mode='batchMode',
                           log_level='logLevel', log_format='logFormat', profile='doCodeProfiling',
                           begin_with='beginWithChain', end_with='endWithChain', store_all='storeResultsEachChain',
                           store_one='storeResultsOneChain', store_none='doNotStoreResults',
                           spark_cfg_file='sparkCfgFile', seed='seeds')


def set_opt_var(opt_key, settings, args):
    """Set configuration variable from user options"""

    value = args.get(opt_key)
    if value is None:
        return
    conf_key = USER_OPTS_CONF_KEYS.get(opt_key, opt_key)
    settings[conf_key] = CONFIG_TYPES.get(conf_key, str)(value)
CONFIG_OPTS_SETTERS = collections.defaultdict(lambda: set_opt_var)


def set_log_level_opt(opt_key, settings, args):
    """Set configuration log level from user option"""

    level = args.get(opt_key)
    if not level:
        return
    if level not in LOG_LEVELS:
        raise ValueError('invalid logging level specified: "{}"'.format(str(level)))

    settings[USER_OPTS_CONF_KEYS.get(opt_key, opt_key)] = LOG_LEVELS[level]
CONFIG_OPTS_SETTERS['log_level'] = set_log_level_opt


def set_begin_end_chain_opt(opt_key, settings, args):
    """Set begin/end-chain variable from user option"""

    chain = args.get(opt_key)
    if not chain:
        return
    if args.get('single_chain'):
        raise RuntimeError('"begin-with" and "end-with" chain options cannot be combined with "single-chain" option')
    settings[USER_OPTS_CONF_KEYS.get(opt_key, opt_key)] = str(chain)
CONFIG_OPTS_SETTERS['begin_with'] = set_begin_end_chain_opt
CONFIG_OPTS_SETTERS['end_with'] = set_begin_end_chain_opt


def set_single_chain_opt(opt_key, settings, args):
    """Set single-chain variable from user option"""

    chain = args.get(opt_key)
    if not chain:
        return
    settings[USER_OPTS_CONF_KEYS['begin_with']] = str(chain)
    settings[USER_OPTS_CONF_KEYS['end_with']] = str(chain)
CONFIG_OPTS_SETTERS['single_chain'] = set_single_chain_opt


def set_seeds(opt_key, settings, args):
    """Set random seeds"""

    seed_args = args.get(opt_key)
    if not seed_args:
        return

    seeds = settings[USER_OPTS_CONF_KEYS.get(opt_key, opt_key)]
    for kv in seed_args:
        kv = kv.strip()
        eq_pos = kv.find('=')
        if eq_pos == 0 or eq_pos == len(kv) - 1:
            raise RuntimeError('expected "key=seed" for --seed command-line argument; got "{}"'.format(kv))
        key, value = (kv[:eq_pos].strip().lower(), kv[eq_pos + 1:].strip()) if eq_pos > 0 else ('default', kv.strip())
        seeds[key] = value
CONFIG_OPTS_SETTERS['seed'] = set_seeds


def set_custom_user_vars(opt_key, settings, args):
    """Set custom user configuration variables"""

    custom_vars = args.get(opt_key)
    if not custom_vars:
        return

    for var in custom_vars:
        # parse key-value pair
        var = var.strip()
        eq_pos = var.find('=')
        if eq_pos < 1 or eq_pos > len(var) - 2:
            raise RuntimeError('Expected "key=value" for --conf-var command-line argument; got "{}"'.format(var))
        key, value = var[:eq_pos].strip(), var[eq_pos + 1:].strip()

        # interpret type of value
        try:
            settings[key] = ast.literal_eval(value)
        except:
            settings[key] = value
CONFIG_OPTS_SETTERS['conf_var'] = set_custom_user_vars
