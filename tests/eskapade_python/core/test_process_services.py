import unittest
import unittest.mock as mock

from eskapade.core.definitions import (CONFIG_VARS, CONFIG_TYPES, CONFIG_DEFAULTS, USER_OPTS,
                                       USER_OPTS_CONF_KEYS,
                                       CONFIG_OPTS_SETTERS, RandomSeeds, set_opt_var, set_log_level_opt,
                                       set_begin_end_chain_opt,
                                       set_single_chain_opt, set_seeds, set_custom_user_vars)
from eskapade.core.process_services import ProcessServiceMeta, ProcessService, ConfigObject, DataStore
from eskapade.logger import Logger


class ProcessServiceMetaTest(unittest.TestCase):
    """Tests for process-service base class type"""

    def test_str(self):
        """Test process-service type-to-string conversion"""

        ps_meta = mock.Mock(name='ProcessServiceMeta_instance')
        ps_meta.__module__ = 'ps_meta_module'
        ps_meta.__name__ = 'ps_meta_name'
        ps_meta_str = ProcessServiceMeta.__str__(ps_meta)
        self.assertEqual(ps_meta_str, 'ps_meta_module.ps_meta_name',
                         'unexpected string conversion for process-service meta class')


class ProcessServiceTest(unittest.TestCase):
    """Tests for process-service base class"""

    def test_persist(self):
        """Test default value of process-service persist flag"""

        self.assertFalse(ProcessService._persist, 'unexpected default value for process-service persist flag')

    def test_str(self):
        """Test process-service instance-to-string conversion"""

        ps = mock.Mock(name='ProcessService_instance')
        ps_str = ProcessService.__str__(ps)
        self.assertEqual(ps_str, '{0!s} ({1:s})'.format(type(ps), hex(id(ps))),
                         'unexpected string conversion for process-service')

    @mock.patch('eskapade.core.process_services.ProcessService.__init__')
    def test_create(self, mock_init):
        """Test process-service create method"""
        ps = mock.Mock(name='ProcessService_instance')
        ps_cls = mock.Mock(name='ProcessService', return_value=ps)
        ps_ = ProcessService.create.__func__(ps_cls)
        ps_cls.assert_called_with()
        mock_init.assert_called_with(ps)
        self.assertIs(ps_, ps)

    @mock.patch('os.path.isfile')
    @mock.patch('pickle.load')
    @mock.patch('eskapade.core.process_services.open')
    def test_import_from_file(self, mock_open, mock_load, mock_isfile):
        """Test process-service file import"""

        # set return values
        mock_isfile.return_value = True
        mock_file = mock.MagicMock(name='service_file')
        mock_file.__enter__.return_value = mock_file
        mock_open.return_value = mock_file

        # create mock process-service class and instance
        logger = Logger()
        ps_cls = type('ps_cls', (), {'persist': True, 'logger': logger})
        ps = mock.Mock(name='ProcessService_instance')
        ps.__class__ = ps_cls

        # test normal import
        mock_load.return_value = ps
        ps_ = ProcessService.import_from_file.__func__(ps_cls, 'mock_file_path')
        self.assertIs(ps_, ps, 'unexpected process-service instance returned')
        mock_open.assert_called_once_with('mock_file_path', 'rb')
        mock_load.assert_called_once_with(mock_file)
        mock_open.reset_mock()
        mock_load.reset_mock()

        # test importing instance of incorrect type
        mock_load.return_value = None
        with self.assertRaises(TypeError):
            ProcessService.import_from_file.__func__(ps_cls, 'mock_file_path')
        mock_open.reset_mock()
        mock_load.reset_mock()

        # test import with non-persisting service
        ps_cls.persist = False
        ps_ = ProcessService.import_from_file.__func__(ps_cls, 'mock_file_path')
        self.assertIs(ps_, None, 'unexpected return value for non-persisting service')

    @mock.patch('pickle.dump')
    @mock.patch('eskapade.core.process_services.open')
    @mock.patch('eskapade.core.process_services.type')
    def test_persist_in_file(self, mock_type, mock_open, mock_dump):
        """Test process-service file persistence"""

        # set return values
        mock_file = mock.MagicMock(name='service_file')
        mock_file.__enter__.return_value = mock_file
        mock_open.return_value = mock_file

        # create mock process-service class and instance
        ps_cls = type('ps_cls', (), {'persist': True})
        ps = mock.Mock(name='ProcessService_instance')
        ps.__class__ = ps_cls
        mock_type.side_effect = lambda a: ps_cls if a is ps else type(a)

        # test normal export
        ProcessService.persist_in_file(ps, 'mock_file_path')
        mock_open.assert_called_once_with('mock_file_path', 'wb')
        mock_dump.assert_called_once_with(ps, mock_file)
        mock_open.reset_mock()
        mock_dump.reset_mock()

        # test export with non-persisting service
        ps_cls.persist = False
        ProcessService.persist_in_file(ps, 'mock_file_path')
        mock_open.assert_not_called()
        mock_dump.assert_not_called()


class ConfigObjectTest(unittest.TestCase):
    """Tests for configuration object"""

    def test_persist(self):
        """Test value of config-object persist flag"""

        self.assertTrue(ConfigObject._persist, 'unexpected value for config-object persist flag')

    @unittest.skip('This test needs to fixed or removed!')
    @mock.patch.dict('eskapade.core.definitions.CONFIG_DEFAULTS', clear=True)
    @mock.patch.dict('eskapade.core.definitions.CONFIG_VARS', clear=True)
    @mock.patch('eskapade.utils.get_dir_path')
    @mock.patch('eskapade.utils.get_env_var')
    def test_init(self, mock_get_env_var, mock_get_dir_path):
        """Test initialization of config object"""

        # set return values of project utility functions
        mock_get_env_var.side_effect = lambda *a, **k: ':0.0' if a and a[0] == 'display' else mock.DEFAULT
        mock_get_dir_path.side_effect = lambda *a, **k: 'es_path' if a and a[0] == 'es_root' else mock.DEFAULT

        # create mock config object
        mock_config_object = mock.MagicMock(name='ConfigObject_instance')
        settings = {}
        mock_config_object.__getitem__ = lambda s, k: settings.__getitem__(k)
        mock_config_object.__setitem__ = lambda s, k, v: settings.__setitem__(k, v)
        mock_config_object.get = settings.get

        # call init method with mock variables
        CONFIG_VARS.update([('sec1', ['var1']), ('sec2', ['var2', 'var3'])])
        CONFIG_DEFAULTS.update(var1='foo', var3=42)
        ConfigObject.__init__(mock_config_object)

        # check values of settings variables
        exp_settings = dict(var1='foo',
                            var2=None,
                            var3=42,
                            batchMode=False,
                            esRoot='es_path',
                            resultsDir='es_path/results',
                            dataDir='es_path/data',
                            macrosDir='es_path/tutorials',
                            templatesDir='es_path/templates',
                            configDir='es_path/config', )

        self.assertDictEqual(settings, exp_settings, 'unexpected resulting settings dictionary')

    def test_random_seeds(self):
        """Test container class for random seeds"""

        # create mock seeds instance
        mock_seeds = mock.MagicMock(name='mock_seeds')

        # test init method
        RandomSeeds.__init__(mock_seeds, foo=42, bar=13)
        self.assertEqual(mock_seeds._seeds, {}, 'seeds dictionary not properly initialized')
        self.assertEqual(mock_seeds._default, 1, 'unexpected value for default seed')
        mock_seeds.assert_has_calls([mock.call.__setitem__('foo', 42), mock.call.__setitem__('bar', 13)],
                                    any_order=True)
        mock_seeds.reset_mock()

        # test getitem method
        mock_seeds._seeds = dict(foo=42, bar=13)
        for key, val in [('foo', 42), ('Foo', 42), (' fOO\t', 42), ('baz', mock_seeds._default)]:
            self.assertEqual(RandomSeeds.__getitem__(mock_seeds, key), val, 'unexpected value for "{}"'.format(key))

        # test setitem method
        seeds_ = dict(bar=13)
        default_ = mock.Mock()
        mock_seeds._seeds = dict(bar=13)
        mock_seeds._default = default_
        for key, val in [('foo', 42), ('Foo', 43), (' fOO\t', 44)]:
            RandomSeeds.__setitem__(mock_seeds, key, val)
            seeds_['foo'] = val
            self.assertDictEqual(mock_seeds._seeds, seeds_, 'unexpected keys after "{}"'.format(key))
            self.assertEqual(mock_seeds._default, default_, 'unexpected default value after "{}"'.format(key))

        # test setting default value
        RandomSeeds.__setitem__(mock_seeds, 'default', 999)
        self.assertDictEqual(mock_seeds._seeds, seeds_, 'unexpected keys after setting default')
        self.assertEqual(mock_seeds._default, 999, 'unexpected default value after setting default')

        # test setting non-integer value
        with self.assertRaises(TypeError):
            RandomSeeds.__setitem__(mock_seeds, 'foo', 'fortytwo')

    @mock.patch.dict('eskapade.core.definitions.USER_OPTS', clear=True)
    @mock.patch.dict('eskapade.core.definitions.CONFIG_OPTS_SETTERS', clear=True)
    def test_set_user_opts(self):
        """Test setting command-line options as settings"""

        # create mock parsed arguments
        mock_parsed_args = mock.Mock(name='mock_parsed_args')
        mock_parsed_args.__dict__ = dict(arg1='foo', arg2=42, arg4='unknown')

        # create mock user options
        USER_OPTS.update([('sec1', ['arg1']), ('sec2', ['arg2', 'arg3'])])
        CONFIG_OPTS_SETTERS.update(('arg{:d}'.format(i),
                                    mock.Mock(name='arg{:d}_setter'.format(i))) for i in range(1, 5))

        # call user-options method
        mock_config_object = mock.Mock(name='MockConfigObject_instance')
        ConfigObject.set_user_opts(mock_config_object, mock_parsed_args)

        # check options-setters calls
        CONFIG_OPTS_SETTERS['arg1'].assert_called_once_with('arg1', mock_config_object, mock_parsed_args.__dict__)
        CONFIG_OPTS_SETTERS['arg2'].assert_called_once_with('arg2', mock_config_object, mock_parsed_args.__dict__)
        CONFIG_OPTS_SETTERS['arg3'].assert_not_called()
        CONFIG_OPTS_SETTERS['arg4'].assert_not_called()

    def test_config_opts_setters_dict(self):
        """Test container for option-setter functions"""

        # check setter-function keys
        self.assertEqual(set(CONFIG_OPTS_SETTERS.keys()),
                         set(['log_level', 'begin_with', 'end_with', 'single_chain', 'seed', 'conf_var']),
                         'unexpected keys for options setters')

        # check setter-function IDs
        self.assertIs(CONFIG_OPTS_SETTERS.default_factory(), set_opt_var, 'unexpected default setter')
        self.assertIs(CONFIG_OPTS_SETTERS.get('log_level'), set_log_level_opt, 'unexpected log-level setter')
        self.assertIs(CONFIG_OPTS_SETTERS.get('begin_with'), set_begin_end_chain_opt, 'unexpected begin-with setter')
        self.assertIs(CONFIG_OPTS_SETTERS.get('end_with'), set_begin_end_chain_opt, 'unexpected end-with setter')
        self.assertIs(CONFIG_OPTS_SETTERS.get('single_chain'), set_single_chain_opt, 'unexpected single-chain setter')
        self.assertIs(CONFIG_OPTS_SETTERS.get('conf_var'), set_custom_user_vars, 'unexpected conf-var setter')

    def loop_set_opt_vars(self, set_func, args, args_, settings, settings_):
        """Call option-variable setter for all arguments"""

        sett = settings.copy()
        for opt_key in args_.keys():
            # update settings reference dictionary
            sett_key = USER_OPTS_CONF_KEYS.get(opt_key, opt_key)
            if sett_key in settings_:
                sett[sett_key] = settings_[sett_key]

            # execute function and check settings and arguments
            set_func(opt_key, settings, args)
            self.assertDictEqual(settings, sett, 'unexpected settings dictionary after "{}"'.format(opt_key))
            self.assertDictEqual(args, args_, 'arguments dictionary modified while setting "{}"'.format(opt_key))

    @mock.patch.dict('eskapade.core.definitions.CONFIG_TYPES', clear=True)
    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_CONF_KEYS', clear=True)
    def test_set_opt_var(self):
        """Test default setter function for user options"""

        # set option keys and type
        USER_OPTS_CONF_KEYS.update(opt1='var1', opt2='var2')
        CONFIG_TYPES.update(var1=int)

        # create arguments and settings
        args = dict(opt1=42, opt2='two', opt3=False, opt4=None)
        args_ = args.copy()
        settings = dict(var0='zero', var2='value2', opt3=None)
        settings_ = dict(var0='zero', var1=42, var2='two', opt3='False')

        # test normal operation
        self.loop_set_opt_vars(set_opt_var, args, args_, settings, settings_)

        # test setting variable with specified type
        CONFIG_TYPES['opt5'] = float
        args = dict(opt5='not_a_float')
        with self.assertRaises(ValueError):
            set_opt_var('opt5', settings, args)
        args['opt5'] = '3.14'
        settings_['opt5'] = 3.14
        set_opt_var('opt5', settings, args)
        self.assertDictEqual(settings, settings_, 'unexpected settings dictionary after "opt5"')

        # test setting option that is not in args
        args = {}
        set_opt_var('opt6', settings, args)
        self.assertDictEqual(settings, settings_, 'unexpected settings dictionary after "opt6"')

    @unittest.skip('What are we testing here? This either needs to be removed or rewritten.')
    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_CONF_KEYS', clear=True)
    def test_set_log_level_opt(self):
        """Test setter function for log level"""

        # set option keys and log levels
        USER_OPTS_CONF_KEYS.update(log_level1='logLevel1')
        mock_level = mock.Mock(name='mock_log_level')

        # create arguments and settings
        args = dict(log_level1='LEVEL', log_level2='LEVEL', log_level3=None)
        args_ = args.copy()
        settings = dict(var0='zero', logLevel1=None)
        settings_ = dict(var0='zero', logLevel1=mock_level, log_level2=mock_level)

        # test normal operation
        self.loop_set_opt_vars(set_log_level_opt, args, args_, settings, settings_)

        # test with invalid log level
        args = dict(log_level4='LEVEL4')

        with self.assertRaises(ValueError):
            set_log_level_opt('log_level4', settings, args)

    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_CONF_KEYS', clear=True)
    def test_set_begin_end_chain_opt(self):
        """Test setter function for begin/end chain options"""

        # set option keys and type
        USER_OPTS_CONF_KEYS.update(begin_with='beginWith')

        # create arguments and settings
        args = dict(begin_with='foo', end_with='bar', single_chain=None)
        args_ = args.copy()
        settings = dict(var0='zero', end_with='baz')
        settings_ = dict(var0='zero', beginWith='foo', end_with='bar')

        # test normal operation
        self.loop_set_opt_vars(set_begin_end_chain_opt, args, args_, settings, settings_)

        # test with "single-chain" defined
        args = dict(begin_with='foo', single_chain='bar')
        with self.assertRaises(RuntimeError):
            set_begin_end_chain_opt('begin_with', settings, args)

    def test_set_single_chain_opt(self):
        """Test setter function for single-chain option"""

        # check if begin and end keys are defined
        begin_key = USER_OPTS_CONF_KEYS.get('begin_with')
        end_key = USER_OPTS_CONF_KEYS.get('end_with')
        self.assertIsInstance(begin_key, str, 'begin-with variable key is not a string')
        self.assertIsInstance(end_key, str, 'end-with variable key is not a string')
        self.assertTrue(begin_key, 'begin-with variable key has no value')
        self.assertTrue(end_key, 'begin-with variable key has no value')

        # create arguments and settings
        args = dict(single_chain='foo', opt2=None)
        args_ = args.copy()
        settings = {'var0': 'zero', end_key: 'baz'}
        settings_ = {'var0': 'zero', begin_key: 'foo', end_key: 'foo'}

        # test normal operation
        set_single_chain_opt('single_chain', settings, args)
        set_single_chain_opt('opt2', settings, args)
        self.assertDictEqual(settings, settings_, 'unexpected settings dictionary')
        self.assertDictEqual(args, args_, 'arguments dictionary modified')

    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_CONF_KEYS', clear=True)
    def test_set_seeds(self):
        """Test setter function for seeds"""

        # set option keys
        USER_OPTS_CONF_KEYS.update(**dict(('opt{:d}'.format(it), 'seeds') for it in range(1, 9)))

        # create mock seeds container
        mock_seeds = mock.MagicMock(name='random_seeds_instance')

        # create arguments and settings
        args = dict(opt1=['foo=1', 'Bar=2', 'BAZ=100'], opt2=['42'], opt3=[], opt4=None)
        args_ = args.copy()
        settings = dict(var0='zero', seeds=mock_seeds)
        settings_ = dict(var0='zero', seeds=mock_seeds)

        # test normal operation
        for opt_key in args:
            set_seeds(opt_key, settings, args)
        self.assertDictEqual(settings, settings_, 'unexpected settings dictionary')
        self.assertDictEqual(args, args_, 'arguments dictionary modified')
        calls = [mock.call.__setitem__(kv.split('=')[0].lower(), kv.split('=')[1]) for kv in args['opt1']]
        mock_seeds.assert_has_calls(calls, any_order=True)
        mock_seeds.reset_mock()

        # test setting malformed key-value pairs
        args = dict(opt5=['='], opt6=['key='], opt7=['=42'])
        for opt_key in args:
            with self.assertRaises(RuntimeError, msg='no error for malformed key-value pair in "{}"'.format(opt_key)):
                set_seeds(opt_key, settings, args)

    def test_set_custom_user_vars(self):
        """Test setter function for custom user variables"""

        # create arguments and settings
        args = dict(opt1=['foo=bar', 'vint=42', "vdict={'f': 'y=pi', 'pi': 3.14}"], opt2=['var=val'], opt3=[],
                    opt4=None)
        args_ = args.copy()
        settings = dict(var0='zero', vint='one')
        settings_ = dict(var0='zero', foo='bar', vint=42, vdict={'f': 'y=pi', 'pi': 3.14}, var='val')

        # test normal operation
        for opt_key in args:
            set_custom_user_vars(opt_key, settings, args)
        self.assertDictEqual(settings, settings_, 'unexpected settings dictionary')
        self.assertDictEqual(args, args_, 'arguments dictionary modified')

        # test setting malformed key-value pairs
        args = dict(opt5=['keyeqvalue'], opt6=['='], opt7=['key='], opt8=['=value'])
        for opt_key in args:
            with self.assertRaises(RuntimeError, msg='no error for malformed key-value pair in "{}"'.format(opt_key)):
                set_custom_user_vars(opt_key, settings, args)


class DataStoreTest(unittest.TestCase):
    """Tests for data store"""

    def test_persist(self):
        """Test value of data-store persist flag"""

        self.assertTrue(DataStore._persist, 'unexpected value for data-store persist flag')
