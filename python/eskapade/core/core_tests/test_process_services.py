import unittest
import mock

from ..definitions import (LOG_LEVELS, CONFIG_VARS, CONFIG_TYPES, CONFIG_DEFAULTS, USER_OPTS, USER_OPTS_CONF_KEYS,
                           CONFIG_OPTS_SETTERS, set_opt_var, set_log_level_opt, set_begin_end_chain_opt,
                           set_single_chain_opt, set_custom_user_vars)
from ..process_services import ProcessService, ConfigObject


class ProcessServiceTest(unittest.TestCase):
    """Tests for process-service base class"""

    @mock.patch('eskapade.core.process_services.ProcessService.__init__')
    def test_create(self, mock_init):
        """Test process-service create method"""

        ps = mock.Mock(name='ProcessService_instance')
        ps_cls = mock.Mock(name='ProcessService', return_value=ps)
        ps_ = ProcessService.create.__func__(ps_cls)
        ps_cls.assert_called()
        mock_init.assert_called_with(ps)
        self.assertIs(ps_, ps)


class ConfigObjectTest(unittest.TestCase):
    """Tests for configuration object"""

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
        exp_settings = dict(var1='foo', var2=None, var3=42, batchMode=False, esRoot='es_path',
                            resultsDir='es_path/results', dataDir='es_path/data', macrosDir='es_path/tutorials',
                            templatesDir='es_path/templates')
        self.assertDictEqual(settings, exp_settings, 'unexpected resulting settings dictionary')

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
                         set(['log_level', 'begin_with', 'end_with', 'single_chain', 'conf_var']),
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
        for opt_key, opt_val in args_.items():
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

    @mock.patch.dict('eskapade.core.definitions.LOG_LEVELS', clear=True)
    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_CONF_KEYS', clear=True)
    def test_set_log_level_opt(self):
        """Test setter function for log level"""

        # set option keys and log levels
        USER_OPTS_CONF_KEYS.update(log_level1='logLevel1')
        mock_level = mock.Mock(name='mock_log_level')
        LOG_LEVELS.update(LEVEL=mock_level)

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

    def test_set_custom_user_vars(self):
        """Test setter function for custom user variables"""

        # create arguments and settings
        args = dict(opt1=['foo=bar', 'vint=42', "vdict={'f': 'y=pi', 'pi': 3.14}"], opt2=['var=val'], opt3=[],
                    opt4=None)
        args_ = args.copy()
        settings = dict(var0='zero', vint='one')
        settings_ = dict(var0='zero', foo='bar', vint=42, vdict={'f': 'y=pi', 'pi': 3.14}, var='val')

        # test normal operation
        for opt_key in args.keys():
            set_custom_user_vars(opt_key, settings, args)
        self.assertDictEqual(settings, settings_, 'unexpected settings dictionary')
        self.assertDictEqual(args, args_, 'arguments dictionary modified')

        # test setting malformed key-value pairs
        args = dict(opt5=['keyeqvalue'], opt6=['='], opt7=['key='], opt8=['=value'])
        for opt_key in args.keys():
            with self.assertRaises(RuntimeError, msg='no error for malformed key-value pair in "{}"'.format(opt_key)):
                set_custom_user_vars(opt_key, settings, args)
