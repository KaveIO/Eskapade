import unittest
import unittest.mock as mock

from eskapade.mixins import ConfigMixin


class ConfigMixinTest(unittest.TestCase):
    """Test configuration-settings mixin"""

    def test_init(self):
        """Test initialization of configuration mixin"""

        # test with different argument values
        for kwargs in ({}, dict(config_path=''), dict(config_path=None), dict(config_path='path2')):
            # get path
            path = kwargs.get('config_path') if kwargs.get('config_path') else ''

            # create mock configuration-mixin instance and call init function
            mock_cfgmix = mock.Mock(name='config_mixin')
            ConfigMixin.__init__(mock_cfgmix, **kwargs)

            # check if attributes exist
            self.assertTrue(hasattr(mock_cfgmix, '_config_path'), 'config_path not set')
            self.assertTrue(hasattr(mock_cfgmix, '_config'), 'config attribute not set')

            # check attribute values
            self.assertEqual(mock_cfgmix._config_path, path, 'unexpected value for config_path')
            self.assertIs(mock_cfgmix._config, None, 'unexpected value for config attribute')

    def check_get_property(self, property_name, value):
        """Check value returned by property getter"""

        # create mock configuration-mixin instance
        mock_cfgmix = mock.Mock(name='config_mixin')
        setattr(mock_cfgmix, '_' + property_name, value)

        # call config_path getter and check returned value
        val_get = getattr(ConfigMixin, property_name).__get__(mock_cfgmix)
        self.assertEqual(val_get, value, 'unexpected value returned by {} getter'.format(property_name))

    def check_set_property(self, property_name, values, invalid_values):
        """Test setting the config-file path"""

        for val in values:
            # create mock configuration-mixin instance
            mock_cfgmix = mock.Mock(name='config_mixin')

            # call config_path setter and check attribute value
            getattr(ConfigMixin, property_name).__set__(mock_cfgmix, val)
            self.assertTrue(hasattr(mock_cfgmix, '_' + property_name), '{} not set'.format(property_name))
            self.assertEqual(getattr(mock_cfgmix, '_' + property_name), str(val),
                             'unexpected value set by {} setter'.format(property_name))

        # call setter with invalid arguments
        for val in invalid_values:
            with self.assertRaises(ValueError):
                ConfigMixin.config_path.__set__(mock_cfgmix, val)

    def test_get_config_path(self):
        """Test getting the config-file path"""

        self.check_get_property('config_path', 'my_cfg_dir/my_cfg_file.cfg')

    def test_set_config_path(self):
        """Test setting the config-file path"""

        path = 'my_cfg_dir/my_cfg_path.cfg'
        self.check_set_property('config_path',
                                (path, type('sec_arg_valid', (), {'__str__': lambda s: path})()),
                                ('', None, type('sec_arg_invalid', (), {'__str__': lambda s: ''})()))

    @mock.patch('configparser.ConfigParser')
    def test_get_config(self, mock_cfg_cls):
        """Test getting the configuration settings"""

        # create and configure mock objects
        mock_cfgmix = mock.Mock(name='config_mixin')
        mock_cfg_exist = mock.Mock(name='ConfigParser_existing')
        mock_cfg_create = mock.Mock(name='ConfigParser_created')
        mock_cfg_cls.return_value = mock_cfg_create

        # test returning existing config
        mock_cfgmix._config = mock_cfg_exist
        cfg = ConfigMixin.get_config(mock_cfgmix)
        self.assertIs(cfg, mock_cfg_exist, 'unexpected existing config settings returned')
        mock_cfg_cls.assert_not_called()

        # test returning created config
        path = 'my_cfg_dir/my_cfg_path.cfg'
        mock_cfgmix._config = None
        mock_cfgmix.config_path = path
        cfg = ConfigMixin.get_config(mock_cfgmix)
        self.assertIs(cfg, mock_cfg_create, 'unexpected created config settings returned')
        self.assertIs(mock_cfgmix._config, mock_cfg_create, 'unexpected created config settings set')
        mock_cfg_cls.assert_called_once_with()
        mock_cfg_create.read.assert_called_once_with(path)
        mock_cfg_cls.reset_mock()

        # test creating config without path set
        mock_cfgmix._config = None
        mock_cfgmix.config_path = ''
        with self.assertRaises(RuntimeError):
            ConfigMixin.get_config(mock_cfgmix)
        mock_cfg_cls.reset_mock()
