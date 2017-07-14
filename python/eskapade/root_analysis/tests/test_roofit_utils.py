import unittest
import mock

import ROOT

from ..roofit_utils import load_libesroofit


class LoadLibesroofitTest(unittest.TestCase):
    """Tests for loading Eskapade RooFit library"""

    @mock.patch('ROOT.gSystem.GetLibraries')
    @mock.patch('ROOT.gSystem.Load')
    @mock.patch('eskapade.root_analysis.roofit_utils.CUSTOM_ROOFIT_OBJECTS')
    @mock.patch('eskapade.root_analysis.roofit_utils.log')
    @mock.patch('eskapade.utils.build_cxx_library')
    def test_load_libesroofit(self, mock_build_cxx_library, mock_log, mock_objects, mock_load, mock_get_libraries):
        """Test loading Eskapade RooFit library"""

        # set custom object attributes
        ROOT.MyCustomClass = mock.Mock(name='MyCustomClass')
        ROOT.MyCustomNamespace = mock.Mock(name='MyCustomNamespace')

        # test normal build/load
        mock_objects.__iter__ = lambda s: iter(('MyCustomClass', ('MyCustomNamespace', 'MyCustomFunction')))
        mock_load.return_value = 0
        mock_get_libraries.return_value = 'lib/libmylib.so'
        load_libesroofit()
        mock_build_cxx_library.assert_called_once_with(lib_key='roofit', accept_existing=True)
        mock_load.assert_called_once_with('libesroofit')
        mock_build_cxx_library.reset_mock()
        mock_load.reset_mock()

        # test with loaded library
        mock_get_libraries.return_value = 'lib/libmylib.so lib/libesroofit.so'
        load_libesroofit()
        mock_build_cxx_library.assert_not_called()
        mock_load.assert_not_called()
        mock_get_libraries.return_value = 'lib/libmylib.so'
        mock_build_cxx_library.reset_mock()
        mock_load.reset_mock()

        # test failed load
        mock_load.return_value = -1
        with self.assertRaises(RuntimeError):
            load_libesroofit()
        mock_load.return_value = 0
        mock_build_cxx_library.reset_mock()
        mock_load.reset_mock()

        # test missing custom class
        mock_objects.__iter__ = lambda s: iter(('NoSuchClass',))
        with self.assertRaises(RuntimeError):
            load_libesroofit()

        # delete custom class attribute
        del ROOT.MyCustomClass
