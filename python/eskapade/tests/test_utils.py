import sys
import unittest
import unittest.mock as mock

from eskapade.utils import set_matplotlib_backend, build_cxx_library


class MatplotlibBackendTest(unittest.TestCase):
    """Test for setting Matplotlib backend"""

    @mock.patch('eskapade.utils.log')
    @mock.patch('eskapade.utils.get_env_var')
    @mock.patch.dict('sys.modules', clear=False)
    @mock.patch('matplotlib.rcsetup')
    @mock.patch('matplotlib.get_backend')
    @mock.patch('matplotlib.interactive')
    @mock.patch('matplotlib.use')
    def test_set_matplotlib_backend(self, mock_use, mock_interactive, mock_get_backend, mock_rcsetup, mock_get_env_var,
                                    mock_log):
        """Test setting Matplotlib backend"""

        # remove pyplot from mock modules
        sys.modules.pop('matplotlib.pyplot', None)

        # set mock backends
        mock_get_backend.return_value = 'Qt5Agg'
        mock_rcsetup.non_interactive_bk = ['agg', 'pdf']

        # set normal display variable
        mock_get_env_var.side_effect = lambda v: ':0.0' if v == 'display' else ''

        # test default operation with display
        set_matplotlib_backend(silent=False)
        mock_interactive.assert_not_called()
        mock_use.assert_not_called()
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test setting batch mode with display
        set_matplotlib_backend(batch=True, silent=False)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test setting interactive backend with display
        set_matplotlib_backend(backend='gtkcairo', silent=False)
        mock_interactive.assert_not_called()
        mock_use.assert_called_once_with('gtkcairo')
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test setting non-interactive backend with display
        set_matplotlib_backend(backend='agg', silent=False)
        mock_interactive.assert_not_called()
        mock_use.assert_called_once_with('agg')
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # add pyplot to mock modules
        sys.modules['matplotlib.pyplot'] = mock.Mock(name='mock_matplotlib_pyplot')

        # test setting batch mode with pyplot loaded
        with self.assertRaises(RuntimeError):
            set_matplotlib_backend(backend='agg', silent=False)
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test silently setting backend with pyplot loaded
        set_matplotlib_backend(backend='agg', silent=True)
        mock_interactive.assert_not_called()
        mock_use.assert_not_called()
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # remove pyplot from mock modules
        sys.modules.pop('matplotlib.pyplot')

        # test setting interactive backend in batch mode with display
        with self.assertRaises(RuntimeError):
            set_matplotlib_backend(backend='gtkcairo', batch=True, silent=False)
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test silently setting interactive backend in batch mode with display
        set_matplotlib_backend(backend='gtkcairo', batch=True, silent=True)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_log.warning.assert_called_once_with('Set Matplotlib backend to "%s"; non-interactive backend required, but "%s" requested', 'agg', 'gtkcairo')
        mock_interactive.reset_mock()
        mock_use.reset_mock()
        mock_log.reset_mock()

        # set empty display variable
        mock_get_env_var.side_effect = lambda v: ''

        # test default operation without display
        set_matplotlib_backend(silent=False)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test setting batch mode without display
        set_matplotlib_backend(batch=True, silent=False)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test setting interactive backend without display
        with self.assertRaises(RuntimeError):
            set_matplotlib_backend(backend='gtkcairo', silent=False)
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test silently setting interactive backend without display
        set_matplotlib_backend(backend='gtkcairo', silent=True)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_log.warning.assert_called_once_with('Set Matplotlib backend to "%s"; non-interactive backend required, but "%s" requested', 'agg', 'gtkcairo')
        mock_interactive.reset_mock()
        mock_use.reset_mock()
        mock_log.reset_mock()

        # test setting non-interactive backend without display
        set_matplotlib_backend(backend='agg', silent=False)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test setting no batch mode without display
        with self.assertRaises(RuntimeError):
            set_matplotlib_backend(batch=False, silent=False)
        mock_interactive.reset_mock()
        mock_use.reset_mock()

        # test silently setting no batch mode without display
        set_matplotlib_backend(batch=False, silent=True)
        mock_interactive.assert_called_once_with(False)
        mock_use.assert_called_once_with('agg')
        mock_log.warning.assert_called_once_with('Matplotlib cannot be used interactively; no display found')
        mock_interactive.reset_mock()
        mock_use.reset_mock()
        mock_log.reset_mock()


class BuildCxxLibraryTest(unittest.TestCase):
    """Test for calling function to build C++ library"""

    @mock.patch('eskapade.utils.get_dir_path')
    @mock.patch('eskapade.utils.log')
    @mock.patch('os.path.isfile')
    @mock.patch('subprocess.PIPE')
    @mock.patch('subprocess.run')
    def test_build_cxx_library(self, mock_run, mock_pipe, mock_isfile, mock_log, mock_get_dir_path):
        """Test calling C++ library build function"""

        # create mock for build-process result
        mock_build_result = mock.Mock(name='CompletedProcess_instance')
        mock_build_result.returncode = 0
        mock_build_result.stdout = 'Make stdout'
        mock_build_result.stderr = 'Make stderr'
        mock_run.return_value = mock_build_result

        # mock directories
        mock_dirs = dict(es_lib='lib_dir', es_cxx_make='make_dir')
        mock_get_dir_path.side_effect = lambda k: mock_dirs.get(k, 'no_such_dir')

        # mock isfile
        mock_isfile.return_value = False

        # test normal build
        build_cxx_library()
        mock_run.assert_called_once_with(['make', '-C', 'make_dir', 'install'], stdout=mock_pipe, stderr=mock_pipe,
                                         universal_newlines=True)
        mock_run.reset_mock()

        # test building specific library
        build_cxx_library(lib_key='roofit')
        mock_run.assert_called_once_with(['make', '-C', 'make_dir', 'roofit-install'], stdout=mock_pipe,
                                         stderr=mock_pipe, universal_newlines=True)
        mock_run.reset_mock()

        # test calling with non-existing library key
        with self.assertRaises(AssertionError):
            build_cxx_library(lib_key='no_such_key')
        mock_run.assert_not_called()
        mock_run.reset_mock()

        # set return code for failed build
        mock_build_result.returncode = 1

        # test failed build
        with self.assertRaises(RuntimeError):
            build_cxx_library(lib_key='')
        mock_run.assert_called_once_with(['make', '-C', 'make_dir', 'install'], stdout=mock_pipe, stderr=mock_pipe,
                                         universal_newlines=True)
        mock_run.reset_mock()

        # test failed build for specific library
        with self.assertRaises(RuntimeError):
            build_cxx_library(lib_key='roofit', accept_existing=True)
        mock_run.assert_called_once_with(['make', '-C', 'make_dir', 'roofit-install'], stdout=mock_pipe,
                                         stderr=mock_pipe, universal_newlines=True)
        mock_run.reset_mock()

        # set return value for existing-library check
        mock_isfile.return_value = True

        # test failed build for specific existing library (not accepted)
        with self.assertRaises(RuntimeError):
            build_cxx_library(lib_key='roofit', accept_existing=False)
        mock_run.assert_called_once_with(['make', '-C', 'make_dir', 'roofit-install'], stdout=mock_pipe,
                                         stderr=mock_pipe, universal_newlines=True)
        mock_run.reset_mock()

        # test failed build for specific existing library (accepted)
        mock_isfile.reset_mock()
        build_cxx_library(lib_key='roofit', accept_existing=True)
        mock_run.assert_called_once_with(['make', '-C', 'make_dir', 'roofit-install'], stdout=mock_pipe,
                                         stderr=mock_pipe, universal_newlines=True)
        mock_isfile.assert_called_once_with('lib_dir/libesroofit.so')
        mock_log.warning.assert_called_once_with('Failed to build library with target "%s"; using existing version', 'roofit-install')
        mock_run.reset_mock()
        mock_isfile.reset_mock()
        mock_log.reset_mock()
