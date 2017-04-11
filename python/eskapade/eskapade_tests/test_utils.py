import unittest
import mock
import sys

from ..utils import set_matplotlib_backend


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
        mock_log.warning.assert_called_once()
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
        mock_log.warning.assert_called_once()
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
        mock_log.warning.assert_called_once()
        mock_interactive.reset_mock()
        mock_use.reset_mock()
        mock_log.reset_mock()
