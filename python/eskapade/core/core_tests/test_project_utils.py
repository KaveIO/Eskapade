import unittest
import mock
import sys
import os

from ..definitions import USER_OPTS, USER_OPTS_SHORT, USER_OPTS_KWARGS
from ..project_utils import set_matplotlib_backend, create_arg_parser


class MatplotlibBackendTest(unittest.TestCase):
    """Test for setting Matplotlib backend"""

    @mock.patch('eskapade.core.project_utils.log')
    @mock.patch('eskapade.core.project_utils.get_env_var')
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


class CommandLineArgumentTest(unittest.TestCase):
    """Tests for parsing command-line arguments in run"""

    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_KWARGS', clear=True)
    @mock.patch.dict('eskapade.core.definitions.USER_OPTS_SHORT', clear=True)
    @mock.patch.dict('eskapade.core.definitions.USER_OPTS', clear=True)
    @mock.patch('argparse.ArgumentParser')
    def test_create_arg_parser(self, MockArgumentParser):
        """Test creation of argument parser for Eskapade run"""

        # create mock ArgumentParser instance
        arg_parse_inst = mock.Mock(name='ArgumentParser_instance')
        MockArgumentParser.return_value = arg_parse_inst

        # call create function with mock options
        USER_OPTS.update([('sec1', ['opt_1']), ('sec2', ['opt_2', 'opt_3'])])
        USER_OPTS_SHORT.update(opt_2='a')
        USER_OPTS_KWARGS.update(opt_1=dict(help='option 1', action='store_true'),
                                opt_2=dict(help='option 2', type=int))
        arg_parser = create_arg_parser()

        # assert argument parser was created
        MockArgumentParser.assert_called_once_with()
        self.assertIs(arg_parser, arg_parse_inst)

        # check number of added arguments
        num_add = arg_parse_inst.add_argument.call_count
        self.assertEqual(num_add, 4, 'Expected {0:d} arguments to be added, got {1:d}'.format(4, num_add))

        # check arguments that were added
        calls = [mock.call.add_argument('config_files', nargs='+', metavar='CONFIG_FILE',
                                        help='configuration file to execute'),
                 mock.call.add_argument('--opt-1', help='option 1', action='store_true'),
                 mock.call.add_argument('--opt-2', '-a', help='option 2', type=int),
                 mock.call.add_argument('--opt-3')]
        arg_parse_inst.assert_has_calls(calls, any_order=False)
