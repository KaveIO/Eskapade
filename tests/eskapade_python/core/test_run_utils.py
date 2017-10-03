import unittest
import unittest.mock as mock

from eskapade.core.definitions import USER_OPTS, USER_OPTS_SHORT, USER_OPTS_KWARGS
from eskapade.core.run_utils import create_arg_parser


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
