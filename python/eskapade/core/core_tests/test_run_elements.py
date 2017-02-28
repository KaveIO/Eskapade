import unittest
import mock

from ..definitions import StatusCode
from ..run_elements import Chain, Link
from ..process_manager import ProcessManager
from .. import execution


def _status_side_effect(link):
    if link.name == 'fail':
        return StatusCode.Failure
    elif link.name == 'skip':
        return StatusCode.SkipChain
    else:
        return StatusCode.Success


def _filepath_side_effect(io_type, io_conf, sub_path):
    if sub_path.startswith('mlFrom_'):
        return 'ml_filepath'
    elif sub_path.startswith('dataFrom_'):
        return 'ds_filepath'
    elif sub_path.startswith('configFrom_'):
        return 'settings_filepath'
    elif sub_path.startswith('ml_'):
        return 'ml_symlink'
    elif sub_path.startswith('data_'):
        return 'data_symlink'
    elif sub_path.startswith('config_'):
        return 'settings_symlink'
    else:
        raise ValueError('No valid sub_path specified in method being tested.')


class LinkTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_clone(self):
        l1 = Link('l1')

        l1_clone = l1.clone()

        self.assertNotEqual(id(l1), id(l1_clone))
        self.assertEqual(l1.name, l1_clone.name)

    def test_load(self):
        pass

    def test_store(self):
        pass

    def tearDown(self):
        execution.reset_eskapade()


# test with the real Link class, Link class not mocked
class ChainTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_clone(self):
        c1 = Chain('c1')
        l1 = Link('l1')
        l2 = Link('l2')
        c1.links = [l1, l2]

        c1_clone = c1.clone()

        self.assertNotEqual(id(c1), id(c1_clone))
        self.assertNotEqual(id(c1.links[0]), id(c1_clone.links[0]))
        self.assertEqual(c1.name, c1_clone.name)
        self.assertEqual(c1.links[0].name, c1_clone.links[0].name)

    @mock.patch('eskapade.core.run_elements.Link.initialize_link')
    @mock.patch('eskapade.core.run_elements.Link.execute_link')
    @mock.patch('eskapade.core.run_elements.Link.finalize_link')
    def test_execute(self, mock_finalize, mock_execute, mock_initialize):
        c1 = Chain('c1')
        l1 = Link('l1')
        l2 = Link('l2')
        l3 = Link('l3')

        # test happy flow
        c1.links = [l1, l2, l3]
        mock_initialize.return_value = StatusCode.Success
        mock_execute.return_value = StatusCode.Success
        mock_finalize.return_value = StatusCode.Success
        mock_parent = mock.MagicMock(autospec=True)
        mock_parent.attach_mock(mock_initialize, 'initialize')
        mock_parent.attach_mock(mock_execute, 'execute')
        mock_parent.attach_mock(mock_finalize, 'finalize')
        status = c1.initialize()
        self.assertEqual(status, StatusCode.Success)
        status = c1.execute()
        self.assertEqual(status, StatusCode.Success)
        status = c1.finalize()
        self.assertEqual(status, StatusCode.Success)
        calls = [mock.call.initialize()]*3 + [mock.call.execute()]*3 + [mock.call.finalize()]*3
        mock_parent.assert_has_calls(calls, any_order=False)

    @mock.patch('eskapade.core.run_elements.Link.initialize_link', autospec=True)
    @mock.patch('eskapade.core.run_elements.Link.execute_link', autospec=True)
    @mock.patch('eskapade.core.run_elements.Link.finalize_link', autospec=True)
    def test_execute_status(self, mock_finalize, mock_execute, mock_initialize):
        c1 = Chain('c1')
        l1 = Link('l1')
        lskip = Link('skip')
        l2 = Link('l2')
        lfail = Link('fail')

        # initialize should sent skipChain status
        c1.links = [l1, lskip, l2]
        mock_initialize.side_effect = _status_side_effect
        mock_execute.return_value = StatusCode.Success
        mock_finalize.return_value = StatusCode.Success
        status = c1.initialize()
        self.assertEqual(status, StatusCode.SkipChain)
        calls = [mock.call(l1), mock.call(lskip)]
        mock_initialize.assert_has_calls(calls, any_order=False)
        self.assertEqual(len(mock_execute.mock_calls), 0)
        self.assertEqual(len(mock_finalize.mock_calls), 0)

        # initialize should sent fail status
        mock_initialize.reset_mock()
        mock_execute.reset_mock()
        mock_finalize.reset_mock()
        c1.links = [l1, lfail, l2]
        mock_initialize.side_effect = _status_side_effect
        status = c1.initialize()
        self.assertEqual(status, StatusCode.Failure)
        calls = [mock.call(l1), mock.call(lfail)]
        mock_initialize.assert_has_calls(calls, any_order=False)
        self.assertEqual(len(mock_execute.mock_calls), 0)
        self.assertEqual(len(mock_finalize.mock_calls), 0)

        # execute should sent fail status
        mock_initialize.reset_mock()
        mock_execute.reset_mock()
        mock_finalize.reset_mock()
        c1.links = [l1, lfail, l2]
        mock_initialize.side_effect = None
        mock_initialize.return_value = StatusCode.Success
        mock_execute.side_effect = _status_side_effect
        mock_finalize.return_value = StatusCode.Success
        status = c1.execute()
        self.assertEqual(status, StatusCode.Failure)
        init_calls = [mock.call(l1), mock.call(lfail), mock.call(l2)]
        exec_calls = [mock.call(l1), mock.call(lfail)]
        self.assertEqual(len(mock_initialize.mock_calls), 0)
        mock_execute.assert_has_calls(exec_calls, any_order=False)
        self.assertEqual(len(mock_finalize.mock_calls), 0)

        # finalize should sent fail status
        mock_initialize.reset_mock()
        mock_execute.reset_mock()
        mock_finalize.reset_mock()
        c1.links = [l1, lfail, l2]
        mock_execute.side_effect = None
        mock_execute.return_value = StatusCode.Success
        mock_finalize.side_effect = _status_side_effect
        status = c1.finalize()
        self.assertEqual(status, StatusCode.Failure)
        init_calls = [mock.call(l1), mock.call(lfail), mock.call(l2)]
        exec_calls = [mock.call(l1), mock.call(lfail), mock.call(l2)]
        final_calls = [mock.call(l1), mock.call(lfail)]
        self.assertEqual(len(mock_initialize.mock_calls), 0)
        self.assertEqual(len(mock_execute.mock_calls), 0)
        mock_finalize.assert_has_calls(final_calls, any_order=False)

    # This test uses the real processManager, ProcessManager class is not mocked
    def test_finalize(self):
        c1 = Chain('c1')
        status = c1.finalize()
        self.assertEqual(status, StatusCode.Success)

    def tearDown(self):
        execution.reset_eskapade()
