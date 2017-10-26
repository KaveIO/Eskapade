import unittest
import unittest.mock as mock

from eskapade.core.definitions import StatusCode
from eskapade.core.process_manager import process_manager, ProcessManager
from eskapade.core.process_services import ConfigObject
from eskapade.core.process_services import ProcessService
from eskapade.core.element import Chain


def _status_side_effect(chain):
    if chain.name == 'fail':
        return StatusCode.Failure
    elif chain.name == 'skip':
        return StatusCode.SkipChain
    else:
        return StatusCode.Success


def _chain_idx_side_effect(value):
    return int(value) - 1


class ProcessServiceMock(ProcessService):
    pass


class ProcessManagerTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_singleton(self):
        pm_1 = ProcessManager()
        pm_2 = ProcessManager()
        self.assertIs(pm_1, pm_2, 'process manager is not a singleton')

    @unittest.skip('We are just mocking the process manager?!?! This test needs to be rewritten or removed!')
    @mock.patch('eskapade.core.process_services._ProcessService.create')
    def test_service(self, mock_create):
        pm = mock.Mock(name='process_manager')

        # register service by specifying type
        ps = ProcessServiceMock()
        mock_create.return_value = ps
        pm._services = {}
        ps_ = process_manager.service(pm, ProcessServiceMock)
        self.assertIn(ProcessServiceMock, pm._services)
        self.assertIs(ps_, ps)
        self.assertIs(pm._services[ProcessServiceMock], ps)

        # register service by specifying instance
        ps = ProcessServiceMock()
        pm._services = {}
        ps_ = process_manager.service(pm, ps)
        self.assertIn(ProcessServiceMock, pm._services)
        self.assertIs(ps_, ps)
        self.assertIs(pm._services[ProcessServiceMock], ps)

        # register service with wrong value
        ps = ProcessServiceMock()
        pm._services = {ProcessServiceMock: None}
        with self.assertRaises(ValueError):
            process_manager.service(pm, ps)

        # register service with wrong type
        with self.assertRaises(TypeError):
            process_manager.service(pm, object)

    @unittest.skip('We are just mocking the process manager?!?!')
    def test_get_services(self):
        pm = mock.Mock(name='process_manager')

        # get three mock services
        serv1, serv2, serv3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        pm._services = {serv1: 'Service1', serv2: 'Service2', serv3: 'Service3'}
        services = process_manager.get_services(pm)
        self.assertSetEqual(services, {serv1, serv2, serv3})

    @unittest.skip('We are just mocking the process manager?!?!')
    def test_get_service_tree(self):
        pm = mock.Mock(name='process_manager')

        # get service tree with three mock services
        serv1, serv2, serv3 = mock.MagicMock(), mock.MagicMock(), mock.MagicMock()
        serv1.__module__ = 'foo'
        serv2.__module__ = 'foo.bar'
        serv3.__module__ = 'foo.bar'
        pm.get_services = mock.Mock(return_value={serv1, serv2, serv3})
        serv_tree = {'foo': {'-services-': {serv1}, 'bar': {'-services-': {serv2, serv3}}}}
        serv_tree_ = process_manager.get_service_tree(pm)
        self.assertDictEqual(serv_tree_, serv_tree)

    def test_add_chain(self):
        pm = process_manager
        c = []
        with self.assertRaises(TypeError):
            pm.add_chain(c, new_name=c)
        with self.assertRaises(NotImplementedError):
            pm.add_chain(c)

        chain = pm.add_chain('name')
        self.assertIsInstance(chain, Chain)
        self.assertEqual(chain.name, 'name')
        self.assertIn(chain, pm.chains)

        with self.assertRaises(RuntimeError):
            pm.add_chain('name')

    def test_remove_chain(self):
        pm = ProcessManager()

        pm.add_chain('1')

        # Remove non-existing chain.
        # Should leave everything unmodified.
        pm.remove_chain('2')
        self.assertTrue(pm.chains, 'Process manager has no chains!')
        self.assertEqual(len(pm.chains), 1, 'Number of chains is not equal to 1!')

        # Remove existing chain.
        pm.remove_chain('1')
        self.assertFalse(pm.chains, 'Process manager has chains!')

    def test_remove_chains(self):
        pm = ProcessManager()
        pm.add_chain('1')
        pm.add_chain('2')
        pm.add_chain('3')

        pm.remove_chains()

        self.assertFalse(pm.chains, 'Process manager has chains!')

    def test_reset(self):
        pm = ProcessManager()

        pm.add_chain('dummy')
        self.assertTrue(pm.chains, 'Process manager has no chains!')

        pm.reset()
        self.assertFalse(pm.chains, 'Process manager has chains!')

    def test_get_chain_idx(self):
        pm = process_manager
        pm.add_chain('1')
        pm.add_chain('2')
        pm.add_chain('3')

        idx = pm.idx('2')
        self.assertEqual(idx, 1)

    @unittest.skip('We are just mocking the process manager?!?!')
    @mock.patch('eskapade.core.process_manager.ProcessManager.Print')
    def test_initialize(self, mock_print):
        pm = process_manager
        pm.add_chain('1')
        pm.add_chain('2')
        pm.add_chain('3')

        status = pm.initialize()

        self.assertEqual(pm.chains[0].prev_chain_name, '')
        self.assertEqual(pm.chains[1].prev_chain_name, '1')
        self.assertEqual(pm.chains[2].prev_chain_name, '2')
        assert mock_print.called
        self.assertIsInstance(status, StatusCode)

    @unittest.skip('This test needs to rewritten!')
    @mock.patch('eskapade.core.process_manager.ProcessManager.persist_services')
    @mock.patch('eskapade.core.process_manager.ProcessManager.import_services')
    @mock.patch('eskapade.core.process_manager.ProcessManager.execute')
    def test_execute_all(self, mock_execute, mock_import, mock_persist):
        pm = process_manager
        pm.service(ConfigObject)['analysisName'] = 'test_execute_all'
        mock_execute.return_value = StatusCode.Success
        pm.chains = [Chain(str(it + 1)) for it in range(3)]
        for it, ch in enumerate(pm.chains):
            ch.prev_chain_name = str(it)
        status = pm.execute_all()
        self.assertEqual(status, StatusCode.Success)
        mock_import.assert_not_called()
        calls = [mock.call(ch) for ch in pm.chains]
        mock_execute.assert_has_calls(calls, any_order=False)
        mock_persist.assert_called()

        mock_execute.reset_mock()
        mock_import.reset_mock()
        mock_persist.reset_mock()

        pm.reset()
        pm.chains = [Chain(str(it + 1)) for it in range(5)]
        for it, ch in enumerate(pm.chains):
            ch.prev_chain_name = str(it)
        settings = pm.service(ConfigObject)
        settings['analysisName'] = 'test_execute_all'
        settings['doNotStoreResults'] = False
        settings['storeResultsEachChain'] = True
        settings['beginWithChain'] = '2'
        settings['endWithChain'] = '3'
        pm.execute_all()
        mock_import.assert_called_once_with()
        calls = [mock.call(ch) for ch in pm.chains[1:3]]
        mock_execute.assert_has_calls(calls, any_order=False)
        mock_persist.assert_called()
        executed_chains = [arg[0] for arg in mock_execute.call_args_list]
        for ch_idx in [0, 3, 4]:
            self.assertNotIn(pm.chains[ch_idx], executed_chains)

    @unittest.skip('Need to fix this test.')
    @mock.patch('eskapade.core.process_manager.ProcessManager.execute', side_effect=_status_side_effect)
    def test_execute_all_status_return(self, mock_execute):
        pm = process_manager
        pm.service(ConfigObject)['analysisName'] = 'test_execute_all_status_return'
        c1 = Chain('1')
        c2 = Chain('2')
        c3 = Chain('fail')
        c4 = Chain('4')
        pm.chains = [c1, c2, c3, c4]
        status = pm.execute_all()
        self.assertEqual(status, StatusCode.Failure)
        executed_chains = [arg[0] for arg in mock_execute.call_args_list]
        self.assertNotIn(c4, executed_chains)

        pm.reset()
        pm.service(ConfigObject)['analysisName'] = 'test_execute_all_status_return'
        mock_execute.reset_mock()
        c1 = Chain('1')
        c2 = Chain('2')
        c3 = Chain('skip')
        c4 = Chain('4')
        pm.chains = [c1, c2, c3, c4]
        status = pm.execute_all()
        self.assertEqual(status, StatusCode.Success)
        executed_chains = [arg[0][0] for arg in mock_execute.call_args_list]
        self.assertIn(c4, executed_chains)

    @mock.patch('eskapade.core.element.Chain.initialize')
    @mock.patch('eskapade.core.element.Chain.execute')
    @mock.patch('eskapade.core.element.Chain.finalize')
    def test_execute(self, mock_finalize, mock_execute, mock_initialize):
        pm = process_manager
        c1 = Chain('1')

        mock_initialize.return_value = StatusCode.Success
        mock_execute.return_value = StatusCode.Success
        mock_finalize.return_value = StatusCode.Success
        mock_parent = mock.MagicMock(autospec=True)
        mock_parent.attach_mock(mock_initialize, 'initialize')
        mock_parent.attach_mock(mock_execute, 'execute')
        mock_parent.attach_mock(mock_finalize, 'finalize')
        calls = [mock.call.initialize(), mock.call.execute(), mock.call.finalize()]
        status = pm.execute(c1)
        mock_parent.assert_has_calls(calls, any_order=False)
        self.assertEqual(pm.prev_chain_name, c1.name)
        self.assertEqual(status, StatusCode.Success)

    def test_execute_status_return(self):
        pm = process_manager
        c2 = Chain('skip')
        c3 = Chain('fail')

        with mock.patch('eskapade.core.element.Chain.initialize', side_effect=_status_side_effect, autospec=True):
            with mock.patch('eskapade.core.element.Chain.execute') as \
                    mock_execute:
                with mock.patch('eskapade.core.element.Chain.finalize') as \
                        mock_finalize:
                    status = pm.execute(c2)
                    self.assertEqual(status, StatusCode.SkipChain)
                    self.assertEqual(pm.prev_chain_name, c2.name)
                    # assert that chain is indeed skipped
                    assert not mock_execute.called
                    assert not mock_finalize.called
                    status = pm.execute(c3)
                    self.assertEqual(status, StatusCode.Failure)

        with mock.patch('eskapade.core.element.Chain.initialize', return_value=StatusCode.Success):
            with mock.patch('eskapade.core.element.Chain.execute', side_effect=_status_side_effect, autospec=True):
                status = pm.execute(c3)
                self.assertEqual(status, StatusCode.Failure)

        with mock.patch('eskapade.core.element.Chain.initialize', return_value=StatusCode.Success):
            with mock.patch('eskapade.core.element.Chain.execute', return_value=StatusCode.Success):
                with mock.patch('eskapade.core.element.Chain.finalize', side_effect=_status_side_effect,
                                autospec=True):
                    status = pm.execute(c3)
                    self.assertEqual(status, StatusCode.Failure)

    def tearDown(self):
        from eskapade.core import execution
        execution.reset_eskapade()
