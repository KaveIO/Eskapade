import unittest
import unittest.mock as mock

from eskapade.core.definitions import StatusCode
from eskapade.core.process_manager import process_manager
from eskapade.core.process_services import ConfigObject
from eskapade.core.process_services import ProcessService
from eskapade.core.element import Chain, Link


class ProcessServiceMock(ProcessService):
    pass


class ProcessManagerTest(unittest.TestCase):

    @mock.patch('eskapade.core.process_services.ProcessService.create')
    def test_service(self, mock_create):
        pm = process_manager

        # register service by specifying type
        ps = ProcessServiceMock()
        mock_create.return_value = ps
        ps_ = process_manager.service(ProcessServiceMock)
        self.assertIn(ProcessServiceMock, pm._services)
        self.assertIs(ps_, ps)
        self.assertIs(pm._services[ProcessServiceMock], ps)
        pm.reset()

        # register service by specifying instance
        ps = ProcessServiceMock()
        ps_ = process_manager.service(ps)
        self.assertIn(ProcessServiceMock, pm._services)
        self.assertIs(ps_, ps)
        self.assertIs(pm._services[ProcessServiceMock], ps)
        pm.reset()

        # register service with wrong type
        with self.assertRaises(TypeError):
            process_manager.service(object)
        pm.reset()

    def test_chain(self):
        pm = process_manager
        c = 'dummy'

        with self.assertRaises(TypeError):
            pm.add(c)

        chain = Chain(c, pm)
        with self.assertRaises(KeyError):
            pm.add(chain)

    def test_discard(self):
        pm = process_manager
        one = Chain('one', pm)
        self.assertTrue(pm.n_chains == 1, 'Process manager has no chains!')
        pm.discard(one)
        self.assertTrue(pm.n_chains == 0, 'Process manager has chains!')

    def test_clear(self):
        pm = process_manager
        Chain('one', pm)
        Chain('two', pm)
        Chain('three', pm)

        pm.clear()
        self.assertFalse(pm.n_chains == 3, 'Process manager has chains!')

    def test_reset(self):
        pm = process_manager
        Chain('dummy', pm)
        self.assertTrue(pm, 'Process manager has no chains!')
        pm.reset()
        self.assertFalse(pm, 'Process manager has chains!')

    def test_initialize(self):
        pm = process_manager
        one = Chain('1', pm)
        two = Chain('2', pm)
        three = Chain('3', pm)

        status = pm.initialize()

        self.assertEqual(one.prev_chain_name, '')
        self.assertEqual(two.prev_chain_name, '1')
        self.assertEqual(three.prev_chain_name, '2')
        self.assertTrue(status == StatusCode.Success, 'Process Manager failed to initialize!')

    def test_execute_status_return(self):
        pm = process_manager
        pm.service(ConfigObject)['analysisName'] = 'test_execute_status_return'

        class Success(Link):
            chains = []

            def execute(self):
                self.chains.append(self.parent.name)
                return StatusCode.Success

        c1 = Chain('1', pm)
        c1.add(Success('1'))
        c2 = Chain('2', pm)
        c2.add(Success('2'))
        c3 = Chain('fail', pm)

        class Fail(Link):
            def execute(self):
                return StatusCode.Failure

        c3.add(Fail('fail'))

        c4 = Chain('4', pm)
        c4.add(Success('4'))

        status = pm.execute()

        self.assertEqual(status, StatusCode.Failure, 'Process Manager failed to fail!')
        self.assertEqual(Success.chains, ['1', '2'])

        pm.reset()

        pm.service(ConfigObject)['analysisName'] = 'test_execute_all_status_return'

        Success.chains = list()

        c1 = Chain('1', pm)
        c1.add(Success('1'))
        c2 = Chain('2', pm)
        c2.add(Success('2'))
        c3 = Chain('skip')

        class Skip(Link):
            def execute(self):
                return StatusCode.SkipChain

        c3.add(Skip('skip'))

        c4 = Chain('4', pm)
        c4.add(Success('4'))

        status = pm.execute()

        self.assertEqual(status, StatusCode.Success)
        self.assertEqual(Success.chains, ['1', '2', '4'])

    def tearDown(self):
        from eskapade.core import execution
        execution.reset_eskapade()
