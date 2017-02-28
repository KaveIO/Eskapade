import unittest
import mock

from .. import execution
from ..process_services import ProcessService


class ProcessServiceTest(unittest.TestCase):

    @mock.patch('eskapade.core.process_services.ProcessService.__init__')
    def test_create(self, mock_init):
        ps = mock.Mock(name='process_service_instance')
        ps_cls = mock.Mock(name='process_service_class', return_value=ps)
        ps_ = ProcessService.create.__func__(ps_cls)
        ps_cls.assert_called()
        mock_init.assert_called_with(ps)
        self.assertIs(ps_, ps)

    def tearDown(self):
        execution.reset_eskapade()
