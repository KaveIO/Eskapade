import unittest
import unittest.mock as mock

from eskapade.root_analysis.roofit_manager import RooFitManager
from eskapade.root_analysis.roofit_models import RooFitModel


class RooFitManagerTest(unittest.TestCase):
    """Tests for RooFit-manager process service"""

    def test_persist(self):
        """Test persistence flag of RooFit manager"""

        self.assertTrue(RooFitManager._persist, 'RooFit manager is not persisting')

    def test_init(self):
        """Test initialization of RooFit manager"""

        mock_rfm = mock.Mock(name='mock_roofit_manager')
        RooFitManager.__init__(mock_rfm)
        self.assertFalse(bool(mock_rfm._ws), 'incorrect initialization of workspace attribute')
        self.assertDictEqual(mock_rfm._models, {}, 'incorrect initialization of models attribute')

    @mock.patch('ROOT.SetOwnership')
    @mock.patch('ROOT.RooWorkspace')
    def test_ws(self, mock_roows, mock_set_ownership):
        """Test creation of RooFit workspace"""

        mock_rfm = mock.Mock(name='mock_roofit_manager')
        created_roows = mock.Mock(name='CreatedRooWorkspace')
        existing_roows = mock.Mock('ExistingRooWorkspace')
        mock_roows.return_value = created_roows

        # test returning of existing workspace
        mock_rfm._ws = existing_roows
        ws = RooFitManager.ws.__get__(mock_rfm)
        mock_roows.assert_not_called()
        mock_set_ownership.assert_not_called()
        self.assertIs(ws, existing_roows, 'incorrect workspace returned')
        self.assertIs(mock_rfm._ws, existing_roows, 'incorrect workspace set')

        # test returning of created workspace
        mock_rfm._ws = None
        ws = RooFitManager.ws.__get__(mock_rfm)
        mock_roows.assert_called_once_with('esws', 'Eskapade workspace')
        mock_set_ownership.assert_called_once_with(created_roows, False)
        self.assertIs(ws, created_roows, 'incorrect workspace set')
        self.assertIs(mock_rfm._ws, created_roows, 'incorrect workspace returned')

        # test workspace cannot be set
        with self.assertRaises(AttributeError):
            RooFitManager.ws.__set__(mock_rfm, existing_roows)

    def test_set_var_vals(self):
        """Test setting variable values in workspace"""

        # create mock RooFit manager with workspace
        mock_rfm = mock.MagicMock(name='mock_roofit_manager')
        mock_vars = dict(var1=mock.MagicMock(name='var1'), var2=mock.MagicMock(name='var2'))
        mock_vars_ = mock_vars.copy()
        mock_rfm.ws.obj.side_effect = lambda v: mock_vars.get(v)
        mock_rfm.ws.var.side_effect = lambda v: mock_vars.get(v)
        mock_rfm.ws.__getitem__.side_effect = lambda v: mock_vars.get(v)

        # test setting values
        vals = [('var1', (3.14, 0.1)), ('var2', (42., 1.))]
        RooFitManager.set_var_vals(mock_rfm, dict(vals))
        for var, (val, err) in vals:
            mock_vars_[var].assert_has_calls([mock.call.setVal(val), mock.call.setError(err)], any_order=True)

    def test_model(self):
        """Test obtaining RooFit model from RooFit manager"""

        mock_rfm = mock.Mock(name='mock_roofit_manager')
        mock_model = mock.Mock(name='mock_roofit_model', spec=RooFitModel)
        mock_model_cls = mock.Mock(name='RooFitModel')
        mock_model_cls.return_value = mock_model

        # test obtaining exising model
        mock_rfm._models = {'my_model': mock_model}
        model = RooFitManager.model(mock_rfm, 'my_model')
        self.assertIs(model, mock_model, 'incorrect model returned')
        self.assertDictEqual(mock_rfm._models, {'my_model': mock_model}, 'incorrect model set')
        mock_rfm._models = {}

        # test obtaining non-existing model
        model = RooFitManager.model(mock_rfm, 'my_model')
        self.assertIs(model, None, 'incorrect model returned')
        self.assertDictEqual(mock_rfm._models, {}, 'incorrect model set')

        # test obtaining created model
        mock_kwarg = mock.Mock('mock_kwarg')
        model = RooFitManager.model(mock_rfm, 'my_model', model_cls=mock_model_cls, my_kwarg=mock_kwarg)
        self.assertIs(model, mock_model, 'incorrect model returned')
        self.assertDictEqual(mock_rfm._models, {'my_model': mock_model}, 'incorrect model set')
        mock_model_cls.assert_called_once_with(mock_rfm.ws, 'my_model', my_kwarg=mock_kwarg)
        mock_rfm._models = {}
        mock_model_cls.reset_mock()

        # test creating model which is not a RooFitModel
        mock_model_cls.return_value = mock.Mock(name='NotARooFitModel')
        with self.assertRaises(TypeError):
            RooFitManager.model(mock_rfm, 'my_model', model_cls=mock_model_cls)
        self.assertDictEqual(mock_rfm._models, {}, 'incorrect model set')
        mock_model_cls.reset_mock()
