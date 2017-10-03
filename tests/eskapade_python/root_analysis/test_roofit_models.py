import functools
import itertools
import unittest
import unittest.mock as mock

import ROOT

from eskapade.root_analysis.roofit_models import RooFitModel, TruncExponential


class RooFitModelTest(unittest.TestCase):
    """Tests for RooFit-model base class"""

    @mock.patch('eskapade.root_analysis.roofit_utils.load_libesroofit')
    def test_init(self, mock_load_lib):
        """Test initialization of RooFit model"""

        # test normal initialization
        for name, load_lib in itertools.product(('', 'my_name'), (False, True)):
            # call init method
            mock_model = mock.Mock(name='mock_roofit_model', spec=RooFitModel)
            mock_ws = mock.Mock(name='mock_rooworkspace', spec=ROOT.RooWorkspace)
            RooFitModel.__init__(mock_model, mock_ws, name=name, load_libesroofit=load_lib)

            # check name
            self.assertIsInstance(mock_model._name, str, 'model name is not a string with name "{}"'.format(name))
            self.assertTrue(bool(mock_model._name), 'no model name set with name "{}"'.format(name))
            if name:
                self.assertEqual(mock_model._name, name, 'incorrect name set')

            # check other attributes
            self.assertIs(mock_model._ws, mock_ws, 'incorrect workspace set')
            self.assertFalse(bool(mock_model._is_built), 'incorrect value for "is_built" attribute set')
            self.assertFalse(bool(mock_model._pdf_name), 'incorrect value for "pdf_name" attribute set')

            # test library loading
            if load_lib:
                mock_load_lib.assert_called_once_with()
            else:
                mock_load_lib.assert_not_called()
            mock_load_lib.reset_mock()

        # test initialization with workspace of wrong type
        mock_model = mock.Mock(name='mock_roofit_model', spec=RooFitModel)
        mock_ws = mock.Mock(name='mock_rooworkspace')
        with self.assertRaises(TypeError):
            RooFitModel.__init__(mock_model, mock_ws)


class TruncExponentialTest(unittest.TestCase):
    """Tests for truncated-exponential RooFit model"""

    @mock.patch('eskapade.root_analysis.roofit_models.RooFitModel.__init__')
    @mock.patch('eskapade.root_analysis.roofit_models.super')
    def test_init(self, mock_super, mock_init):
        """Test initialization of truncated-exponential RooFit model"""

        # create mock objects for testing
        mock_trunc_exp = mock.Mock(name='mock_trunc_exp')
        mock_trunc_exp.name = 'my_trunc_exp'
        mock_ws = mock.Mock(name='mock_workspace')
        mock_roofit_model = type('MockRooFitModel', (), {})()
        mock_roofit_model.__init__ = functools.partial(mock_init, mock_trunc_exp)
        mock_super.return_value = mock_roofit_model

        # test initialization with default and custom parameters
        par_vals = ({}, dict(var_range=(-1, 3), var=('myvar', 1), max_var=('mymaxvar', 2),
                             exp=[('exp1', -5), ('exp2', -11), ('exp3', -17)], fracs=[('f1', 0.1), ('f2', 0.2)]))
        par_vals_ = (dict(var_range=(0., 1.), var=('var', 0.), max_var=('max_var', 1.), exp=[('exp', -1.)], fracs=[]),
                     dict(var_range=(-1., 3.), var=('myvar', 1.), max_var=('mymaxvar', 2.),
                          exp=[('exp1', -5.), ('exp2', -11.), ('exp3', -17.)], fracs=[('f1', 0.1), ('f2', 0.2)]))
        for vals, vals_ in zip(par_vals, par_vals_):
            TruncExponential.__init__(mock_trunc_exp, mock_ws, name='my_trunc_exp', **vals)
            mock_super.assert_any_call(TruncExponential, mock_trunc_exp)
            mock_init.assert_called_once_with(mock_trunc_exp, mock_ws, name='my_trunc_exp', load_libesroofit=True)
            self.assertEqual(mock_trunc_exp._pdf_name, 'my_trunc_exp', 'incorrect PDF name')
            self.assertTupleEqual(mock_trunc_exp._var_range, vals_['var_range'], 'incorrect value for "var_range"')
            self.assertTupleEqual(mock_trunc_exp._var, vals_['var'], 'incorrect value for "var"')
            self.assertTupleEqual(mock_trunc_exp._max_var, vals_['max_var'], 'incorrect value for "max_var"')
            self.assertListEqual(mock_trunc_exp._exp, vals_['exp'], 'incorrect value for "exp"')
            self.assertListEqual(mock_trunc_exp._fracs, vals_['fracs'], 'incorrect value for "fracs"')
            mock_super.reset_mock()
            mock_init.reset_mock()

    def test_build_model(self):
        """Test building truncated-exponential RooFit model"""

        # create mock objects for testing
        mock_trunc_exp = mock.Mock(name='mock_trunc_exp')
        mock_trunc_exp.name = 'my_trunc_exp'
        mock_trunc_exp._is_built = False
        mock_ws = mock.MagicMock(name='mock_workspace')
        mock_trunc_exp.ws = mock_ws
        mock_trunc_exp._var_range = (-1., 3.)
        mock_trunc_exp._var = ('myvar', 1.)
        mock_trunc_exp._max_var = ('mymaxvar', 2.)
        mock_trunc_exp._exp = [('exp1', -5.), ('exp2', -11.), ('exp3', -17.)]
        mock_trunc_exp._fracs = [('f1', 0.1), ('f2', 0.2)]

        # test normal build
        mock_trunc_exp.pdf = mock.Mock(name='mock_pdf', spec=ROOT.RooAbsPdf)
        TruncExponential.build_model(mock_trunc_exp)
        self.assertTrue(any(c[0] and isinstance(c[0][0], str) and
                            c[0][0].startswith('TruncExponential::my_trunc_exp(') and
                            c[0][0].endswith(')') for c in mock_ws.factory.call_args_list),
                        'no factory call to create PDF')
        mock_ws.assert_has_calls([mock.call.defineSet(*a) for a in [('var_set', 'myvar'),
                                                                    ('max_var_set', 'mymaxvar'),
                                                                    ('all_vars_set', 'myvar,mymaxvar')]],
                                 any_order=True)
        self.assertTrue(mock_trunc_exp._is_built)

        # test failed build
        mock_trunc_exp.pdf = mock.Mock(name='mock_pdf')
        with self.assertRaises(RuntimeError):
            TruncExponential.build_model(mock_trunc_exp)

    @unittest.skip('Please investigate and fix or rewrite me!!')
    @mock.patch('ROOT.RooArgSet')
    @mock.patch('ROOT.RooConstVar')
    @mock.patch('ROOT.RooDataWeightedAverage')
    def test_create_norm(self, mock_dwa, mock_constvar, mock_arg_set):
        """Test creating norm of truncated-exponential RooFit model"""

        mock_trunc_exp = mock.Mock(name='mock_trunc_exp')
        mock_trunc_exp.name = 'my_trunc_exp'
        mock_pdf = mock.MagicMock(name='mock_pdf')
        mock_trunc_exp.pdf = mock_pdf
        mock_data = mock.MagicMock(name='mock_data', spec=ROOT.RooDataSet)
        mock_data.GetName.return_value = 'my_data'
        mock_data.reduce.return_value = mock_data

        # test without data integral
        pdf_full, pdf_data = TruncExponential.create_norm(mock_trunc_exp, data=None, range_min=0., range_max=1.)
        mock_pdf.clone().redirectServers.assert_called_once_with(mock_arg_set(), True)
        mock_pdf.clone().createIntegral.assert_called_once_with(mock_trunc_exp.var_set, mock_arg_set())
        self.assertIs(pdf_full, mock_pdf.clone().createIntegral(), 'unexpected full PDF integral')
        self.assertIs(pdf_data, mock_pdf.clone().createIntegral().clone(), 'unexpected data PDF integral')
        mock_constvar.assert_called_with(mock_trunc_exp.max_var.GetName(), '', 1e+30)
        mock_pdf.reset_mock()
        mock_dwa.reset_mock()
        mock_constvar.reset_mock()

        # test with data integral
        pdf_full, pdf_data = TruncExponential.create_norm(mock_trunc_exp, data=mock_data, range_min=0., range_max=1.)
        self.assertEqual(mock_pdf.clone().redirectServers.call_count, 2,
                         'expected exactly two redirect-server calls on PDF clones')
        self.assertEqual(mock_pdf.clone().createIntegral.call_count, 2,
                         'expected exactly two create-integral calls on PDF clones')
        mock_dwa.assert_called_once_with()
        self.assertIn(mock_pdf.clone().createIntegral(), mock_dwa.call_args[0],
                      'RooDataWeightedAverage not called with integral of PDF clone')
        self.assertIs(pdf_full, mock_pdf.clone().createIntegral(), 'unexpected full PDF integral')
        self.assertIs(pdf_data, mock_dwa(), 'unexpected data PDF integral')
        mock_constvar.assert_called_with()
