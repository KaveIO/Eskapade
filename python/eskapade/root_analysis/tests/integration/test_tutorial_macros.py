import os
import pandas as pd

import ROOT

from eskapade.tests.integration.test_tutorial_macros import TutorialMacrosTest
from eskapade.core import persistence
from eskapade import ProcessManager, ConfigObject, DataStore
from eskapade.root_analysis import RooFitManager


class RootAnalysisTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on root-analysis tutorial macros"""

    def test_esk401(self):
        """Test Esk-401: ROOT hist fill, plot, convert"""

        # run Eskapade
        self.run_eskapade('esk401_roothist_fill_plot_convert.py')
        ds = ProcessManager().service(DataStore)

        # histogram checks
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)
        columns = ['x1', 'x2', 'x3', 'x4', 'x5', 'x1:x2', 'x2:x3', 'x4:x5']
        self.assertListEqual(sorted(ds['hist'].keys()), sorted(columns))
        for col in columns:
            self.assertIsInstance(ds['hist'][col], ROOT.TH1)

        # data-generation checks
        self.assertIn('n_correlated_data', ds)
        self.assertEqual(500, ds['n_correlated_data'])
        self.assertIn('n_rdh_x1', ds)
        self.assertEqual(100, ds['n_rdh_x1'])
        self.assertIn('n_rds_x2_vs_x3', ds)
        self.assertEqual(114, ds['n_rds_x2_vs_x3'])

        # roofit objects check
        self.assertIn('hpdf', ds)
        self.assertIsInstance(ds['hpdf'], ROOT.RooHistPdf)
        self.assertIn('rdh_x1', ds)
        self.assertIsInstance(ds['rdh_x1'], ROOT.RooDataHist)
        self.assertIn('rds_x2_vs_x3', ds)
        self.assertIsInstance(ds['rds_x2_vs_x3'], ROOT.RooDataSet)
        self.assertIn('vars_x2_vs_x3', ds)
        self.assertIsInstance(ds['vars_x2_vs_x3'], ROOT.RooArgSet)

        # data-summary checks
        io_conf = ProcessManager().service(ConfigObject).io_conf()
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col.replace(':', '_vs_')) for col in columns]
        for fname in file_names:
            path = persistence.io_path('results_data', io_conf, 'report/{}'.format(fname))
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk402(self):
        """Test Esk-402: RooDataHist fill"""

        # run Eskapade
        self.run_eskapade('esk402_roodatahist_fill.py')
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertIn('n_accounts', ds)
        self.assertEqual(650, ds['n_accounts'])
        self.assertIn('n_rdh_accounts', ds)
        self.assertEqual(650, ds['n_rdh_accounts'])
        self.assertIn('to_factorized', ds)
        self.assertIsInstance(ds['to_factorized'], dict)
        self.assertIn('to_original', ds)
        self.assertIsInstance(ds['to_original'], dict)
        self.assertIn('map_rdh_accounts_to_original', ds)
        self.assertIsInstance(ds['map_rdh_accounts_to_original'], dict)

        # roofit objects check
        self.assertIn('accounts_catset', ds)
        self.assertIsInstance(ds['accounts_catset'], ROOT.RooArgSet)
        self.assertEqual(2, len(ds['accounts_catset']))
        self.assertIn('accounts_varset', ds)
        self.assertIsInstance(ds['accounts_varset'], ROOT.RooArgSet)
        self.assertEqual(6, len(ds['accounts_varset']))
        self.assertIn('rdh_accounts', ds)
        self.assertIsInstance(ds['rdh_accounts'], ROOT.RooDataHist)

    def test_esk403(self):
        """Test Esk-403: RooDataSet convert"""

        # run Eskapade
        self.run_eskapade('esk403_roodataset_convert.py')
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertIn('n_accounts', ds)
        self.assertEqual(650, ds['n_accounts'])
        self.assertIn('n_rds_accounts', ds)
        self.assertEqual(650, ds['n_rds_accounts'])
        self.assertIn('n_df_from_rds', ds)
        self.assertEqual(650, ds['n_df_from_rds'])

        self.assertIn('to_factorized', ds)
        self.assertIsInstance(ds['to_factorized'], dict)
        self.assertIn('to_original', ds)
        self.assertIsInstance(ds['to_original'], dict)
        self.assertIn('rds_to_original', ds)
        self.assertIsInstance(ds['rds_to_original'], dict)

        # roofit objects check
        self.assertIn('accounts_fact_varset', ds)
        self.assertIsInstance(ds['accounts_fact_varset'], ROOT.RooArgSet)
        self.assertEqual(4, len(ds['accounts_fact_varset']))
        self.assertIn('rds_accounts', ds)
        self.assertIsInstance(ds['rds_accounts'], ROOT.RooDataSet)

        # assert that refactored df equals original
        self.assertIn('accounts', ds)
        self.assertIn('df_refact', ds)
        df1 = ds['accounts']
        df2 = ds['df_refact']
        self.assertEqual(len(df1.index), 650)
        self.assertEqual(len(df2.index), 650)
        self.assertTrue('eyeColor' in df1.columns)
        self.assertTrue('favoriteFruit' in df1.columns)
        self.assertTrue('eyeColor' in df2.columns)
        self.assertTrue('favoriteFruit' in df2.columns)
        self.assertListEqual(df1['eyeColor'].values.tolist(), df2['eyeColor'].values.tolist())
        self.assertListEqual(df1['favoriteFruit'].values.tolist(), df2['favoriteFruit'].values.tolist())

    def test_esk404(self):
        """Test Esk-404: Workspace create PDF, simulate, fit, plot"""

        # run Eskapade
        self.run_eskapade('esk404_workspace_createpdf_simulate_fit_plot.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # data-generation checks
        self.assertIn('n_df_simdata', ds)
        self.assertEqual(1000, ds['n_df_simdata'])

        # roofit objects check in datastore
        self.assertIn('fit_result', ds)
        self.assertIsInstance(ds['fit_result'], ROOT.RooFitResult)

        # successful fit result
        fit_result = ds['fit_result']
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        self.assertIn('simdata', ds)
        self.assertIsInstance(ds['simdata'], ROOT.RooDataSet)
        self.assertIn('simdata_plot', ds)
        self.assertIsInstance(ds['simdata_plot'], ROOT.RooPlot)

        # roofit objects check in workspace
        self.assertIn('model', ws)
        self.assertIn('bkg', ws)
        self.assertIn('sig', ws)

    def test_esk405(self):
        """Test Esk-405: Simulation based on binned data"""

        # run Eskapade
        self.run_eskapade('esk405_simulation_based_on_binned_data.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # data-generation checks
        self.assertIn('n_rdh_accounts', ds)
        self.assertEqual(650, ds['n_rdh_accounts'])

        # roofit objects check in workspace
        self.assertIn('hpdf_Ndim', ws)
        self.assertIn('rdh_accounts', ws)

        mcats = ws.set('rdh_cats')
        self.assertFalse(not mcats)
        self.assertEqual(1, len(mcats))
        mvars = ws.set('rdh_vars')
        self.assertFalse(not mvars)
        self.assertEqual(3, len(mvars))
        mdata = ws.data('rdh_accounts')
        self.assertEqual(650, mdata.sumEntries())

    def test_esk406(self):
        """Test Esk-406: Simulation based on unbinned data"""

        # run Eskapade
        self.run_eskapade('esk406_simulation_based_on_unbinned_data.py')
        ds = ProcessManager().service(DataStore)

        # data-generation checks
        self.assertIn('n_correlated_data', ds)
        self.assertEqual(500, ds['n_correlated_data'])
        self.assertIn('n_rds_correlated_data', ds)
        self.assertEqual(500, ds['n_rds_correlated_data'])
        self.assertIn('n_df_simdata', ds)
        self.assertEqual(5000, ds['n_df_simdata'])

        self.assertIn('df_simdata', ds)
        self.assertIsInstance(ds['df_simdata'], pd.DataFrame)
        self.assertIn('hist', ds)
        self.assertIsInstance(ds['hist'], dict)

        # roofit objects check
        self.assertIn('keys_varset', ds)
        self.assertIsInstance(ds['keys_varset'], ROOT.RooArgSet)
        self.assertEqual(2, len(ds['keys_varset']))
        self.assertIn('rds_correlated_data', ds)
        self.assertIsInstance(ds['rds_correlated_data'], ROOT.RooDataSet)
        self.assertIn('simdata', ds)
        self.assertIsInstance(ds['simdata'], ROOT.RooDataSet)

    def test_esk407(self):
        """Test Esk-407: Classification unbiased fit estimate"""

        # run Eskapade
        self.run_eskapade('esk407_classification_unbiased_fit_estimate.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # roofit objects check in datastore
        self.assertIn('fit_result', ds)
        self.assertIsInstance(ds['fit_result'], ROOT.RooFitResult)

        # roofit objects check in workspace
        mdata = ws.data('data')
        self.assertFalse(not mdata)
        self.assertEqual(1000, mdata.numEntries())
        mpdf = ws.pdf('hist_model')
        self.assertFalse(not mpdf)

        # successful fit result
        fit_result = ds['fit_result']
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        lo_risk = ws.var('N_low_risk')
        self.assertFalse(not lo_risk)
        self.assertTrue(lo_risk.getVal() < 1000)
        self.assertTrue(lo_risk.getError() > 0)
        hi_risk = ws.var('N_high_risk')
        self.assertFalse(not hi_risk)
        self.assertTrue(hi_risk.getVal() > 0)
        self.assertTrue(hi_risk.getError() > 0)

    def test_esk408(self):
        """Test Esk-408: Classification error propagation after fit"""

        # run Eskapade
        self.run_eskapade('esk408_classification_error_propagation_after_fit.py')
        ds = ProcessManager().service(DataStore)
        ws = ProcessManager().service(RooFitManager).ws

        # data-generation checks
        self.assertIn('n_df_pvalues', ds)
        self.assertEqual(500, ds['n_df_pvalues'])
        self.assertIn('df_pvalues', ds)
        self.assertIsInstance(ds['df_pvalues'], pd.DataFrame)
        df = ds['df_pvalues']
        self.assertTrue('high_risk_pvalue' in df.columns)
        self.assertTrue('high_risk_perror' in df.columns)

        # roofit objects check in workspace
        fit_result = ws.obj('fit_result')
        self.assertFalse(not fit_result)
        self.assertIsInstance(fit_result, ROOT.RooFitResult)
        # test for successful fit result
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        frac = ws.var('frac')
        self.assertFalse(not frac)
        self.assertTrue(frac.getVal() > 0)
        self.assertTrue(frac.getError() > 0)

    def test_esk409(self):
        """Test Esk-409: Unredeemed vouchers"""

        # run Eskapade
        self.run_eskapade('esk409_unredeemed_vouchers.py')
        proc_mgr = ProcessManager()
        ds = proc_mgr.service(DataStore)

        # check generated data
        self.assertIn('voucher_redeems', ds)
        self.assertIn('voucher_ages', ds)
        self.assertIsInstance(ds['voucher_redeems'], ROOT.RooDataSet)
        self.assertIsInstance(ds['voucher_ages'], ROOT.RooDataSet)
        self.assertLess(ds['voucher_redeems'].numEntries(), 6000)
        self.assertGreater(ds['voucher_redeems'].numEntries(), 0)
        self.assertEqual(ds['voucher_ages'].numEntries(), 10000)

        # check fit result
        fit_link = proc_mgr.get_chain('Fitting').get_link('Fit')
        self.assertEqual(fit_link.fit_result.status(), 0)
        n_ev_pull = (fit_link.results['n_ev'][0] - 6000.) / fit_link.results['n_ev'][1]
        self.assertGreater(n_ev_pull, -3.)
        self.assertLess(n_ev_pull, 3.)

        # check plot output
        plot_path = persistence.io_path('results_data', proc_mgr.service(ConfigObject).io_conf(), 'voucher_redeem.pdf')
        self.assertTrue(os.path.exists(plot_path))
        statinfo = os.stat(plot_path)
        self.assertGreater(statinfo.st_size, 0)
