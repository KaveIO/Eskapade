import os
import unittest
from glob import glob

import ROOT
import pandas as pd

from eskapade import process_manager, resources, ConfigObject, DataStore
from eskapade.core import persistence
from eskapade.root_analysis.roofit_manager import RooFitManager
from eskapade_python.bases import TutorialMacrosTest


class RootAnalysisTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on root-analysis tutorial macros"""

    def test_esk401(self):
        """Test Esk-401: ROOT hist fill, plot, convert."""
        # run Eskapade
        self.eskapade_run(resources.tutorial('esk401_roothist_fill_plot_convert.py'))
        ds = process_manager.service(DataStore)

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
        self.assertEqual(40, ds['n_rdh_x1'])
        self.assertIn('n_rds_x2_vs_x3', ds)
        self.assertEqual(23, ds['n_rds_x2_vs_x3'])

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
        file_names = ['report.tex'] + ['hist_{}.pdf'.format(col.replace(':', '_vs_')) for col in columns]
        for fname in file_names:
            path = persistence.io_path('results_data', 'report/{}'.format(fname))
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk402(self):
        """Test Esk-402: RooDataHist fill"""

        # run Eskapade
        self.eskapade_run(resources.tutorial('esk402_roodatahist_fill.py'))
        ds = process_manager.service(DataStore)

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
        self.eskapade_run(resources.tutorial('esk403_roodataset_convert.py'))
        ds = process_manager.service(DataStore)

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
        self.eskapade_run(resources.tutorial('esk404_workspace_createpdf_simulate_fit_plot.py'))
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

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
        self.eskapade_run(resources.tutorial('esk405_simulation_based_on_binned_data.py'))
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

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
        macro = resources.tutorial('esk406_simulation_based_on_unbinned_data.py')
        self.eskapade_run(macro)
        ds = process_manager.service(DataStore)

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
        """Test Esk-407: Classification unbiased fit estimate."""
        # run Eskapade
        macro = resources.tutorial('esk407_classification_unbiased_fit_estimate.py')
        self.eskapade_run(macro)
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

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
        """Test Esk-408: Classification error propagation after fit."""
        # run Eskapade
        self.eskapade_run(resources.tutorial('esk408_classification_error_propagation_after_fit.py'))
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

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

    @unittest.skip('The new chain interface does not have a method get. '
                   'BTW how do I know which chains/links are defined?')
    def test_esk409(self):
        """Test Esk-409: Unredeemed vouchers."""
        # run Eskapade
        macro = resources.tutorial('esk409_unredeemed_vouchers.py')
        self.eskapade_run(macro)
        ds = process_manager.service(DataStore)

        # check generated data
        self.assertIn('voucher_redeems', ds)
        self.assertIn('voucher_ages', ds)
        self.assertIsInstance(ds['voucher_redeems'], ROOT.RooDataSet)
        self.assertIsInstance(ds['voucher_ages'], ROOT.RooDataSet)
        self.assertLess(ds['voucher_redeems'].numEntries(), 6000)
        self.assertGreater(ds['voucher_redeems'].numEntries(), 0)
        self.assertEqual(ds['voucher_ages'].numEntries(), 10000)

        # check fit result
        fit_link = process_manager.get_chain('Fitting').get_link('Fit')
        self.assertEqual(fit_link.fit_result.status(), 0)
        n_ev_pull = (fit_link.results['n_ev'][0] - 6000.) / fit_link.results['n_ev'][1]
        self.assertGreater(n_ev_pull, -3.)
        self.assertLess(n_ev_pull, 3.)

        # check plot output
        plot_path = persistence.io_path('results_data', 'voucher_redeem.pdf')
        self.assertTrue(os.path.exists(plot_path))
        statinfo = os.stat(plot_path)
        self.assertGreater(statinfo.st_size, 0)

    def test_esk410(self):
        """Test Esk-410: Hypothesis test of categorical observables."""
        # run Eskapade
        macro = resources.tutorial('esk410_testing_correlations_between_categories.py')
        self.eskapade_run(macro)
        ds = process_manager.service(DataStore)

        # report checks
        self.assertIn('report_pages', ds)
        self.assertIsInstance(ds['report_pages'], list)
        self.assertEqual(12, len(ds['report_pages']))

        # data-summary checks
        settings = process_manager.service(ConfigObject)
        file_names = ['report.tex']
        for fname in file_names:
            path = '{0:s}/{1:s}/data/v0/report/{2:s}'.format(settings['resultsDir'], settings['analysisName'], fname)
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertTrue(statinfo.st_size > 0)

    def test_esk411(self):
        """Test Esk-411: Predictive maintenance Weibull fit."""
        # run Eskapade
        macro = resources.tutorial('esk411_weibull_predictive_maintenance.py')
        self.eskapade_run(macro)
        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # roofit objects check in datastore
        self.assertIn('fit_result', ds)
        self.assertIsInstance(ds['fit_result'], ROOT.RooFitResult)

        # roofit objects check in workspace
        self.assertIn('binnedData', ds)
        self.assertIsInstance(ds['binnedData'], ROOT.RooDataHist)
        mdata = ds['binnedData']
        self.assertTrue(mdata)
        self.assertEqual(300, mdata.numEntries())
        mpdf = ws.pdf('sum3pdf')
        self.assertTrue(mpdf)

        # successful fit result
        fit_result = ds['fit_result']
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        n1 = ws.var('N1')
        self.assertTrue(n1)
        self.assertGreater(n1.getVal(), 2.e5)
        n2 = ws.var('N2')
        self.assertTrue(n2)
        self.assertGreater(n2.getVal(), 4.e5)
        n3 = ws.var('N3')
        self.assertTrue(n3)
        self.assertGreater(n3.getVal(), 5.e4)

        # data-summary checks
        file_names = ['weibull_fit_report.tex', 'correlation_matrix_fit_result.pdf', 'floating_pars_fit_result.tex',
                      'fit_of_time_difference_medium_range.pdf']
        for fname in file_names:
            path = persistence.io_path('results_data', 'report/{}'.format(fname))
            self.assertTrue(os.path.exists(path))
            statinfo = os.stat(path)
            self.assertGreater(statinfo.st_size, 0)

    def test_tutorial5(self):
        """Test Tutorial 5: Workspace create PDF, simulate, fit, plot."""
        my_pdf = 'MyPdf'

        def remove_pdf():
            # cleanup of temporary pdf files
            rm_files = glob(my_pdf+'.*') + glob(my_pdf+'_cxx*') + glob('doesnotexit.cxx')
            for rm_file in rm_files:
                os.remove(rm_file)

        self.addCleanup(remove_pdf)

        # run Eskapade
        macro = resources.tutorial('tutorial_5.py')
        self.eskapade_run(macro)

        # check existence of class MyPdfV3
        cl = ROOT.TClass.GetClass(my_pdf)
        self.assertTrue(cl)

        ds = process_manager.service(DataStore)
        ws = process_manager.service(RooFitManager).ws

        # roofit objects check in datastore
        self.assertIn('fit_result', ds)
        self.assertIsInstance(ds['fit_result'], ROOT.RooFitResult)

        # successful fit result
        fit_result = ds['fit_result']
        self.assertEqual(0, fit_result.status())
        self.assertEqual(3, fit_result.covQual())

        # data-generation checks
        self.assertIn('simdata', ds)
        self.assertIsInstance(ds['simdata'], ROOT.RooDataSet)
        self.assertEqual(400, ds['simdata'].numEntries())

        self.assertIn('simdata_plot', ds)
        self.assertIsInstance(ds['simdata_plot'], ROOT.RooPlot)

        # roofit objects check in workspace
        self.assertIn('testpdf', ws)
