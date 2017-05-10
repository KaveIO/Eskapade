import pandas as pd
import numpy as np

from eskapade.tests.integration.test_tutorial_macros import TutorialMacrosTest
from eskapade import ProcessManager, DataStore


class DataQualityTutorialMacrosTest(TutorialMacrosTest):
    """Integration tests based on data quality tutorial macros"""

    def test_esk501(self):
        """Test Esk-501: fixing pandas dataframe"""

        # run Eskapade
        self.run_eskapade('esk501_fix_pandas_dataframe.py')
        ds = ProcessManager().service(DataStore)

        self.assertIn('vrh', ds)
        self.assertIn('vrh_fix1', ds)
        self.assertIn('vrh_fix2', ds)
        self.assertIn('vrh_fix3', ds)

        self.assertIsInstance(ds['vrh'], pd.DataFrame)
        self.assertIsInstance(ds['vrh_fix1'], pd.DataFrame)
        self.assertIsInstance(ds['vrh_fix2'], pd.DataFrame)
        self.assertIsInstance(ds['vrh_fix3'], pd.DataFrame)

        self.assertEqual(len(ds['vrh'].index), 5)
        self.assertEqual(len(ds['vrh_fix1'].index), 5)
        self.assertEqual(len(ds['vrh_fix2'].index), 5)
        self.assertEqual(len(ds['vrh_fix3'].index), 5)

        self.assertIsInstance(ds['vrh']['B'].dtype, np.object)
        self.assertIsInstance(ds['vrh']['C'].dtype, np.object)
        self.assertIsInstance(ds['vrh']['D'].dtype.type(), np.float64)

        self.assertListEqual(ds['vrh']['A'].values.tolist(), [True, False, np.nan, np.nan, np.nan])
        self.assertListEqual(ds['vrh']['B'].values.tolist(), ['foo', 'bar', '3', np.nan, np.nan])
        self.assertListEqual(ds['vrh']['C'].values.tolist(), ['1.0', '2.0', 'bal', np.nan, np.nan])
        self.assertListEqual(ds['vrh']['D'].values.tolist()[:3], [1.0, 2.0, 3.0])
        self.assertListEqual(ds['vrh']['E'].values.tolist(), ['1', '2', 'bla', np.nan, np.nan])
        self.assertListEqual(ds['vrh']['F'].values.tolist(), ['1', '2.5', 'bar', np.nan, np.nan])
        self.assertListEqual(ds['vrh']['G'].values.tolist(), ['a', 'b', 'c', 'd', np.nan])
        self.assertListEqual(ds['vrh']['H'].values.tolist(), ['a', 'b', '1', '2', '3'])

        self.assertListEqual(ds['vrh_fix1']['A'].values.tolist(), ['True', 'False', np.nan, np.nan, np.nan])
        self.assertListEqual(ds['vrh_fix1']['B'].values.tolist(), ['foo', 'bar', '3', np.nan, np.nan])
        self.assertListEqual(ds['vrh_fix1']['C'].values.tolist()[:2], [1.0, 2.0])
        self.assertListEqual(ds['vrh_fix1']['D'].values.tolist()[:3], [1.0, 2.0, 3.0])
        self.assertListEqual(ds['vrh_fix1']['E'].values.tolist()[:2], [1, 2])
        self.assertListEqual(ds['vrh_fix1']['F'].values.tolist()[:3], ['1', '2.5', 'bar'])
        self.assertListEqual(ds['vrh_fix1']['G'].values.tolist(), ['a', 'b', 'c', 'd', np.nan])
        self.assertListEqual(ds['vrh_fix1']['H'].values.tolist()[2:5], [1.0, 2.0, 3.0])

        self.assertListEqual(ds['vrh_fix2']['B'].values.tolist()[2:3], [3])
        self.assertListEqual(ds['vrh_fix2']['C'].values.tolist(), ['1.0', '2.0', 'bal', np.nan, np.nan])

        self.assertListEqual(ds['vrh_fix3']['A'].values.tolist(),
                             ['True', 'False', 'not_a_bool', 'not_a_bool', 'not_a_bool'])
        self.assertListEqual(ds['vrh_fix3']['B'].values.tolist(), ['foo', 'bar', '3', 'not_a_str', 'not_a_str'])
        self.assertListEqual(ds['vrh_fix3']['C'].values.tolist()[:2], [1.0, 2.0])
        self.assertListEqual(ds['vrh_fix3']['D'].values.tolist()[:3], [1.0, 2.0, 3.0])
        self.assertListEqual(ds['vrh_fix3']['E'].values.tolist(), [1, 2, -999, -999, -999])
        self.assertListEqual(ds['vrh_fix3']['F'].values.tolist(), ['1', '2.5', 'bar', 'not_a_str', 'not_a_str'])
        self.assertListEqual(ds['vrh_fix3']['G'].values.tolist(), ['a', 'b', 'c', 'd', 'GREPME'])
        self.assertListEqual(ds['vrh_fix3']['H'].values.tolist(), [-999, -999, 1, 2, 3])
