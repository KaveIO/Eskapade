import numpy as np
import unittest


class TestDmUtils(unittest.TestCase):

    def test_scaled_chi(self):

        from eskapade.data_mimic.data_mimic_util import scaled_chi

        np.random.seed(42)
        A = np.random.randint(0, 50, (3, 3))
        B = np.random.randint(0, 50, (3, 3))

        out = scaled_chi(A, B)

        self.assertEqual(out, (94.25618041980256, 0.0))

    def test_wr_kernel(self):

        from eskapade.data_mimic.data_mimic_util import wr_kernel

        out = wr_kernel(0.4, 2, np.arange(0.5))
        self.assertEqual(out.sum(), 1)

    def test_kde_resample(self):

        from eskapade.data_mimic.data_mimic_util import kde_resample

        out = kde_resample(10, np.random.normal(0, 1, 100), [0.2], 'c', [])
        self.assertTrue(len(out), 2)
        self.assertTrue(len(out[0]), 10)
