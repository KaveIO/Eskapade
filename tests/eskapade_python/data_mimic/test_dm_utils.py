import numpy as np
import pandas as pd
import unittest


class TestDmUtils(unittest.TestCase):

    def test_scaled_chi(self):

        from eskapade.data_mimic.data_mimic_util import scaled_chi

        np.random.seed(42)
        a = np.random.randint(0, 50, (3, 3))
        b = np.random.randint(0, 50, (3, 3))

        out = scaled_chi(a, b)

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

    def test_remove_nans(self):

        from eskapade.data_mimic.data_mimic_util import remove_nans

        input_data = pd.DataFrame(np.random.randint(0, 10, 100))
        ids = np.random.randint(0, 100, 10)
        input_data.loc[ids, :] = np.nan

        out = remove_nans(input_data)

        self.assertFalse(out.isnull().any().values[0])

    def test_aitchison_aitken_kernel(self):

        from eskapade.data_mimic.data_mimic_util import aitchison_aitken_kernel

        input_l = 5
        input_c = 3

        out = aitchison_aitken_kernel(input_l, input_c)

        self.assertTrue(np.array_equal(out,np.array([[ 2.5],[-4. ]])))

    def test_aitchison_aitken_convolution(self):

        from eskapade.data_mimic.data_mimic_util import aitchison_aitken_convolution

        input_l = 5
        input_c = 3

        out = aitchison_aitken_convolution(input_l, input_c)

        self.assertTrue(np.array_equal(out,np.array([[-13.75],[ 28.5]])))

    def test_unorderd_mesh_kernel_values(self):

        from eskapade.data_mimic.data_mimic_util import unorderd_mesh_kernel_values

        input_l = np.array([5,5])
        input_c = np.array([5,5])
        input_n_dim = 2

        out = unorderd_mesh_kernel_values(input_l, input_c, input_n_dim)

        self.assertTrue(np.array_equal(out,np.array([ 1.5625, -5,     -5,     16.    ])))

    def test_unorderd_mesh_convolution_values(self):

        from eskapade.data_mimic.data_mimic_util import unorderd_mesh_convolution_values

        input_l = np.array([5,5])
        input_c = np.array([5,5])
        input_n_dim = 2

        out = unorderd_mesh_convolution_values(input_l, input_c, input_n_dim)

        self.assertTrue(np.array_equal(out,np.array([28.22265625, -118.203125,   -118.203125,    495.0625])))

    def test_unordered_mesh_eval(self):

        from eskapade.data_mimic.data_mimic_util import unordered_mesh_eval

        input_l = np.array([5,5])
        input_c = np.array([5,5])
        input_n_dim = 2
        input_n_ons = 10
        input_delta_frequencies = np.array([1, 0, 5, 3])
        input_cv_delta_frequencies = np.array([3, 1, 2, 1])

        out = unordered_mesh_eval(input_l, input_c, input_n_ons, input_n_dim,
                                  input_delta_frequencies, input_cv_delta_frequencies)

        self.assertTrue(out == 9.097556423611111)

    def test_hash_combinations(self):

        from eskapade.data_mimic.data_mimic_util import hash_combinations

        input_hash_function = np.array([5,5])
        input_combinations = np.array([5,5])

        out = hash_combinations(input_hash_function, input_combinations)
        print(out)

        self.assertTrue(np.array_equal(out,50))

    def test_calculate_delta_frequencies(self):

        from eskapade.data_mimic.data_mimic_util import calculate_delta_frequencies

        input_data = np.array([[5,5],[1,1],[1,5]])
        input_n_obs = 3
        input_n_dim = 2

        out = calculate_delta_frequencies(input_data, input_n_obs, input_n_dim)
        print(out[0])

        self.assertTrue(np.array_equal(out[0], np.array([2, 2, 2, 3])))
        self.assertTrue(np.array_equal(out[1], np.array([2, 2, 2, 0])))

    def test_kde_only_unordered_categorical(self):

        from eskapade.data_mimic.data_mimic_util import kde_only_unordered_categorical

        input_data = np.array([[5,5],[1,1],[1,5],[1,5],[1,5],[1,5],[1,5],[1,5],[1,5]])

        out = kde_only_unordered_categorical(input_data)
        print("Note that this test is stochastic, it may fail with a certain, albeit small, probability!")
        self.assertTrue(out[0] - 0.04049764 <= 0.05)
        self.assertTrue(out[1] - 0.04049764 <= 0.05)

    def test_kde_only_unordered_categorical(self):

        from eskapade.data_mimic.data_mimic_util import kde_only_unordered_categorical

        input_data = np.array([[5,5],[1,1],[1,5],[1,5],[1,5],[1,5],[1,5],[1,5],[1,5]])

        out = kde_only_unordered_categorical(input_data)
        print("Note that this test is stochastic, it may fail with a certain, albeit small, probability!")
        self.assertTrue(out[0] - 0.04049764 <= 0.05)
        self.assertTrue(out[1] - 0.04049764 <= 0.05)

    def test_column_hashing(self):

        from eskapade.data_mimic.data_mimic_util import column_hashing

        input_data = np.array([[5,5],[1,1],[1,5],[1,5],[1,5],[1,5],[1,5],[1,5],[1,5]]).astype(np.float64)
        input_columns_to_hash = np.array(['a'])
        input_column_names = ['a', 'b']
        nput_randomness = 42

        out = column_hashing(input_data, input_columns_to_hash, nput_randomness, input_column_names)

        self.assertTrue(np.array_equal(out, [[0, 5], [1, 1], [1, 5], [1, 5], [1, 5], [1, 5], [1, 5], [1, 5], [1, 5]]))


