import unittest
import numpy as np

from eskapade.tests.observers import TestCaseObservable
from eskapade.analysis.histogram import Histogram


class HistogramTest(unittest.TestCase, TestCaseObservable):

    def setUp(self):
        pass

    def test_simulate(self):
        size = 10000
        bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        values = [0.03846154, 0.07692308, 0.11538462, 0.15384615, 0.19230769, 0.15384615, 0.11538462, 0.07692308,
                  0.03846154, 0.03846154]
        h = Histogram((values, bins), variable='x')
        data, h_sim = h.simulate(size)

        self.assertTrue(len(data) == size)
        v, b = np.histogram(data, bins=bins)
        self.assertTrue(np.allclose(np.divide(v, v.sum()), values, rtol=0, atol=0.02))

    def test_to_normalized(self):
        bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        values = [1, 2, 3, 4, 5, 4, 3, 2, 1, 1]
        h = Histogram((values, bins), variable='x')
        h_norm = h.to_normalized()
        values_norm, bins_norm = h_norm.get_bin_vals()

        self.assertTrue(abs(values_norm.sum() - 1) < 0.01)

    def test_surface(self):
        bins = [0, 1, 3, 4, 6, 7]
        values = [3, 2, 4, 2, 5]
        h = Histogram((values, bins), variable='x')
        s = h.surface()

        self.assertTrue(s == 20)

    def tearDown(self):
        pass
