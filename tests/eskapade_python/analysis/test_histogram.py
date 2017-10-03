import unittest
from collections import Counter

import numpy as np

from eskapade.analysis.histogram import Histogram, ValueCounts
from eskapade_python.observers import TestCaseObservable


class HistogramTest(unittest.TestCase, TestCaseObservable):
    def setUp(self):
        pass

    def test_simulate(self):
        size = 10000
        bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        values = [0.03846154, 0.07692308, 0.11538462, 0.15384615, 0.19230769, 0.15384615, 0.11538462, 0.07692308,
                  0.03846154, 0.03846154]
        h = Histogram((values, bins), variable='x')
        data, _ = h.simulate(size)

        self.assertTrue(len(data) == size)
        v, _ = np.histogram(data, bins=bins)
        self.assertTrue(np.allclose(np.divide(v, v.sum()), values, rtol=0, atol=0.02))

    def test_to_normalized(self):
        bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        values = [1, 2, 3, 4, 5, 4, 3, 2, 1, 1]
        h = Histogram((values, bins), variable='x')
        h_norm = h.to_normalized()
        values_norm, _ = h_norm.get_bin_vals()

        self.assertTrue(abs(values_norm.sum() - 1) < 0.01)

    def test_surface(self):
        bins = [0, 1, 3, 4, 6, 7]
        values = [3, 2, 4, 2, 5]
        h = Histogram((values, bins), variable='x')
        s = h.surface()

        self.assertTrue(s == 20)

    def test_constructor1(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(cnt, variable='x', bin_specs=bin_specs)

        self.assertIsInstance(h, Histogram)

    def test_constructor2(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        self.assertIsInstance(h, Histogram)

    def test_contents(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        self.assertEqual(h.n_dim, 1)
        self.assertEqual(h.n_bins, 10)
        self.assertEqual(h._val_counts.sum_counts, 45)

    def test_bin_range(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        bin_range = (0, 19)
        self.assertTupleEqual(h.get_bin_range(), bin_range)

    def test_bin_edges(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        # uniform
        bin_edges = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        self.assertListEqual(h.get_uniform_bin_edges(), bin_edges)

        # truncated uniform bin edges
        truncated_bin_edges = [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0]
        self.assertListEqual(h.truncated_bin_edges([5.5, 12.5]), truncated_bin_edges)

        h_bin_edges = h.bin_edges()
        self.assertIsInstance(h_bin_edges, np.ndarray)
        self.assertListEqual(h_bin_edges.tolist(), bin_edges)

    def test_bin_centers(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        bin_centers = [0.5, 2.5, 4.5, 6.5, 8.5, 10.5, 12.5, 14.5, 16.5, 18.5]
        h_bin_centers = h.bin_centers()
        self.assertIsInstance(h_bin_centers, np.ndarray)
        self.assertListEqual(h_bin_centers.tolist(), bin_centers)

    def test_bin_entries(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        bin_entries = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        h_bin_entries = h.bin_entries()
        self.assertIsInstance(h_bin_entries, np.ndarray)
        self.assertListEqual(h_bin_entries.tolist(), bin_entries)

    def test_bin_labels(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        bin_labels = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        h_bin_labels = h.bin_labels()
        self.assertIsInstance(h_bin_labels, np.ndarray)
        self.assertListEqual(h_bin_labels.tolist(), bin_labels)

    def test_bin_vals(self):

        # constructor
        cnt = Counter()
        for i in range(10):
            cnt[i * 2] = i

        vc = ValueCounts(key='x', counts=cnt)
        bin_specs = {'bin_width': 1, 'bin_offset': 0}

        h = Histogram(vc, variable='x', bin_specs=bin_specs)

        h_bin_vals = h.get_bin_vals()
        self.assertIsInstance(h_bin_vals, tuple)
        self.assertEqual(len(h_bin_vals), 2)
        h_bin_entries, h_bin_edges = h_bin_vals[0], h_bin_vals[1]

        bin_entries = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        self.assertIsInstance(h_bin_entries, np.ndarray)
        self.assertListEqual(h_bin_entries.tolist(), bin_entries)
        bin_edges = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 19]
        self.assertIsInstance(h_bin_edges, np.ndarray)
        self.assertListEqual(h_bin_edges.tolist(), bin_edges)

        h_bin_vals = h.get_bin_vals(variable_range=[5.5, 12.5])
        self.assertIsInstance(h_bin_vals, tuple)
        self.assertEqual(len(h_bin_vals), 2)
        h_bin_entries, h_bin_edges = h_bin_vals[0], h_bin_vals[1]

        bin_entries = [0, 3, 0, 4, 0, 5, 0, 6]
        self.assertIsInstance(h_bin_entries, np.ndarray)
        self.assertListEqual(h_bin_entries.tolist(), bin_entries)
        bin_edges = [5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0]
        self.assertIsInstance(h_bin_edges, np.ndarray)
        self.assertListEqual(h_bin_edges.tolist(), bin_edges)

    def tearDown(self):
        pass
