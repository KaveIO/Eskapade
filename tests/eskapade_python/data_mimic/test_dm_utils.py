import sys
import unittest
import unittest.mock as mock

from eskapade import data_mimic


class DataMimicUtilTest(unittest.TestCase):
    """Test data mimic utils"""

    @mock.patch("eskapade.data_mimic.data_mimic_util.find_peaks")
    def test_peaks(self, mock_peaks):

        peaks = mock_peaks()
        peaks.return_value = {'a': 1}
        mock_peaks.assert_called()
        
