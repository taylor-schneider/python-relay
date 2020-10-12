from unittest import TestCase
from Relay.Transformations.PandasDataframeTransformations import number_segment
import pandas
import numpy

class test_number_segment(TestCase):

    def test_success__empty_dataframe(self):

        df = pandas.DataFrame()

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_v"
        }

        with self.assertRaises(Exception) as context:
            number_segment(**kwargs)

    def test_success__empty_dataframe_column(self):

        df = pandas.DataFrame({
            "ip": [],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "ip",
            "new_column_suffix": "_seg"
        }

        with self.assertRaises(Exception) as context:
            number_segment(**kwargs)

    def test_success__small_dataframe(self):

        # this dataframe is smaller than the window

        df = pandas.DataFrame({
            "ip": [0],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "ip",
            "new_column_suffix": "_seg"
        }
        number_segment(**kwargs)

        self.assertEqual((1, 2), df.shape)
        self.assertEqual(0, df["ip"][0])
        self.assertEqual(1, df["ip_seg"][0])

    def test_success__large_dataframe(self):

        # Dataframe is larger size than the window

        df = pandas.DataFrame({
            "ip": [0, 1, 0, 1],
            "ip_seg": [1, 2, 2, numpy.nan]
        })

        kwargs = {
            "dataframe": df,
            "column_name": "ip",
            "new_column_suffix": "_seg"
        }
        number_segment(**kwargs)

        self.assertEqual((4, 2), df.shape)
        self.assertEqual(0, df["ip"][0])
        self.assertEqual(1, df["ip_seg"][0])
        self.assertEqual(1, df["ip"][1])
        self.assertEqual(2, df["ip_seg"][1])
        self.assertEqual(0, df["ip"][2])
        self.assertEqual(2, df["ip_seg"][2])
        self.assertEqual(1, df["ip"][3])
        self.assertEqual(3, df["ip_seg"][3])
