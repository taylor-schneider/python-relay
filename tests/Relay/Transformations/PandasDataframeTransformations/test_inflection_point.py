from unittest import TestCase
from Relay.Transformations.PandasDataframeTransformations import inflection_point
import pandas
import numpy

class test_inflection_point(TestCase):

    def test_success__empty_dataframe(self):

        df = pandas.DataFrame()

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ip"
        }

        with self.assertRaises(Exception) as context:
            inflection_point(**kwargs)

    def test_success__empty_dataframe_column(self):

        df = pandas.DataFrame({
            "open": [],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ip"
        }

        with self.assertRaises(Exception) as context:
            inflection_point(**kwargs)

    def test_success__small_dataframe(self):

        # this dataframe is smaller than the window

        df = pandas.DataFrame({
            "open": [1],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ip"
        }
        inflection_point(**kwargs)

        self.assertEqual((1, 2), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertEqual(0, df["open_ip"][0])

    def test_success__window_dataframe(self):

        # Dataframe is the same size as the window

        df = pandas.DataFrame({
            "open": [1, 2],
            "open_ip": [0, numpy.nan]
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ip"
        }
        inflection_point(**kwargs)

        self.assertEqual((2,2), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertEqual(0, df["open_ip"][0])
        self.assertEqual(2, df["open"][1])
        self.assertEqual(0, df["open_ip"][1])

    def test_success__large_dataframe(self):

        # Dataframe is larger size than the window

        df = pandas.DataFrame({
            "open": [1, 2, 5, 3],
            "open_ip": [0, 0, 0, numpy.nan]
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ip"
        }
        inflection_point(**kwargs)

        self.assertEqual((4, 2), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertEqual(0, df["open_ip"][0])
        self.assertEqual(2, df["open"][1])
        self.assertEqual(0, df["open_ip"][1])
        self.assertEqual(5, df["open"][2])
        self.assertEqual(0, df["open_ip"][2])
        self.assertEqual(3, df["open"][3])
        self.assertEqual(1, df["open_ip"][3])
