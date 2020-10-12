from unittest import TestCase
from Relay.Transformations.PandasDataframeTransformations import segment_linear_regression_prediction
import pandas
import numpy

class test_segment_linear_regression_prediction(TestCase):

    def test_success__empty_dataframe(self):

        df = pandas.DataFrame()

        kwargs = {
            "dataframe": df,
            "x_column_name": "open",
            "y_column_name": "",
            "segment_column_name": "open_v_ip_seg",
            "new_column_suffix": "_pred"
        }

        with self.assertRaises(Exception) as context:
            segment_linear_regression_prediction(**kwargs)

    def test_success__empty_dataframe_column(self):

        df = pandas.DataFrame({
            "open": [],
        })

        kwargs = {
            "dataframe": df,
            "x_column_name": "open",
            "y_column_name": "",
            "segment_column_name": "open_v_ip_seg",
            "new_column_suffix": "_pred"
        }

        with self.assertRaises(Exception) as context:
            segment_linear_regression_prediction(**kwargs)

    def test_success__small_dataframe(self):

        # this dataframe is smaller than the window

        df = pandas.DataFrame({
            "open":          [1],
            "date_ord":      [1],
            "open_v_ip_seg": [1]
        })

        kwargs = {
            "dataframe": df,
            "x_column_name": "date_ord",
            "y_column_name": "open",
            "segment_column_name": "open_v_ip_seg",
            "new_column_suffix": "_pred"
        }

        segment_linear_regression_prediction(**kwargs)

        self.assertEqual((1, 4), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertEqual(1, df["date_ord"][0])
        self.assertEqual(1, df["open_v_ip_seg"][0])
        self.assertTrue(numpy.isnan(df["open_pred"][0]))

    def test_success__large_dataframe(self):

        # this dataframe is smaller than the window

        df = pandas.DataFrame({
            "open":          [1, 3, 5],
            "date_ord":      [1, 2, 3],
            "open_v_ip_seg": [1, 1, 1],
            "open_pred":     [numpy.nan, 5, numpy.nan]
        })

        kwargs = {
            "dataframe": df,
            "x_column_name": "date_ord",
            "y_column_name": "open",
            "segment_column_name": "open_v_ip_seg",
            "new_column_suffix": "_pred"
        }

        segment_linear_regression_prediction(**kwargs)

        self.assertEqual((3, 4), df.shape)

        self.assertEqual(1, df["open"][0])
        self.assertEqual(1, df["date_ord"][0])
        self.assertEqual(1, df["open_v_ip_seg"][0])
        self.assertTrue(numpy.isnan(df["open_pred"][0]))

        self.assertEqual(3, df["open"][1])
        self.assertEqual(2, df["date_ord"][1])
        self.assertEqual(1, df["open_v_ip_seg"][1])
        self.assertEqual(5, df["open_pred"][1])

        self.assertEqual(5, df["open"][2])
        self.assertEqual(3, df["date_ord"][2])
        self.assertEqual(1, df["open_v_ip_seg"][2])
        self.assertEqual(6.999999999999999, df["open_pred"][2])

    def test_success__multi_segment(self):

        # this dataframe is smaller than the window

        df = pandas.DataFrame({
            "open":          [1, 4, 5],
            "date_ord":      [1, 2, 3],
            "open_v_ip_seg": [1, 2, 2],
            "open_pred":     [numpy.nan, 5, numpy.nan]
        })

        kwargs = {
            "dataframe": df,
            "x_column_name": "date_ord",
            "y_column_name": "open",
            "segment_column_name": "open_v_ip_seg",
            "new_column_suffix": "_pred"
        }

        segment_linear_regression_prediction(**kwargs)

        self.assertEqual((3, 4), df.shape)

        self.assertEqual(1, df["open"][0])
        self.assertEqual(1, df["date_ord"][0])
        self.assertEqual(1, df["open_v_ip_seg"][0])
        self.assertTrue(numpy.isnan(df["open_pred"][0]))

        self.assertEqual(4, df["open"][1])
        self.assertEqual(2, df["date_ord"][1])
        self.assertEqual(2, df["open_v_ip_seg"][1])
        self.assertEqual(5, df["open_pred"][1])

        self.assertEqual(5, df["open"][2])
        self.assertEqual(3, df["date_ord"][2])
        self.assertEqual(2, df["open_v_ip_seg"][2])
        self.assertEqual(6, df["open_pred"][2])
