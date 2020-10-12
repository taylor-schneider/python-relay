from unittest import TestCase
from Relay.Transformations.PandasDataframeTransformations import exponential_moving_average
import pandas
import numpy

class test_exponential_moving_average(TestCase):

    def test_success__empty_dataframe(self):

        df = pandas.DataFrame()

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ewma",
            "window": 3,
            "decay": 0.9
        }

        with self.assertRaises(Exception) as context:
            exponential_moving_average(**kwargs)


    def test_success__empty_dataframe_column(self):

        df = pandas.DataFrame({
            "open": [],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ewma",
            "window": 3,
            "decay": 0.9
        }
        exponential_moving_average(**kwargs)

        self.assertEqual((1,2), df.shape)
        self.assertTrue(numpy.isnan(df["open"][0]))
        self.assertTrue(numpy.isnan(df["open_ewma"][0]))

    def test_success__small_dataframe(self):

        # this dataframe is smaller than the window

        df = pandas.DataFrame({
            "open": [1, 2, 3],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ewma",
            "window": 5,
            "decay": 0.9
        }
        exponential_moving_average(**kwargs)

        self.assertEqual((3,2), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertTrue(numpy.isnan(df["open_ewma"][0]))
        self.assertEqual(2, df["open"][1])
        self.assertTrue(numpy.isnan(df["open_ewma"][1]))
        self.assertEqual(3, df["open"][2])
        self.assertTrue(numpy.isnan(df["open_ewma"][2]))

    def test_success__window_dataframe(self):

        # Dataframe is the same size as the window

        df = pandas.DataFrame({
            "open": [1, 2, 3],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ewma",
            "window": 3,
            "decay": 0.9
        }
        exponential_moving_average(**kwargs)

        self.assertEqual((3,2), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertTrue(numpy.isnan(df["open_ewma"][0]))
        self.assertEqual(2, df["open"][1])
        self.assertTrue(numpy.isnan(df["open_ewma"][1]))
        self.assertEqual(3, df["open"][2])
        self.assertEqual(2.4567699836867867, df["open_ewma"][2])

    def test_success__large_dataframe(self):

        # Dataframe is larger size than the window

        df = pandas.DataFrame({
            "open": [1, 2, 3, 4],
            "open_ewma": [numpy.nan, numpy.nan, 2.4567699836867867, numpy.nan]
        })

        kwargs = {
            "dataframe": df,
            "column_name": "open",
            "new_column_suffix": "_ewma",
            "window": 3,
            "decay": 0.9
        }
        exponential_moving_average(**kwargs)

        self.assertEqual((4,2), df.shape)
        self.assertEqual(1, df["open"][0])
        self.assertTrue(numpy.isnan(df["open_ewma"][0]))
        self.assertEqual(2, df["open"][1])
        self.assertTrue(numpy.isnan(df["open_ewma"][1]))
        self.assertEqual(3, df["open"][2])
        self.assertEqual(2.4567699836867867, df["open_ewma"][2])
        self.assertEqual(4, df["open"][3])
        self.assertEqual(3.4567699836867862, df["open_ewma"][3])