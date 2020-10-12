from unittest import TestCase
from Relay.Transformations.PandasDataframeTransformations import date_ordinals
from Relay.Callbacks.Utilities import convert_date_string_to_date
import pandas


class test_date_ordinals(TestCase):

    def test_success__empty_dataframe(self):

        df = pandas.DataFrame()

        kwargs = {
            "dataframe": df,
            "column_name": "date",
            "new_column_suffix": "_ord"
        }

        with self.assertRaises(Exception) as context:
            date_ordinals(**kwargs)

    def test_success__empty_dataframe_column(self):

        df = pandas.DataFrame({"date": []})

        kwargs = {
            "dataframe": df,
            "column_name": "date",
            "new_column_suffix": "_ord"
        }

        with self.assertRaises(Exception) as context:
            date_ordinals(**kwargs)

    def test_success__small_dataframe(self):

        # this dataframe is smaller than the window

        d1 = convert_date_string_to_date('2019-07-01')

        df = pandas.DataFrame({
            "date": [d1],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "date",
            "new_column_suffix": "_ord"
        }
        date_ordinals(**kwargs)

        self.assertEqual((1, 2), df.shape)
        self.assertEqual(d1, df["date"][0])
        self.assertEqual(737241.0, df["date_ord"][0])

    def test_success__large_dataframe(self):

        # Dataframe is the same size as the window

        d1 = convert_date_string_to_date('2019-07-01')
        d2 = convert_date_string_to_date('2019-07-02')

        df = pandas.DataFrame({
            "date": [d1, d2],
        })

        kwargs = {
            "dataframe": df,
            "column_name": "date",
            "new_column_suffix": "_ord"
        }

        date_ordinals(**kwargs)

        self.assertEqual((2, 2), df.shape)
        self.assertEqual(d1, df["date"][0])
        self.assertEqual(737242.0, df["date_ord"][1])

