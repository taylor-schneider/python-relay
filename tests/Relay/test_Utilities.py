from unittest import TestCase
import pandas
from Relay import Utilities


class test_df_serialization(TestCase):

    def test_success(self):

        df = pandas.DataFrame({
            "open": [5],
            "date": [Utilities.convert_date_string_to_date('2019-07-01')]
        })

        message = Utilities.df_row_to_json(df, 0)
        new_df = Utilities.df_row_from_json(message)

        self.assertTrue(list(df.columns) == list(new_df.columns))
        self.assertTrue(df.index == new_df.index)
        self.assertEqual(df.shape, new_df.shape)

