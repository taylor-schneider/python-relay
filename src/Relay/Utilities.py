import numpy
import pandas
import json

def convert_date_string_to_date(input_string):

    # We then do our manipulation
    input_string = input_string.strip()

    # Make it a date
    result = numpy.datetime64(input_string, 'D')

    return result

# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html

def df_row_to_json(dataframe, row_index):
    message = dataframe.iloc[[row_index]].to_json(date_unit='ns', orient='table')
    return message

def df_row_from_json(message):
    df = pandas.read_json(message, date_unit='ns', orient='table')
    return df
