import numpy
import pandas
import json
import logging
from Relay.Callbacks.CallbackResult import CallbackResult
from Relay.Transformations import PandasDataframeTransformations
from Relay import Utilities

def send_message(*args, **kwargs):
    relay = kwargs['relay']
    message = kwargs["message"]
    logging.debug("send: {0}".format(message))
    relay.send_message(message)
    return message, kwargs


# In some cases, we cant a callback to run a given function against multiple columns in a dataframe
# For example, running a chain of transformations against multiple columns in the dataframe

def dataframe_callback_shell_colusre(callback, columns):
    def dataframe_callback_shell(*args, **kwargs):

        # Execute the callback for each column
        message = kwargs["message"]
        callback_results = CallbackResult(message, {})

        for column_name in columns:

            # Update the kwargs with the column name
            kwargs["column_name"] = column_name

            # Run the callback and store the results
            results = callback(*args, **kwargs)
            if type(results) == CallbackResult:
                callback_results.message = results.message
                callback_results.misc = results.misc

        return callback_results

    return dataframe_callback_shell

def demo_algo_callback(*args, **kwargs):

    # Get basic some params
    relay = kwargs['relay']
    message = kwargs["message"]
    price_column_name = kwargs["price_column_name"]
    date_column_name = kwargs["date_column_name"]
    df = relay.dataframe

    # execute some transformations
    ewma_suffix = kwargs["ewma_suffix"]
    ewma_column_name = price_column_name + ewma_suffix
    ewma_window = kwargs["ewma_window"]
    ewma_decay = kwargs["ewma_decay"]
    PandasDataframeTransformations.exponential_moving_average(
        df, price_column_name, ewma_suffix, ewma_window, ewma_decay)

    v_suffix = kwargs["v_suffix"]
    v_column_name = ewma_column_name + v_suffix
    PandasDataframeTransformations.diff(
        df, ewma_column_name, v_suffix)

    ip_suffix = kwargs["ip_suffix"]
    ip_column_name = v_column_name + ip_suffix
    PandasDataframeTransformations.inflection_point(
        df, v_column_name, ip_suffix)

    ord_suffix = kwargs["ord_suffix"]
    PandasDataframeTransformations.date_ordinals(
        df, date_column_name, ord_suffix)

    seg_suffix = kwargs["seg_suffix"]
    seg_column_name = ip_column_name + seg_suffix
    PandasDataframeTransformations.number_segment(
        df, ip_column_name, seg_suffix)

    lr_suffix = kwargs["lr_suffix"]
    lr_column_name = price_column_name + lr_suffix
    lr_x = kwargs["LR_x"]
    lr_y = kwargs["LR_y"]
    PandasDataframeTransformations.segment_linear_regression_prediction(
        df, lr_x, lr_y, seg_column_name, lr_suffix)


    # Update the message with the new json
    row_json = Utilities.df_row_to_json(df, df.shape[0] - 1)
    results = CallbackResult(row_json, {})
