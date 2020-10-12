import numpy
import pandas
import json
import logging
from Relay.Callbacks.CallbackResult import CallbackResult
from Relay.Transformations import PandasDataframeTransformations

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
    column_name = kwargs["column_name"]
    df = relay.dataframe

    # execute some transformations
    ewma_suffix = kwargs["ewma_suffix"]
    PandasDataframeTransformations.exponential_moving_average(df, column_name, ewma_suffix)

    v_suffix = kwargs["v_suffix"]
    PandasDataframeTransformations.diff(df, column_name, v_suffix)

    ip_suffix = kwargs["ip_suffix"]
    PandasDataframeTransformations.inflection_point(df, column_name + v_suffix, ip_suffix)

    # Update the message with the new json
    row_json = df[df.shape[0] - 1].to_json()
    results = CallbackResult(row_json, {})
