import numpy
import json
import logging
from Relay.Callbacks.CallbackResult import CallbackResult


def send_message(*args, **kwargs):
    relay = kwargs['relay']
    message = kwargs["message"]
    logging.debug("send: {0}".format(message))
    relay.send_message(message)
    return message, kwargs


# This closure will create shell function that execute against a list of columns in a pandas dataframe

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


def calculate_exponential_moving_average(**kwargs):

    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)
    message = kwargs["message"]
    d = json.loads(message)

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    results.misc["new_column_name"] = new_column_name

    # Retrieve some params
    window = kwargs["window"]
    decay = kwargs["decay"]
    relay = kwargs["relay"]
    df = relay.dataframe

    # Calculate the moving average
    # If this is the first message, the dataframe does not have any columns
    # So we will need to handle this edge case

    if df.shape == (0, 0):
        ma = numpy.nan
    elif df.shape[0] < window - 1:
        ma = numpy.nan
    else:
        # Slice the column so we can calc the ma
        column = df[column_name]
        n = len(column)
        start = n - window -1
        column = column[start:n]

        # Add the new data point to the column
        column[n + 1] = d[0][column_name]

        # The column is expected to be a pandas series
        # Calculate the moving average
        ma = list(column.ewm(com=decay).mean())[-1]

    # Update the message in the results

    d[0][new_column_name] = ma
    results.message = json.dumps(d)
    results.misc["new_column_name"] = new_column_name
    logging.debug("ewma: {0}".format(results.message))

    return results


def __same_signs(a,b):
    if a > 0 and b > 0:
        return True
    if a > 0 and b < 0:
        return False
    if a < 0 and b > 0:
        return False
    if a < 0 and b < 0:
        return True
    else:
        return False


def calculate_velocity(*args, **kwargs):

    # Create a var to hold the result
    results = CallbackResult("", {})

    # Determine the name of the new column
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)
    results.misc["new_column_name"] = new_column_name

    # Retrieve the dataframe from the relay
    relay = kwargs["relay"]
    df = relay.dataframe

    # Calculate the velocity using data in the dataframe and the message
    message = kwargs["message"]
    d = json.loads(message)
    if df.shape == (0, 0):
        v = numpy.nan
    else:
        column = df[column_name]
        v = d[0][column_name] - column[df.shape[0] - 1]

    # Update the message in the results
    d[0][new_column_name] = v
    results.message = json.dumps(d)

    logging.debug("velocity: {0}".format(results.message))

    # Update the misc info and pass a param to the next function so we know what column was updated
    results.misc["new_column_name"] = new_column_name

    return results


def calculate_inflection_point(*args, **kwargs):

    # Create a var to hold the result
    results = CallbackResult("", {})

    # Determine the name of the new column
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)

    # Retrieve the dataframe from the relay
    relay = kwargs["relay"]
    df = relay.dataframe

    # Calculate the inflection point
    message = kwargs["message"]
    d = json.loads(message)
    if df.shape == (0, 0):
        ip = 0
    else:
        column = df[column_name]
        a = d[0][column_name]
        b = column[df.shape[0] - 1]

        if numpy.isnan(a) and numpy.isnan(b):
            ip = 0
        else:
            ip = not __same_signs(a, b)


    # Update the message in the results
    d[0][new_column_name] = ip
    results.message = json.dumps(d)

    logging.debug("velocity: {0}".format(results.message))

    # Update the misc info and pass a param to the next function so we know what column was updated
    results.misc["new_column_name"] = new_column_name

    return results


def calculate_ordinals(*args, **kwargs):

    # Create a var to hold the result
    results = CallbackResult("", {})

    # Determine the name of the new column
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)

    # Calculate the ordinal
    message = kwargs["message"]
    d = json.loads(message)
    ordinal = d[column_name].toordinal()

    # Update the message in the results
    d[new_column_name] = ordinal
    results.message = json.dumps(d)
    results.misc["new_column_name"] = new_column_name
    logging.debug("velocity: {0}".format(results.message))

    return results

