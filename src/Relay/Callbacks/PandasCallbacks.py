import numpy
import pandas
import json
import logging
from Relay.Callbacks.CallbackResult import CallbackResult
from sklearn.linear_model import LinearRegression

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

    # Get Params
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    message = kwargs["message"]
    window = kwargs["window"]
    decay = kwargs["decay"]
    relay = kwargs["relay"]

    df = relay.dataframe

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)
    results.misc["new_column_name"] = new_column_name

    # Add the new column to the dataframe
    if new_column_name not in df.columns:
        df[new_column_name] = numpy.nan

    # Calculate the moving average
    column = df[column_name]
    n = len(column)
    if n < window:
        ma = numpy.nan
    else:
        start = n - window - 1
        column = column[start:n]
        ma = list(column.ewm(com=decay).mean())[-1]

    # Update the df
    df.at[n - 1, new_column_name] = ma

    # Update the message in the results
    d = json.loads(message)
    d[0][new_column_name] = ma
    results.message = json.dumps(d)
    results.misc["new_column_name"] = new_column_name

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

    # Get Params
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    message = kwargs["message"]
    relay = kwargs["relay"]
    df = relay.dataframe

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)
    results.misc["new_column_name"] = new_column_name

    # Add the new column to the dataframe
    if new_column_name not in df.columns:
        df[new_column_name] = numpy.nan

    # Calculate the moving average
    column = df[column_name]
    n = df.shape[0]
    if n < 2:
        v = numpy.nan
    else:
        v = column[n - 1] - column[n - 2]

    # Update the df
    df.at[n - 1, new_column_name] = v

    # Update the message in the results
    d = json.loads(message)
    d[0][new_column_name] = v
    results.message = json.dumps(d)
    results.misc["new_column_name"] = new_column_name

    return results


def calculate_inflection_point(*args, **kwargs):

    # Get Params
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    message = kwargs["message"]
    relay = kwargs["relay"]

    df = relay.dataframe

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)
    results.misc["new_column_name"] = new_column_name

    # Add the new column to the dataframe
    if new_column_name not in df.columns:
        df[new_column_name] = numpy.nan

    # Calculate the moving average
    column = df[column_name]
    n = df.shape[0]
    if n < 2:
        ip = 0
    else:
        a = column[n - 1]
        b = column[n - 2]
        if numpy.isnan(a) and numpy.isnan(b):
            ip = 0
        else:
            ip = int(not __same_signs(a, b))

    # Update the df
    df.at[n - 1, new_column_name] = ip

    # Update the message in the results
    d = json.loads(message)
    d[0][new_column_name] = ip
    results.message = json.dumps(d)
    results.misc["new_column_name"] = new_column_name

    return results


def calculate_ordinals(*args, **kwargs):

    # Get Params
    column_name = kwargs["column_name"]
    new_column_suffix = kwargs["new_column_suffix"]
    message = kwargs["message"]
    relay = kwargs["relay"]

    df = relay.dataframe

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)
    results.misc["new_column_name"] = new_column_name

    # Add the new column to the dataframe
    if new_column_name not in df.columns:
        df[new_column_name] = numpy.nan

    # Calculate the moving ordinals
    ordinal = df[column_name][df.shape[0] - 1].toordinal()

    # Update the df
    n = df.shape[0]
    df.at[n - 1, new_column_name] = ordinal

    # Update the message in the results
    d = json.loads(message)
    d[0][new_column_name] = int(ordinal)
    results.message = json.dumps(d)
    results.misc["new_column_name"] = new_column_name

    return results


def linear_regression_prediction(*args, **kwargs):

    # Get Params
    column_name = kwargs["column_name"]
    pred_column_name = kwargs["pred_column"]
    slope_suffix_name = kwargs["slope_suffix"]
    train_x_name = kwargs["train_x_name"]
    train_y_name = kwargs["train_y_name"]
    ip_column_name = kwargs["ip_column_name"]
    message = kwargs["message"]
    relay = kwargs["relay"]

    df = relay.dataframe

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    pred_column_name = "{0}{1}".format(column_name, pred_column_name)
    slope_column_name = "{0}{1}".format(column_name, slope_suffix_name)

    results.misc["pred_column_name"] = pred_column_name
    results.misc["slope_suffix_name"] = slope_column_name

    # Add the new column to the dataframe
    if pred_column_name not in df.columns:
        df[pred_column_name] = numpy.nan
    if slope_suffix_name not in df.columns:
        df[slope_suffix_name] = numpy.nan

    # Determine the index of the previous ip
    n = df.shape[0]
    for i in reversed(range(0, n)):
        ip = df[ip_column_name][i]
        ip_index = i
        if bool(ip):
            break

    # Determine if we have enough info
    enough_info = n - ip_index - 1> 2

    # If we dont have enough info, return nan
    if not enough_info:
        p = numpy.nan
        c = numpy.nan
    else:
        # Gather the data to train the model
        train_x = df[train_x_name][ip_index:n - 1].values.reshape(-1, 1)
        train_y = df[train_y_name][ip_index:n - 1]

        # Train the model
        reg = LinearRegression().fit(train_x, train_y)

        # Make a prediction
        t_n = train_x.shape[0]
        x = train_x[t_n - 1].reshape(-1, 1) # I am not sure why we need to reshape the data
        p = float(reg.predict(x))
        c = float(reg.coef[0])

    # Update the df
    df.at[n - 1, pred_column_name] = p
    df.at[n - 1, slope_column_name] = c

    # Update the message in the results
    d = json.loads(message)
    d[0][pred_column_name] = p
    d[0][slope_suffix_name] = c
    results.message = json.dumps(d)

    return results


def determine_buy_sell_signal(*args, **kwargs):

    # Get Params
    column_name = kwargs["column_name"]
    pred_column_name = kwargs["pred_column_name"]
    message = kwargs["message"]
    relay = kwargs["relay"]

    df = relay.dataframe

    # Create a var to hold the result
    results = CallbackResult(message, {})

    # Determine the name of the new column
    buy_column_name = "{0}{1}".format(column_name, "_buy")
    sell_column_name = "{0}{1}".format(column_name, "_sell")

    results.misc["buy_column_name"] = buy_column_name
    results.misc["sell_column_name"] = sell_column_name

    # Add the new column to the dataframe
    if buy_column_name not in df.columns:
        df[buy_column_name] = numpy.nan
    if sell_column_name not in df.columns:
        df[sell_column_name] = numpy.nan

    # Determine the signal
    n = df.shape[0]
    v = df[column_name][n - 1]
    p = df[pred_column_name][n - 1]
    b = numpy.nan
    s = numpy.nan
    if v > p:
        s = df[column_name][n - 1]
    elif p > v:
        b = df[column_name][n - 1]

    # Update the df
    df.at[n - 1, buy_column_name] = b
    df.at[n - 1, sell_column_name] = s

    # Update the message in the results
    d = json.loads(message)
    d[0][buy_column_name] = b
    d[0][sell_column_name] = s
    results.message = json.dumps(d)

    return results


