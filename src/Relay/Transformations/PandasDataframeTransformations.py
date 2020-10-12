import numpy
from sklearn.linear_model import LinearRegression
import Relay.Transformations.Utilities as utils

def exponential_moving_average(dataframe, column_name, new_column_suffix, window, decay, update_df=True):

    n = dataframe.shape[0]

    # Calculate the moving average
    column = dataframe[column_name]
    if n < window:
        ma = numpy.nan
    else:
        start = n - window
        column = column[start:n]
        ma = list(column.ewm(com=decay).mean())[-1]

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, column_name, new_column_suffix, ma, numpy.nan)
    else:
        return ma


def diff(dataframe, column_name, new_column_suffix, update_df=True):

    n = dataframe.shape[0]

    # Calculate the diff
    column = dataframe[column_name]
    if 1 <= n < 2:
        d = numpy.nan
    else:
        d = column[n - 1] - column[n - 2]

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, column_name, new_column_suffix, d, numpy.nan)
    else:
        return d


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


def inflection_point(dataframe, column_name, new_column_suffix, update_df=True):

    n = dataframe.shape[0]

    if 1 <= n < 3:
        ip = 0
    else:
        column = dataframe[column_name]
        v2 = column[n - 1] - column[n - 2]
        v1 = column[n - 2] - column[n - 3]

        if numpy.isnan(v1) and numpy.isnan(v2):
            ip = 0
        else:
            ip = int(not __same_signs(v1, v2))

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, column_name, new_column_suffix, ip, numpy.nan)
    else:
        return ip


def date_ordinals(dataframe, column_name, new_column_suffix, update_df=True):

    n = dataframe.shape[0]

    # Calculate the ordinal
    ordinal = int(dataframe[column_name][n - 1].toordinal())

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, column_name, new_column_suffix, ordinal, numpy.nan)
    else:
        return ordinal


def number_segment(dataframe, column_name, new_column_suffix, update_df=True):

    # this should be pointed at the a binary column
    # for example the inflection point column

    # Determine the name of the new column
    new_column_name = "{0}{1}".format(column_name, new_column_suffix)

    n = dataframe.shape[0]
    if n == 1:
        segment = 1
    else:
        signal = dataframe[column_name][n-1]
        prev_seg = dataframe[new_column_name][n-2]
        if bool(signal):
            segment = prev_seg + 1
        else:
            segment = prev_seg

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, column_name, new_column_suffix, segment, numpy.nan)
    else:
        return segment


def segment_linear_regression_prediction(dataframe, x_column_name, y_column_name, segment_column_name, new_column_suffix, update_df=True):
    n = dataframe.shape[0]

    segment_num = dataframe[segment_column_name][n - 1]
    segment_df = dataframe.loc[dataframe[segment_column_name] == segment_num]

    # If not enough data points, return nan
    if segment_df.shape[0] < 2:
        pred_y = numpy.nan
    else:
        x_train = segment_df[x_column_name].values.reshape(-1, 1) # I am not sure why we need to reshape the data
        y_train = segment_df[y_column_name]
        reg = LinearRegression().fit(x_train, y_train)

        pred_x = dataframe[x_column_name][n - 1] + 1
        pred_y = reg.predict([[pred_x]])

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, y_column_name, new_column_suffix, pred_y, numpy.nan)
    else:
        return pred_y