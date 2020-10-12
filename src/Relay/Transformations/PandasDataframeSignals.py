import numpy
from Relay.Signals.TradingAction import TradingAction
import Relay.Transformations.Utilities as utils


def linear_regression_signal(dataframe, actual_column_name, pred_column_name, new_column_suffix, update_df=True):

    n = dataframe.shape[0]

    actual = dataframe[actual_column_name][n]
    pred = dataframe[pred_column_name][n]

    if numpy.isnan(actual) or numpy.isnan(pred):
        signal = TradingAction.NONE
    elif pred > actual:
        signal = TradingAction.BUY
    elif pred < actual:
        signal = TradingAction.SELL
    else:
        signal = TradingAction.HOLD

    # Update the dataframe
    if update_df:
        utils.update_dataframe_column(dataframe, pred_column_name, new_column_suffix, signal, numpy.nan)
    else:
        return signal