import pandas
import logging
from Relay.ActiveMQ.ActiveMQRelay import ActiveMQRelay
from Relay.Utilities import df_row_from_json

class PandasActiveMQRelay(ActiveMQRelay):

    def __init__(self, source, destination, amq_conn_param, listener=None):
        self.dataframe = pandas.DataFrame({})
        super(PandasActiveMQRelay, self).__init__(source, destination, amq_conn_param, listener)

    # This function will add the datapoint to the internal cache
    def process(self, *args, **kwargs):

        message = kwargs["message"]
        logging.debug("pandas process: {0}".format(message))
        df = df_row_from_json(message)

        if self.dataframe.shape == (0, 0):
            self.dataframe = df
        else:
            self.dataframe.loc[self.dataframe.shape[0]] = df.iloc[0]
