import json
import pandas
import logging
import io
import os
from Relay.ActiveMQ.ActiveMQRelay import ActiveMQRelay

class PandasActiveMQRelay(ActiveMQRelay):

    def __init__(self, source, destination, amq_conn_param, listener=None):
        self.dataframe = pandas.DataFrame({})
        super(PandasActiveMQRelay, self).__init__(source, destination, amq_conn_param, listener)

    # This function will add the datapoint to the internal cache
    def process(self, *args, **kwargs):

        message = kwargs["message"]
        logging.debug("pandas process: {0}".format(message))
        df = pandas.read_json(message, orient='records')
        if self.dataframe.shape == (0, 0):
            # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
            self.dataframe = df
        else:
            self.dataframe.loc[self.dataframe.shape[0]] = df.loc[0]
