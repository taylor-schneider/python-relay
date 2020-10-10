import json
import pandas
import logging
from Relay.ActiveMQ.ActiveMQRelay import ActiveMQRelay

class PandasActiveMQRelay(ActiveMQRelay):

    def __init__(self, source, destination, amq_conn_param, listener=None):
        self.dataframe = pandas.DataFrame({})
        super(PandasActiveMQRelay, self).__init__(source, destination, amq_conn_param, listener)

    # This function will add the datapoint to the internal cache
    def process(self, *args, **kwargs):

        message = kwargs["message"]
        logging.debug("pandas process: {0}".format(message))
        d = json.loads(message)
        if self.dataframe.shape == (0, 0):
            self.dataframe = pandas.DataFrame(d, index=[0])
        else:
            self.dataframe.loc[self.dataframe.shape[0]] = d
