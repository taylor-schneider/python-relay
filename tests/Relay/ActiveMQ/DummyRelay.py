import pandas
from Relay.ActiveMQ.ActiveMQRelayListener import ActiveMQRelayListener

class DummyRelay:

    def __init__(self):
        self.dataframe = pandas.DataFrame()
        self.listener = ActiveMQRelayListener()