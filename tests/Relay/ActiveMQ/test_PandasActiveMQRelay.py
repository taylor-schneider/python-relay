from unittest import TestCase
from Relay.ActiveMQ.PandasActiveMQRelay import PandasActiveMQRelay
from Relay.Callbacks.CallbackResult import CallbackResult
from Relay.ActiveMQ.ActiveMQConnectionParameters import ActiveMQConnectionParameters
from Relay.Callbacks import PandasCallbacks
from Relay.Callbacks.CallbackMapping import CallbackMapping
from tests.Relay.ActiveMQ.DummyRelay import DummyRelay
import Relay.Utilities as Utilities
import pandas
import logging
import os

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)6s :: %(threadName)12s :: %(filename)30s:%(lineno)4s - %(funcName)25s() ::  %(message)s')


def demo_callback(*args, **kwargs):
    try:
        # Retrieve params
        message = kwargs["message"]
        value = kwargs["value"]
        mutex = kwargs["mutex"]

        # Update the shared memory buffer in a thread safe way
        mutex.acquire()
        value.value += 1
        message_count = value.value
        mutex.release()

        # Send a nice message to the user
        logging.debug("callback {0}: {1}".format(message_count, message))

    except Exception as e:
        logging.error("Callback {0}: encountered an exception")
        logging.error(e)

    return CallbackResult(message, {})

# Define a utility function to help us load the data



def load_data_into_dataframe():
    # Read the file into a dataframe
    file_path = "../../nasdaq_2019.csv"
    converter_mapping = {
        "date": Utilities.convert_date_string_to_date
    }
    nasdaq_dataframe = pandas.read_csv(file_path, converters=converter_mapping)

    # Sort based on the date column
    nasdaq_dataframe = nasdaq_dataframe.sort_values("date")

    aaba_dataframe = nasdaq_dataframe.loc[nasdaq_dataframe.ticker == "AABA"]
    aaba_dataframe.index = range(0, aaba_dataframe.shape[0])  # reset the index to make our life easier

    return aaba_dataframe


class Test_PandasActiveMQRelay(TestCase):


    def test_success_offline__process_one_message(self):

        # Setup a dummy relay
        relay = DummyRelay()
        relay.process = PandasActiveMQRelay.process

        # Create a dummy message
        df = pandas.DataFrame({
            "open": [5],
            "date": [Utilities.convert_date_string_to_date('2019-07-01')]
        })
        message = Utilities.df_row_to_json(df, 0)

        # Create the callback mappings for the relay's listener
        relay.listener.callback_mappings = [
            CallbackMapping(
                relay.process,
                [relay],
                {
                    "relay": relay,
                    "column_name": open,
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip"
                }
            )
        ]

        relay.listener.on_message({}, message)

        self.assertTrue(list(df.columns) == list(relay.dataframe.columns))
        self.assertTrue(df.index == relay.dataframe.index)
        self.assertEqual(df.shape, relay.dataframe.shape)

    def test_success_offline__process_many_messages(self):

        # Setup a dummy relay
        relay = DummyRelay()
        relay.process = PandasActiveMQRelay.process

        # Create a dummy message
        df = pandas.DataFrame({
            "open": [5, 4, 3],
            "date": [Utilities.convert_date_string_to_date('2019-07-01'),
                     Utilities.convert_date_string_to_date('2019-07-02'),
                     Utilities.convert_date_string_to_date('2019-07-03')]
        })
        messages = []
        messages.append(Utilities.df_row_to_json(df, 0))
        messages.append(Utilities.df_row_to_json(df, 1))
        messages.append(Utilities.df_row_to_json(df, 2))

        # Create the callback mappings for the relay's listener
        relay.listener.callback_mappings = [
            CallbackMapping(
                relay.process,
                [relay],
                {
                    "relay": relay,
                    "column_name": open,
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip"
                }
            )
        ]

        for message in messages:
            relay.listener.on_message({}, message)

        self.assertEqual(df.shape, relay.dataframe.shape)
        self.assertTrue(list(df.columns) == list(relay.dataframe.columns))
        self.assertTrue(all(df.index == relay.dataframe.index))

