from unittest import TestCase
from Relay import Utilities
from Relay.ActiveMQ.PandasActiveMQRelay import PandasActiveMQRelay
from Relay.Callbacks.CallbackMapping import CallbackMapping
from tests.Relay.ActiveMQ.DummyRelay import DummyRelay
from Relay.Callbacks.PandasCallbacks import demo_algo_callback
import pandas
import os
import logging
import time
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)6s :: %(threadName)12s :: %(filename)30s:%(lineno)4s - %(funcName)25s() ::  %(message)s')



class test_demo_algo_callback(TestCase):

    def test_success__one_message(self):

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
                    "relay": relay
                }
            ),
            CallbackMapping(
                demo_algo_callback,
                [],
                {
                    "relay": relay,
                    "price_column_name": "open",
                    "date_column_name": "date",
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip",
                    "ewma_window": 3,
                    "ewma_decay": 0.9,
                    "ord_suffix": "_ord",
                    "seg_suffix": "_seg",
                    "lr_suffix": "_LR",
                    "LR_x": "date_ord",
                    "LR_y": "open"
                }
            )
        ]

        relay.listener.on_message({}, message)

        logging.debug(os.linesep + str(relay.dataframe.to_string()))
        time.sleep(0.5) # Let the log print to screen

        self.assertEqual(relay.dataframe.shape, (1,8))
        self.assertEqual(list(relay.dataframe.columns), ["open", "date", "open_ewma", "open_ewma_v", "open_ewma_v_ip", "date_ord", "open_ewma_v_ip_seg", "open_LR"])
        self.assertTrue(df.index == relay.dataframe.index)

    def test_success__multiple_messages(self):

        # Setup a dummy relay
        relay = DummyRelay()
        relay.process = PandasActiveMQRelay.process

        # Create a dummy message
        df = pandas.DataFrame({
            "open": [5, 6, 7],
            "date": [Utilities.convert_date_string_to_date('2019-07-01'),
                     Utilities.convert_date_string_to_date('2019-07-02'),
                     Utilities.convert_date_string_to_date('2019-07-03')]
        })
        messages = []
        for x in range(0, df.shape[0]):
            message = Utilities.df_row_to_json(df, x)
            messages.append(message)

        # Create the callback mappings for the relay's listener
        relay.listener.callback_mappings = [
            CallbackMapping(
                relay.process,
                [relay],
                {
                    "relay": relay
                }
            ),
            CallbackMapping(
                demo_algo_callback,
                [],
                {
                    "relay": relay,
                    "price_column_name": "open",
                    "date_column_name": "date",
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip",
                    "ewma_window": 3,
                    "ewma_decay": 0.9,
                    "ord_suffix": "_ord",
                    "seg_suffix": "_seg",
                    "lr_suffix": "_LR",
                    "LR_x": "date_ord",
                    "LR_y": "open"
                }
            )
        ]

        for message in messages:
            relay.listener.on_message({}, message)

        logging.debug(os.linesep + str(relay.dataframe.to_string()))
        time.sleep(0.5) # Let the log print to screen

        self.assertEqual(relay.dataframe.shape, (3,8))
        self.assertEqual(list(relay.dataframe.columns), ["open", "date", "open_ewma", "open_ewma_v", "open_ewma_v_ip", "date_ord", "open_ewma_v_ip_seg", "open_LR"])
        self.assertTrue(list(df.index) == list(relay.dataframe.index))

    def test_success__single_ticker_nasdaq_simulation(self):

        # Read the file into a dataframe
        file_path = "../../../../nasdaq_2019.csv"
        converter_mapping = {
            "date": Utilities.convert_date_string_to_date
        }
        nasdaq_dataframe = pandas.read_csv(file_path, converters=converter_mapping)

        aaba_dataframe = nasdaq_dataframe.loc[nasdaq_dataframe.ticker == "AABA"]
        aaba_dataframe.index = range(0, aaba_dataframe.shape[0])

        # Generate a list of messages
        n = aaba_dataframe.shape[0]
        messages = []
        for x in range(0, n):
            message = Utilities.df_row_to_json(aaba_dataframe, x)
            messages.append(message)

        # Setup a dummy relay
        relay = DummyRelay()
        relay.process = PandasActiveMQRelay.process

        # Create the callback mappings for the relay's listener
        relay.listener.callback_mappings = [
            CallbackMapping(
                relay.process,
                [relay],
                {
                    "relay": relay
                }
            ),
            CallbackMapping(
                demo_algo_callback,
                [],
                {
                    "relay": relay,
                    "price_column_name": "open",
                    "date_column_name": "date",
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip",
                    "ewma_window": 3,
                    "ewma_decay": 0.9,
                    "ord_suffix": "_ord",
                    "seg_suffix": "_seg",
                    "lr_suffix": "_LR",
                    "LR_x": "date_ord",
                    "LR_y": "open"
                }
            )
        ]

        for message in messages:
            relay.listener.on_message({}, message)


        logging.debug(os.linesep + str(relay.dataframe.to_string()))
        time.sleep(0.5) # Let the log print to screen

        self.assertEqual(relay.dataframe.shape, (158, 14))
        cols = ['ticker', 'interval', 'date', 'open', 'high', 'low', 'close', 'volume', 'open_ewma', 'open_ewma_v', 'open_ewma_v_ip', 'date_ord', 'open_ewma_v_ip_seg', 'open_LR']
        self.assertEqual(list(relay.dataframe.columns), cols)
        self.assertTrue(list(aaba_dataframe.index) == list(relay.dataframe.index))