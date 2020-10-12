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

    def test_success__emmpy_dataframe(self):

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


        s = ""


