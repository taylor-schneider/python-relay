from unittest import TestCase
from Relay.Callbacks.Utilities import convert_date_string_to_date, df_row_to_json
from Relay.ActiveMQ.PandasActiveMQRelay import PandasActiveMQRelay
from Relay.ActiveMQ.ActiveMQRelayListener import ActiveMQRelayListener
from Relay.Callbacks.CallbackMapping import CallbackMapping
from Relay.Callbacks.PandasCallbacks import demo_algo_callback

import pandas





class test_demo_algo_callback(TestCase):

    def test_success_emmpy_dataframe(self):

        # Setup a dummy relay
        relay = DummyRelay()
        relay.process = PandasActiveMQRelay.process

        # Create a dummy message
        df = pandas.DataFrame({
            "open": [5],
            "date": [convert_date_string_to_date('2019-07-01')]
        })
        message = df_row_to_json(df, 0)

        # Create the callback mappings for the relay's listener
        relay.listener.callback_mappings = [
            CallbackMapping(
                relay.process,
                relay,
                {
                    "relay": relay,
                    "column_name": open,
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip"
                }
            ),
            CallbackMapping(
                demo_algo_callback,
                relay,
                {
                    "relay": relay,
                    "column_name": open,
                    "ewma_suffix": "_ewma",
                    "v_suffix": "_v",
                    "ip_suffix": "_ip"
                }
            ),
        ]

        relay.listener.on_message({}, message)

        self.assertEqual(df.dtypes, relay.dataframe.dtypes)
        self.assertEqual(df.columns, relay.dataframe.columns)

        #cr2 = demo_algo_callback(**kwargs)

        s = ""


