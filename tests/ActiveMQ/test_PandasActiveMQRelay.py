from unittest import TestCase
from multiprocessing import shared_memory
from Relay.ActiveMQ.ActiveMQRelayListener import ActiveMQRelayListener
from Relay.ActiveMQ.PandasActiveMQRelay import PandasActiveMQRelay
from Relay.Callbacks.CallbackResult import CallbackResult
from Relay.ActiveMQ.ActiveMQConnectionParameters import ActiveMQConnectionParameters
from Relay.Callbacks import PandasCallbacks
from Relay.Callbacks.CallbackMapping import CallbackMapping
from Relay import Utilities
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


    def test_process(self):

        # Get a dataframe
        aaba_dataframe = load_data_into_dataframe()

        # Prepare messages to send to the queue
        messages = []
        for x in range(0, 5):
            messages.append(aaba_dataframe.loc[x].to_json())

        # Specify some information to configure the relay
        source = "queue/source"
        destination = "queue/destination"

        # Create the shared memory buffer
        from multiprocessing import shared_memory
        sm = shared_memory.SharedMemory(create=True, size=1)
        sm.buf[0] = 0
        sm_name = sm.name

        # Create the mutex
        from threading import Lock
        mutex = Lock()

        # Create the relay
        amq_relay = PandasActiveMQRelay(source, destination, ActiveMQConnectionParameters)
        amq_relay.listener.callbacks = [amq_relay.process, demo_callback]
        amq_relay.listener.callback_args = []
        amq_relay.listener.callback_kwargs = {"sm_name": sm_name, "mutex": mutex}

        # Start the relay
        amq_relay.start()

        # Send messages to the source queue so that the relay can retrieve them
        for message in messages:
            amq_relay.connection.send(body=message, destination=source)

        # Wait until all the messages are retrieved
        import time
        while sm.buf[0] < 2:
            time.sleep(1)

        # Cleanup the shared mem
        sm.close()
        sm.unlink()

        # Disconnect from activemq so that no rogue kernels/connections are left floating around
        print("disconnecting")
        amq_relay.stop()

        # Show the dataframe has been updated
        logging.debug(amq_relay.dataframe)

        self.assertEqual(amq_relay.dataframe.shape, (5,8))

    def test_ewma_avg(self):

        # Create the shared memory buffer
        from multiprocessing import Value
        value = Value('i', 0)

        # Create the mutex
        from threading import Lock
        mutex = Lock()

        # Get a dataframe prepare messages to send to the queue
        aaba_dataframe = load_data_into_dataframe()
        messages = []
        n = aaba_dataframe.shape[0]
        for x in range(0, n):
            df = aaba_dataframe.loc[[x]]
            message = df.to_json(date_unit='ns', orient='records')
            messages.append(message)

        # Specify some information to configure the relay
        source = "queue/source"
        destination = "queue/destination"

        # Create a PandasActiveMQ relay to retrieve, process, and publish messages
        relay1 = PandasActiveMQRelay(source, destination, ActiveMQConnectionParameters)
        relay1.listener.callback_mappings.append(CallbackMapping(relay1.process, [], {}))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.calculate_exponential_moving_average,
            [],
            {
                "column_name": "open",
                "new_column_suffix": "_ewma",
                "window": 3,
                "decay": 0.9,
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.calculate_velocity,
            [],
            {
                "column_name": "open_ewma",
                "new_column_suffix": "_v",
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.calculate_inflection_point,
            [],
            {
                "column_name": "open_ewma_v",
                "new_column_suffix": "_ip",
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.calculate_ordinals,
            [],
            {
                "column_name": "date",
                "new_column_suffix": "_ord",
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.linear_regression_prediction,
            [],
            {
                "column_name": "open",
                "pred_suffix": "_pred",
                "slope_suffix": "_pred_dy",
                "train_x_name": "date_ord",
                "train_y_name": "open",
                "ip_column_name": "open_ewma_v_ip",
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.determine_buy_sell_signal,
            [],
            {
                "column_name": "open",
                "slope_column_name": "open_pred_dy",
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            PandasCallbacks.send_message,
            [],
            {
                "relay": relay1
            }
        ))
        relay1.listener.callback_mappings.append(CallbackMapping(
            demo_callback,
            [],
            {
                "value": value,
                "mutex": mutex,
                "relay": relay1
            }
        ))

        relay2 = PandasActiveMQRelay(destination, destination, ActiveMQConnectionParameters)
        relay2.listener.callback_mappings.append(CallbackMapping(relay2.process, [], {}))
        relay2.listener.callback_mappings.append(CallbackMapping(
            demo_callback,
            [],
            {
                "value": value,
                "mutex": mutex,
                "relay": relay2
            }
        ))

        # Start the relays
        logging.debug("Starting Relays")
        relay1.start()

        # Send messages to the source queue so that the relay can retrieve them
        logging.debug("Sending messages")
        for message in messages:
            relay1.connection.send(body=message, destination=source)

        # Wait until all the messages are processed
        import time
        go = True
        while go:
            mutex.acquire()
            if value.value >= len(messages):
                go = False
            mutex.release()
            time.sleep(1)

        # Start the second relay
        relay2.start()

        # Wait until all the messages are processed
        import time
        go = True
        while go:
            mutex.acquire()
            if value.value >= len(messages)*2:
                go = False
            mutex.release()
            time.sleep(1)


        # Disconnect from activemq so that no rogue kernels/connections are left floating around
        logging.debug("disconnecting")
        relay1.stop()
        relay2.stop()

        logging.debug(os.linesep + relay2.dataframe.to_string())

        time.sleep(2)

        # Show the dataframe has been updated
#        self.assertEqual((len(messages),14), relay2.dataframe.shape)

