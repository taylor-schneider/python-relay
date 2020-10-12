from unittest import TestCase
from multiprocessing import shared_memory
from Relay.ActiveMQ.ActiveMQRelayListener import ActiveMQRelayListener
from Relay.ActiveMQ import ActiveMQRelay
from Relay.Callbacks.CallbackResult import CallbackResult
from Relay.ActiveMQ.ActiveMQConnectionParameters import ActiveMQConnectionParameters
import numpy
import pandas

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)6s :: %(threadName)12s :: %(filename)30s :: %(lineno)4d ::  %(message)s')


def demo_callback(*args, **kwargs):
    try:
        # Retrieve params
        message = kwargs["message"]
        sm_name = kwargs["sm_name"]
        mutex = kwargs["mutex"]

        # Update the shared memory buffer in a thread safe way
        mutex.acquire()
        sm = shared_memory.SharedMemory(name=sm_name)
        sm.buf[0] = sm.buf[0] + 1
        message_count = sm.buf[0]
        sm.close()
        mutex.release()

        # Send a nice message to the user
        print("callback {0}: {1}".format(message_count, message))

    except Exception as e:
        print("Callback {0}: encountered an exception")
        print(e)

    return CallbackResult(message, {})

# Define a utility function to help us load the data
def convert_date_string_to_date(input_string):

    # We then do our manipulation
    input_string = input_string.strip()

    # Make it a date
    result = numpy.datetime64(input_string, 'D')

    return result

def load_data_into_dataframe():
    # Read the file into a dataframe
    file_path = "../../nasdaq_2019.csv"
    converter_mapping = {
        "date": convert_date_string_to_date
    }
    nasdaq_dataframe = pandas.read_csv(file_path, converters=converter_mapping)

    # Sort based on the date column
    nasdaq_dataframe = nasdaq_dataframe.sort_values("date")

    aaba_dataframe = nasdaq_dataframe.loc[nasdaq_dataframe.ticker == "AABA"]
    aaba_dataframe.index = range(0, aaba_dataframe.shape[0])  # reset the index to make our life easier

    return aaba_dataframe




class Test_ActiveMQRelay(TestCase):

    def test_simple_relay(self):

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

        # Create a listener object which will execute the callback function when it receives the message
        # Note: We pass it the name of our shared memory object so that it knows where to send signals
        callbacks = [demo_callback]
        callback_args = []
        callback_kwargs = {"sm_name": sm_name, "mutex": mutex}
        listener = ActiveMQRelayListener(callbacks, callback_args, callback_kwargs)

        # Create the relay
        logging.info("Creating the relay")
        amq_relay = ActiveMQRelay(source, destination, ActiveMQConnectionParameters, listener)
        amq_relay.listener.callbacks.insert(0, amq_relay.process)

        # Start the relay
        logging.info("Starting the relay)")
        amq_relay.start()

        # Send messages to the source queue so that the relay can retrieve them
        logging.info("Sending messages")
        amq_relay.connection.send(body=messages[0], destination=source)
        amq_relay.connection.send(body=messages[1], destination=source)

        # Wait until all the messages are retrieved
        logging.info("Waiting")
        import time
        while sm.buf[0] < 2:
            time.sleep(1)

        # Disconnect from activemq so that no rogue kernels/connections are left floating around
        logging.info("disconnecting")
        amq_relay.stop()

        # Cleanup the shared mem
        sm.close()
        sm.unlink()

