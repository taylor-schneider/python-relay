# The ActiveMQRelayListener is intended to be a based class for all the relays being built

# The class provides functionality for creating callbacks which use the multiprocessing SharedMemory object
# https://docs.python.org/3/library/multiprocessing.shared_memory.html

# By default, the Listener will call the processing function on the relay when it receives a message

import stomp
from threading import Lock
from Relay.Callbacks.CallbackResult import CallbackResult
import logging

class ActiveMQRelayListener(stomp.ConnectionListener):

    def __init__(self):

        self.callback_mappings = []
        self.relay = None
        self.lock = Lock()

        super(ActiveMQRelayListener, self).__init__()


    def on_error(self, headers, message):
        raise Exception('received an error "%s"' % message)

    # This function is called when a message is received
    # We will use it to trigger other functions called callbacks
    # Callback functions are expected implement a standard function signature (*args, **kwargs)
    # The callbacks are expected to return a CallbackResult
    # The callback results may contain

    def on_message(self, headers, message):

        try:
#            self.lock.acquire()

            # Global kwargs are the base set of kwargs passed to all callbacks
            global_kwargs = {"headers": headers, "message": message}

            for callback_mapping in self.callback_mappings:

                # Set params for callback
                callback = callback_mapping.callback
                callback_args = callback_mapping.args
                callback_kwargs = global_kwargs
                callback_kwargs.update(callback_mapping.kwargs)

                # Capture the result of the callback
                callback_result = callback(*callback_args, **callback_kwargs)

                # Update the message object and update the kwargs for the next callback if appropriate
                if type(callback_result) == CallbackResult:
                    global_kwargs.update({"message": callback_result.message})
                    global_kwargs.update(callback_result.misc)

        except Exception as e:
            logging.error(e)
            raise e
        finally:
            pass
#            self.lock.release()
