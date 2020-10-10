# The ActiveMQRelayListener is intended to be a based class for all the relays being built

# The class provides functionality for creating callbacks which use the multiprocessing SharedMemory object
# https://docs.python.org/3/library/multiprocessing.shared_memory.html

# By default, the Listener will call the processing function on the relay when it receives a message

import stomp
from Relay.ActiveMQ.CallbackResult import CallbackResult

class ActiveMQRelayListener(stomp.ConnectionListener):

    def __init__(self):

        self.callback = []
        self.callback_args = []
        self.callback_kwargs = {}
        self.relay = None

        super(ActiveMQRelayListener, self).__init__()


    def on_error(self, headers, message):
        raise Exception('received an error "%s"' % message)

    # This function is called when a message is received
    # We will use it to trigger other functions called callbacks
    # Callback functions are expected implement a standard function signature (*args, **kwargs)
    # The callbacks are expected to return a CallbackResult
    # The callback results may contain

    def on_message(self, headers, message):

        args = self.callback_args
        kwargs = self.callback_kwargs
        kwargs.update({"headers": headers, "message": message})

        for callback in self.callbacks:

            # Capture the result of the callback
            result = callback(*args, **kwargs)

            # Update the message
            # Update the kwargs for the next callback if appropriate
            if type(result) == CallbackResult:
                kwargs.update({"message": result.message})
                kwargs.update(result.misc)
