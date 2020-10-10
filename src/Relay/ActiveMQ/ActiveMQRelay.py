import stomp
import logging
from Relay.ActiveMQ.ActiveMQRelayListener import ActiveMQRelayListener


class ActiveMQRelay():

    def __init__(self, source, destination, amq_conn_param, listener=None):

        # Set some params for activemq
        self.source = source
        self.destination = destination
        self.amq_conn_param = amq_conn_param

        # Link the relay and the listener
        if listener:
            self.listener = listener
        else:
            self.listener = ActiveMQRelayListener()
            self.listener.callbacks = [self.process]

        self.listener.callback_kwargs["relay"] = self

    def start(self):

        # Connect to ActiveMQ
        logging.debug("Starting ActiveMQRelay Connection")
        self.connection = stomp.Connection([(self.amq_conn_param.host, self.amq_conn_param.port)])
        self.connection.set_listener('', self.listener)
        self.connection.connect(self.amq_conn_param.username, self.amq_conn_param.password, wait=True)

        # Start listening to the source
        logging.debug("Starting ActiveMQRelay Subscription")
        self.connection.subscribe(destination=self.source, id=1, ack='auto')

    # This function is intended to be called by the listener when a message is received
    # Any inheriting classes should override this function
    def process(self, *args, **kwargs):
        try:
            message = kwargs["message"]
            if message:
                logging.debug(message)
        except Exception as e:
            self.cleanup()
            raise e

    # This function allows the relay to send a message to the specified destination
    def send_message(self, message):
        try:
            if message:
                self.connection.send(body=message, destination=self.destination)
        except Exception as e:
            self.cleanup()
            raise e

    # This function makes sure the connection is terminated cleanly
    def stop(self):
        self.connection.disconnect()
