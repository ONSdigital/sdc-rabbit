import logging
import time

import pika
from structlog import wrap_logger

from sdc.rabbit.exceptions import BadMessageError, RetryableError
from sdc.rabbit.exceptions import PublishMessageError, QuarantinableError

logger = logging.getLogger(__name__)
logger = wrap_logger(logger)


class AsyncConsumer:
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self,
                 durable_queue,
                 exchange,
                 exchange_type,
                 rabbit_queue,
                 rabbit_urls):
        """Create a new instance of the AsyncConsumer class.

        :param durable_queue: Boolean specifying whether queue is durable
        :param exchange: RabbitMQ exchange name
        :param exchange_type: RabbitMQ exchange type
        :param rabbit_queue: RabbitMQ queue name
        :param rabbit_urls: List of rabbit urls

        :returns Object of type AsyncConsumer
        :rtype AsyncConsumer

        """
        self._exchange = exchange
        self._exchange_type = exchange_type
        self._durable_queue = durable_queue
        self._queue = rabbit_queue
        self._rabbit_urls = rabbit_urls

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = None

    def connect(self):
        """This method connects to RabbitMQ using a SelectConnection object,
        returning the connection handle.

        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """

        count = 1
        no_of_servers = len(self._rabbit_urls)

        while True:
            server_choice = (count % no_of_servers) - 1

            self._url = self._rabbit_urls[server_choice]

            try:
                logger.info('Connecting', attempt=count)
                return pika.SelectConnection(pika.URLParameters(self._url),
                                             self.on_connection_open,
                                             stop_ioloop_on_close=False)
            except pika.exceptions.AMQPConnectionError as e:
                logger.error("Connection error", exception=e)
                count += 1
                logger.error("Connection sleep", no_of_seconds=count)
                time.sleep(count)

                continue

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        logger.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning('Connection closed, reopening in 5 seconds',
                           reply_code=reply_code,
                           reply_text=reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._closing:

            # Create a new connection
            # The ioloop may be stopped in self._connection._handle_ioloop_stop()
            # depending on the value of 'stop_ioloop_on_close'
            self._connection = self.connect()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        logger.warning('Channel was closed',
                       channel=channel,
                       reply_code=reply_code,
                       reply_text=reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        logger.info('Channel opened', channel=channel)
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        logger.info('Declaring exchange', name=exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self._exchange_type)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        logger.info('Exchange declared')
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info('Declaring queue', name=queue_name)
        self._channel.queue_declare(
            self.on_queue_declareok, queue_name, durable=self._durable_queue
        )

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        logger.info('Binding to rabbit', exchange=self._exchange, queue=self._queue)
        self._channel.queue_bind(self.on_bindok, self._queue, self._exchange)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        msg = 'Consumer was cancelled remotely, shutting down: {0!r]'
        logger.info(msg.format(method_frame))
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag, **kwargs):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        logger.info('Acknowledging message', delivery_tag=delivery_tag, **kwargs)
        self._channel.basic_ack(delivery_tag)

    def nack_message(self, delivery_tag, **kwargs):
        """Negative acknowledge a message

        :param int delivery_tag: The deliver tag from the Basic.Deliver frame

        """
        logger.info('Nacking message', delivery_tag=delivery_tag, **kwargs)
        self._channel.basic_nack(delivery_tag)

    def reject_message(self, delivery_tag, requeue=False, **kwargs):
        """Reject the message delivery from RabbitMQ by sending a
        Basic.Reject RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        logger.info('Rejecting message', delivery_tag=delivery_tag, **kwargs)
        self._channel.basic_reject(delivery_tag, requeue=requeue)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        logger.info(
            'Received message',
            delivery_tag=basic_deliver.delivery_tag,
            app_id=properties.app_id,
            msg=body,
        )
        self.acknowledge_message(basic_deliver.delivery_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._channel.basic_qos(prefetch_count=1)
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self._queue)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        logger.info('Queue bound')
        self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        logger.debug('Running rabbit consumer')
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        logger.info('Stopped')


class TornadoConsumer(AsyncConsumer):
    def connect(self):
        """This method connects to RabbitMQ using a TornadoConnection object,
        returning the connection handle.

        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        count = 1
        no_of_servers = len(self._rabbit_urls)

        while True:
            server_choice = (count % no_of_servers) - 1

            self._url = self._rabbit_urls[server_choice]

            try:
                logger.info('Connecting', attempt=count)
                return pika.adapters.tornado_connection.TornadoConnection(pika.URLParameters(self._url),
                                                                          self.on_connection_open,
                                                                          stop_ioloop_on_close=False)
            except pika.exceptions.AMQPConnectionError as e:
                logger.error("Connection error", exception=e)
                count += 1
                logger.error("Connection sleep", no_of_seconds=count)
                time.sleep(count)

                continue


class MessageConsumer(TornadoConsumer):
    """This is a queue consumer that handles messages from RabbitMQ message queues.

    On receipt of a message it takes a number of params from the message
    properties, processes the message, and (if successful) positively
    acknowledges the publishing queue.

    If a message is not successfuly processed, it can be either negatively
    acknowledged, rejected or quarantined, depending on the type of excpetion
    raised.

    """

    @staticmethod
    def tx_id(properties):
        """
        Gets the tx_id for a message from a rabbit queue, using the
        message properties. Will raise KeyError if tx_id is missing from message
        headers.

        : param properties: Message properties

        : returns: tx_id of survey response
        : rtype: str
        """
        tx = properties.headers['tx_id']
        logger.info("Retrieved tx_id from message properties: tx_id={}".format(tx))
        return tx

    def __init__(self,
                 durable_queue,
                 exchange,
                 exchange_type,
                 rabbit_queue,
                 rabbit_urls,
                 quarantine_publisher,
                 process,
                 check_tx_id=True):
        """Create a new instance of the SDXConsumer class

        : param durable_queue: Boolean specifying whether queue is durable
        : param exchange: RabbitMQ exchange name
        : param exchange_type: RabbitMQ exchange type
        : param rabbit_queue: RabbitMQ queue name
        : param rabbit_urls: List of rabbit urls
        : param quarantine_publisher: Object of type sdc.rabbit.QueuePublisher.
            Will publish quarantined messages to the named queue.
        : param process: Function or method to use for processsing message. Will
            be passed the body of the message as a string decoded using UTF - 8.
            Should raise sdc.rabbit.DecryptError, sdc.rabbit.BadMessageError or
            sdc.rabbit.RetryableError on failure, depending on the failure mode.

        : returns: Object of type SDXConsumer
        : rtype: SDXConsumer

        """
        self.process = process
        if not callable(process):
            msg = 'process callback is not callable'
            raise AttributeError(msg.format(process))

        self.quarantine_publisher = quarantine_publisher
        self.check_tx_id = check_tx_id

        super().__init__(durable_queue,
                         exchange,
                         exchange_type,
                         rabbit_queue,
                         rabbit_urls)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Called on receipt of a message from a queue.

        Processes the message using the self._process method or function and positively
        acknowledges the queue if successful. If processing is not succesful,
        the message can either be rejected, quarantined or negatively acknowledged,
        depending on the failure mode.

        : param basic_deliver: AMQP basic.deliver method
        : param properties: Message properties
        : param body: Message body

        : returns: None

        """
        if self.check_tx_id:
            try:
                tx_id = self.tx_id(properties)

                logger.info('Received message',
                            queue=self._queue,
                            delivery_tag=basic_deliver.delivery_tag,
                            app_id=properties.app_id,
                            tx_id=tx_id)

            except KeyError as e:
                self.reject_message(basic_deliver.delivery_tag)
                logger.error("Bad message properties - no tx_id",
                             action="rejected",
                             exception=str(e))
                return None
        else:
            logger.debug("check_tx_id is False. Not checking tx_id for message.",
                         delivery_tag=basic_deliver.delivery_tag)
            tx_id = None

        try:

            try:
                self.process(body.decode("utf-8"), tx_id)
            except TypeError:
                logger.error('Incorrect call to process method')
                raise QuarantinableError

            self.acknowledge_message(basic_deliver.delivery_tag,
                                     tx_id=tx_id)

        except QuarantinableError as e:
            # Throw it into the quarantine queue to be dealt with
            try:
                self.quarantine_publisher.publish_message(body)
                self.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
                logger.error("Quarantinable error occured",
                             action="quarantined",
                             exception=str(e),
                             tx_id=tx_id)
            except PublishMessageError as e:
                logger.error("Unable to publish message to quarantine queue." +
                             " Rejecting message and requeing.")
                self.reject_message(basic_deliver.delivery_tag,
                                    requeue=True,
                                    tx_id=tx_id)

        except BadMessageError as e:
            # If it's a bad message then we have to reject it
            self.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
            logger.error("Bad message",
                         action="rejected",
                         exception=str(e),
                         tx_id=tx_id)

        except RetryableError as e:
            self.nack_message(basic_deliver.delivery_tag, tx_id=tx_id)
            logger.error("Failed to process",
                         action="nack",
                         exception=str(e),
                         tx_id=tx_id)
