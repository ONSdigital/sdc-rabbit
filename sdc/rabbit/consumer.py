from .exceptions import BadMessageError, DecryptError, RetryableError
from .async_consumer import AsyncConsumer
from .publisher import QueuePublisher


class Consumer(AsyncConsumer):
    """This is a queue consumer that handles messages from RabbitMQ message queues.

    On receipt of a message it takes a number of params
    from the message properties, processes the message, and (if successful)
    positively acknowledges the publishing queue.

    If a message is not successfuly processed, it can be either negatively
    acknowledged, rejected or quarantined, depending on the type of excpetion
    raised.

    """
    def __init__(self,
                 exchange,
                 exchange_type,
                 rabbit_queue,
                 rabbit_quarantine_queue,
                 rabbit_url,
                 logger,
                 response_processor,
                 durable_queue=True
                 ):
        """Create a new instance of the Consumer class

        :param exchange: Rabbit exchange name
        :param exchange_type: Exchange type
        :param rabbit_queue: Name of the rabbit queue
        :param rabbit_quarantine_queue: Name of the rabbit quarantine queue
        :param rabbit_url: URL of the rabbit queue to connect to
        :param logger: Logger instance of type logging.Logger
        :param response_processer: Object of type ResponseProcessor
        :param durable_queue: Boolean specifying whether durable queues required

        :returns: Object of type Consumer
        :rtype: Consumer

        """

        self.quarantine_publisher = QueuePublisher(logger,
                                                   rabbit_url,
                                                   rabbit_quarantine_queue)

        self.response_processor = response_processor

        super().__init__(durable_queue,
                         exchange,
                         exchange_type,
                         logger,
                         rabbit_queue,
                         rabbit_url)

    def get_delivery_count_from_properties(self, properties):
        """
        Returns the delivery count for a message from the rabbit queue. The
        value is auto-set by rabbitmq.


        :param properties: Message properties

        :returns: Delivery count value
        :rtype: int

        """
        delivery_count = 0
        try:
            delivery_count = properties.headers['x-delivery-count']
            return delivery_count + 1
        except KeyError as e:
            self.logger.error('No x-delivery-count in header')
            raise e

    def get_tx_id_from_properties(self, properties):
        """
        Gets the tx_id for a message from a rabbit queue, using the
        message properties.

        :param properties: Message properties

        :returns: tx_id of survey response
        :rtype: str
        """
        try:
            tx_id = properties.headers['tx_id']
            self.logger.info("Retrieved tx_id from message properties",
                             tx_id=tx_id)
            return tx_id
        except KeyError as e:
            msg = "No tx_id in message properties. Sending message to quarantine"
            self.logger.error(msg)
            raise e

    def on_message(self, basic_deliver, properties, body):
        """Called on receipt of a message from a queue.

        Processes the message using the ResponseProcessor class and positively
        acknowledges the queue if this is successful. If processing is not
        succesful, the message can either be rejected, quarantined or
        negatively acknowledged.

        :param basic_deliver: AMQP basic.deliver method
        :param properties: Message properties
        :param body: Message body

        :returns: None

        """
        try:
            delivery_count = self.get_delivery_count_from_properties(properties)
        except KeyError as e:
            self.reject_message(basic_deliver.delivery_tag)
            self.logger.error("Bad message properties - no delivery count",
                              action="quarantined",
                              exception=e)

            return None

        try:
            tx_id = self.get_tx_id_from_properties(properties)

            self.logger.info('Received message',
                             queue=self._queue,
                             delivery_tag=basic_deliver.delivery_tag,
                             delivery_count=delivery_count,
                             app_id=properties.app_id,
                             tx_id=tx_id)

        except KeyError as e:
            self.reject_message(basic_deliver.delivery_tag)
            self.logger.error("Bad message properties - no tx_id",
                              action="quarantined",
                              exception=e,
                              delivery_count=delivery_count)
            return None

        try:
            self.response_processor.process(body.decode("utf-8"))
            self.acknowledge_message(basic_deliver.delivery_tag, tx_id=tx_id)

        except DecryptError as e:
            # Throw it into the quarantine queue to be dealt with
            self.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
            self.logger.error("Bad decrypt",
                              action="quarantined",
                              exception=e,
                              tx_id=tx_id,
                              delivery_count=delivery_count)

        except BadMessageError as e:
            # If it's a bad message then we have to reject it
            self.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
            self.logger.error("Bad message",
                              action="rejected",
                              exception=e,
                              tx_id=tx_id,
                              delivery_count=delivery_count)

        except (RetryableError) as e:
            self.nack_message(basic_deliver.delivery_tag, tx_id=tx_id)
            self.logger.error("Failed to process",
                              action="nack",
                              exception=e,
                              tx_id=tx_id,
                              delivery_count=delivery_count)
