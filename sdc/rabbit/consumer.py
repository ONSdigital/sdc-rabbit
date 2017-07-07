import logging

from structlog import wrap_logger

from .exceptions import BadMessageError, DecryptError, RetryableError
from .publisher import QueuePublisher

LOGGER = wrap_logger(logging.getLogger('__name__'))


class MessageConsumer():
    """This is a queue consumer that handles messages from RabbitMQ message queues.

    On receipt of a message it takes a number of params
    from the message properties, processes the message, and (if successful)
    positively acknowledges the publishing queue.

    If a message is not successfuly processed, it can be either negatively
    acknowledged, rejected or quarantined, depending on the type of excpetion
    raised.

    """
    @staticmethod
    def delivery_count(properties):
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
            raise KeyError('No x-delivery-count in header: {}'.format(e))

    @staticmethod
    def tx_id(self, properties):
        """
        Gets the tx_id for a message from a rabbit queue, using the
        message properties.

        :param properties: Message properties

        :returns: tx_id of survey response
        :rtype: str
        """
        try:
            tx = properties.headers['tx_id']
            LOGGER.info("Retrieved tx_id from message properties: tx_id={}".format(tx))
            return tx
        except KeyError as e:
            msg = "No tx_id in message properties. Sending message to quarantine: {}"
            raise KeyError(msg.format(e))

    def __init__(self, consumer, response_processor):
        """Create a new instance of the MessageConsumer class

        :param consumer: Object of type sdc.rabbit.AsyncConsumer
        :param response_processer: Custom response processor object

        :returns: Object of type Consumer
        :rtype: Consumer

        """
        self._consumer = consumer
        self._response_processor = response_processor

        expected_method = getattr(self._response_processor, "process", None)
        if not callable(expected_method):
            msg = 'Object {} does not have a "process" method'
            raise AttributeError(msg.format(response_processor))
        self._quarantine_publisher = QueuePublisher(self.consumer.rabbit_url,
                                                    self.consumer.rabbit_quarantine_queue)

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
            msg = 'Bad message properties - no delivery count'
            LOGGER.error(msg, action="quarantined", exception=e)
            return None

        try:
            tx_id = self.get_tx_id_from_properties(properties)

            LOGGER.info('Received message',
                        queue=self.conssumer._queue,
                        delivery_tag=basic_deliver.delivery_tag,
                        delivery_count=delivery_count,
                        app_id=properties.app_id,
                        tx_id=tx_id)

        except KeyError as e:
            self.reject_message(basic_deliver.delivery_tag)
            LOGGER.error("Bad message properties - no tx_id",
                         action="quarantined",
                         exception=e,
                         delivery_count=delivery_count)
            return None

        try:
            self.response_processor.process(body.decode("utf-8"))
            self.consumer.acknowledge_message(basic_deliver.delivery_tag,
                                              tx_id=tx_id)

        except DecryptError as e:
            # Throw it into the quarantine queue to be dealt with
            self.consumer.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
            LOGGER.error("Bad decrypt",
                         action="quarantined",
                         exception=e,
                         tx_id=tx_id,
                         delivery_count=delivery_count)

        except BadMessageError as e:
            # If it's a bad message then we have to reject it
            self.consumer.reject_message(basic_deliver.delivery_tag, tx_id=tx_id)
            LOGGER.error("Bad message",
                         action="rejected",
                         exception=e,
                         tx_id=tx_id,
                         delivery_count=delivery_count)

        except (RetryableError) as e:
            self.consumer.nack_message(basic_deliver.delivery_tag, tx_id=tx_id)
            LOGGER.error("Failed to process",
                         action="nack",
                         exception=e,
                         tx_id=tx_id,
                         delivery_count=delivery_count)
