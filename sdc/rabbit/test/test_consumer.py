import json
import logging
import unittest

from sdc.rabbit import AsyncConsumer, MessageConsumer


def raise_exc(exc):
    raise exc


class DotDict(dict):

    __getattr__ = dict.get


class TestConsumer(unittest.TestCase):
    logger = logging.getLogger(__name__)

    def setUp(self):
        self.consumer = AsyncConsumer(True, '/', 'topic', 'test',
                                      ['amqp://guest:guest@0.0.0.0:5672'])
        self.message_consumer = MessageConsumer(self.consumer, lambda x: True)

        self.props = DotDict({'headers': {'tx_id': 'test',
                                          'x-delivery-count': 0}})
        self.props_no_tx_id = DotDict({'headers': {'x-delivery-count': 0}})
        self.props_no_x_delivery_count = DotDict({'headers': {'tx_id': 'test'}})
        self.basic_deliver = DotDict({'delivery_tag': 'test'})
        self.body = json.loads('"{test message}"')

    def test_queue_attributes(self):
        self.assertEqual(self.message_consumer._consumer._exchange, '/')
        self.assertEqual(self.message_consumer._consumer._exchange_type, 'topic')
        self.assertEqual(self.message_consumer._consumer._queue, 'test')
        self.assertEqual(self.message_consumer._consumer._rabbit_urls,
                         ['amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(self.message_consumer._consumer._durable_queue, True)

        self.assertEqual(self.message_consumer._quarantine_publisher._queue,
                         'async_consumer_quarantine')
        self.assertEqual(self.message_consumer._quarantine_publisher._urls,
                         ['amqp://guest:guest@0.0.0.0:5672'])

    def test_tx_id(self):
        self.assertEqual('test', self.message_consumer.tx_id(self.props))

        with self.assertRaises(KeyError):
            self.message_consumer.tx_id(self.props_no_tx_id)

    def test_delivery_count(self):
        count = self.message_consumer.delivery_count(self.props)
        self.assertEqual(count, 1)

    def test_delivery_count_no_header(self):
        with self.assertRaises(KeyError):
            self.message_consumer.delivery_count(
                self.props_no_x_delivery_count)
