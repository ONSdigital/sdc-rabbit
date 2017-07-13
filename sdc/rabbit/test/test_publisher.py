import json
import logging
import unittest

from pika.exceptions import AMQPConnectionError
from sdx.common.log_levels import set_level

from sdc.rabbit import QueuePublisher
from sdc.rabbit.test.test_data import test_data

set_level('pika', 'CRITICAL')


class TestPublisher(unittest.TestCase):
    logger = logging.getLogger(__name__)

    publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                'amqp://guest:guest@0.0.0.0:5672'],
                               'test')

    bad_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:672',
                                    'amqp://guest:guest@0.0.0.0:672'],
                                   'test')

    def test_init(self):
        this_publisher = QueuePublisher(['amqp://guest:guest@0.0.0.0:5672',
                                         'amqp://guest:guest@0.0.0.0:5672'],
                                        'test')
        self.assertEqual(this_publisher._urls, ['amqp://guest:guest@0.0.0.0:5672',
                                                'amqp://guest:guest@0.0.0.0:5672'])
        self.assertEqual(this_publisher._queue, 'test')
        self.assertEqual(this_publisher._arguments, {})
        self.assertEqual(this_publisher._connection, None)
        self.assertEqual(this_publisher._channel, None)

    def test_connect_amqp_connection_error(self):
        with self.assertRaises(AMQPConnectionError):
            result = self.bad_publisher._connect()
            self.assertEqual(None, result)

    def test_connect_amqpok(self):

        result = self.publisher._connect()
        self.assertEqual(result, True)

    def test_disconnect_ok(self):
        self.publisher._connect()

        with self.assertLogs(level='DEBUG') as cm:
            self.publisher._disconnect()

        msg = 'Disconnected from queue'
        self.assertIn(msg, cm[1][-1])

    def test_disconnect_error(self):

        self.publisher._disconnect()
        with self.assertLogs(level='ERROR') as cm:
            self.publisher._disconnect()

        msg = 'Unable to close connection'
        self.assertIn(msg, cm.output[0])

    def test_publish_message_no_connection(self):
        result = self.bad_publisher.publish_message(json.loads(test_data['valid']))
        self.assertEqual(False, result)
