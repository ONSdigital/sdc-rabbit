import json
import logging
import unittest
from unittest.mock import patch

from pika import BlockingConnection
from pika.exceptions import AMQPConnectionError
from pika.adapters.blocking_connection import BlockingChannel
from structlog import wrap_logger

from sdx.common.logger_config import logger_initial_config
from sdx.common.queues import QueuePublisher
from sdx.common.queues.test.test_data import test_data


class TestPublisher(unittest.TestCase):
    logger_initial_config(service_name=__name__,
                          log_level='DEBUG')

    logger = wrap_logger(logging.getLogger(__name__))

    publisher = QueuePublisher(logger,
                               ['http://test/test', 'http://test/test'],
                               'test')

    def test_init(self):
        p = QueuePublisher(self.logger,
                           ['http://test/test', 'http://test/test'],
                           'test')
        self.assertIs(p._logger, self.logger)
        self.assertEqual(p._urls, ['http://test/test', 'http://test/test'])
        self.assertEqual(p._queue, 'test')
        self.assertEqual(p._arguments, {})
        self.assertEqual(p._connection, None)
        self.assertEqual(p._channel, None)

    @patch.object(BlockingConnection,
                  '__init__',
                  side_effect=AMQPConnectionError(1))
    def test_connect_amqp_connection_error(self, mock_class):

        p = QueuePublisher(self.logger,
                           ['http://test/test', 'http://test/test'],
                           'test')

        result = p._connect()
        self.assertEqual(result, False)

    @patch.object(BlockingConnection, '__init__', return_value=None)
    @patch.object(BlockingConnection, 'channel')
    @patch.object(BlockingChannel, 'queue_declare')
    def test_connect_amqpok(self,
                            mock_blocking_connection_init,
                            mock_channel,
                            mock_queue):

        p = QueuePublisher(self.logger,
                           ['http://test/test', 'http://test/test'],
                           'test')

        result = p._connect()
        self.assertEqual(result, True)

    @patch.object(BlockingConnection, 'close')
    def test_disconnect_ok(self, mock_blocking_connection):
        p = QueuePublisher(self.logger,
                           ['http://test/test', 'http://test/test'],
                           'test')

        p._connection = mock_blocking_connection

        with self.assertLogs(logger=__name__, level='DEBUG') as cm:
            p._disconnect()

        msg = "DEBUG:common.queues.test.test_publisher:event='Disconnected from queue'"
        self.assertEqual(cm.output, [msg])

    @patch.object(BlockingConnection, '__init__', return_value=None)
    @patch.object(BlockingConnection, 'channel')
    @patch.object(BlockingChannel, 'queue_declare')
    @patch.object(BlockingConnection, 'close', side_effect=Exception)
    def test_disconnect_error(self,
                              mock_blocking_connection,
                              mock_channel,
                              mock_queue,
                              mock_close):

        p = QueuePublisher(self.logger,
                           ['http://test/test', 'http://test/test'],
                           'test')
        p._connect()

        with self.assertLogs(logger=__name__, level='ERROR') as cm:
            p._disconnect()

        msg = "ERROR:common.queues.test.test_publisher:exception='Exception()'" + \
              " event='Unable to close connection'"
        self.assertEqual(cm.output, [msg])

    def test_publish_message_no_connection(self):
        p = QueuePublisher(self.logger,
                           ['http://test/test', 'http://test/test'],
                           'test')

        result = p.publish_message(json.loads(test_data['valid']))
        self.assertEqual(False, result)
