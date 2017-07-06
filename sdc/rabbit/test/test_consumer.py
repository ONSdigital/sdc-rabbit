import json
import logging
import unittest
from unittest.mock import patch

from structlog import wrap_logger

from sdx.common.exceptions import BadMessageError, DecryptError, RetryableError
from sdx.common.logger_config import logger_initial_config
from sdx.common.queues import Consumer, ResponseProcessor


class DotDict(dict):

    __getattr__ = dict.get


class TestConsumer(unittest.TestCase):
    logger_initial_config(service_name=__name__,
                          log_level='DEBUG')

    logger = wrap_logger(logging.getLogger(__name__))

    def setUp(self):
        self.response_processor = ResponseProcessor(self.logger,
                                                    'http://test/',
                                                    '/',
                                                    'test',
                                                    'test')
        self.consumer = Consumer('/',
                                 'test',
                                 'test',
                                 'quarantine',
                                 ['http://test/test'],
                                 self.logger,
                                 self.response_processor,
                                 True)

        self.props = DotDict({'headers': {'tx_id': 'test',
                                          'x-delivery-count': 0}})
        self.props_no_tx_id = DotDict({'headers': {'x-delivery-count': 0}})
        self.props_no_x_delivery_count = DotDict({'headers': {'tx_id': 'test'}})
        self.basic_deliver = DotDict({'delivery_tag': 'test'})
        self.body = json.loads('"{test message}"')

    def test_queue_attributes(self):

        self.assertEqual(self.consumer._exchange, '/')
        self.assertEqual(self.consumer._exchange_type, 'test')
        self.assertEqual(self.consumer._queue, 'test')
        self.assertEqual(self.consumer._rabbit_urls, ['http://test/test'])
        self.assertIs(self.consumer.logger, self.logger)
        self.assertEqual(self.consumer._durable_queue, True)

        self.assertEqual(self.consumer.quarantine_publisher._queue, 'quarantine')
        self.assertEqual(self.consumer.quarantine_publisher._urls,
                         ['http://test/test'])
        self.assertIs(self.consumer.quarantine_publisher._logger, self.logger)

    def test_get_tx_id_from_properties(self):
        self.assertEqual('test', self.consumer.get_tx_id_from_properties(self.props))

        with self.assertRaises(KeyError):
            self.consumer.get_tx_id_from_properties(self.props_no_tx_id)

    def test_get_delivery_count_from_properties(self):
        count = self.consumer.get_delivery_count_from_properties(self.props)
        self.assertEqual(count, 1)

    def test_get_delivery_count_from_properties_no_header(self):
        with self.assertRaises(KeyError):
            with self.assertLogs(logger=__name__, level='ERROR') as cm:
                self.consumer.get_delivery_count_from_properties(self.props_no_x_delivery_count)

        msg = "ERROR:common.queues.test.test_consumer:event='No x-delivery-count in header'"
        self.assertEqual(cm.output[0], msg)

    @patch.object(Consumer, 'reject_message')
    def test_on_message_no_tx_id(self, mock_attribute):
        with self.assertLogs(logger=__name__, level='ERROR') as cm:
            result = self.consumer.on_message(self.basic_deliver,
                                              self.props_no_tx_id,
                                              self.body)
            self.assertEqual(None, result)

        msg = "'Bad message properties - no tx_id'"
        self.assertIn(msg, cm.output[1])

    @patch.object(Consumer, 'reject_message')
    def test_on_message_no_x_delivery_count(self, mock_attribute):
        with self.assertLogs(logger=__name__, level='ERROR') as cm:
            result = self.consumer.on_message(self.basic_deliver,
                                              self.props_no_x_delivery_count,
                                              self.body)
            self.assertEqual(None, result)

        msg = "'Bad message properties - no delivery count'"
        self.assertIn(msg, cm.output[1])

    @patch.object(ResponseProcessor, 'process', side_effect=RetryableError)
    @patch.object(Consumer, 'nack_message')
    def test_on_message_retryable_error(self, mock_processor, mock_consumer):
        with self.assertLogs(logger=__name__, level='ERROR') as cm:
            r = self.consumer.on_message(self.basic_deliver,
                                         self.props,
                                         self.body.encode())
            self.assertEqual(r, None)

        msg = "'Failed to process"
        self.assertIn(msg, cm.output[0])

    @patch.object(ResponseProcessor, 'process', side_effect=DecryptError)
    @patch.object(Consumer, 'reject_message')
    def test_on_message_decrypt_error(self, mock_processor, mock_consumer):
        with self.assertLogs(logger=__name__, level='ERROR') as cm:
            r = self.consumer.on_message(self.basic_deliver,
                                         self.props,
                                         self.body.encode())
            self.assertEqual(r, None)

        msg = "'Bad decrypt"
        self.assertNotEqual(msg, cm.output[0])

    @patch.object(ResponseProcessor, 'process', side_effect=BadMessageError)
    @patch.object(Consumer, 'reject_message')
    def test_on_message_badmessage_error(self, mock_processor, mock_consumer):
        with self.assertLogs(logger=__name__, level='ERROR') as cm:
            r = self.consumer.on_message(self.basic_deliver,
                                         self.props,
                                         self.body.encode())
            self.assertEqual(r, None)

        msg = "'Bad message'"
        self.assertNotEqual(msg, cm.output[0])
