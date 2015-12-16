"""Module testing the kale.consumer module."""
from __future__ import absolute_import

import mock
import unittest

from kale import consumer
from kale import settings


class ConsumerTestCase(unittest.TestCase):
    """Test consumer logic."""

    def test_fetch_batch(self):
        """Test fetching a batch of messages"""
        class MockQueue:
            def get_messages(self, *args, **kwargs):
                return []

        with mock.patch('boto.sqs') as mock_sqs:
            mock_sqs.connect_to_region.return_value = mock.MagicMock()
            mock_sqs.connection.SQSConnection.return_value = mock.MagicMock()
            mock_sqs.connect_to_region.lookup.return_value = MockQueue()
            mock_sqs.connection.SQSConnection.lookup.return_value = MockQueue()
            mock_consumer = consumer.Consumer()
            self.assertIsNotNone(mock_consumer.fetch_batch(
                settings.QUEUE_CLASS, 0, 60))
            self.assertIsNotNone(mock_consumer.fetch_batch(
                settings.QUEUE_CLASS, 0, 60, 10))
