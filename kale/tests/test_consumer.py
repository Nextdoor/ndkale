"""Module testing the kale.consumer module."""
from __future__ import absolute_import

import unittest

from moto import mock_sqs

from kale import consumer
from kale import settings
from kale import sqs


class ConsumerTestCase(unittest.TestCase):
    """Test consumer logic."""

    _previous_region = None

    def setUp(self):
        self.mock_sqs = mock_sqs()
        self.mock_sqs.start()
        sqs.SQSTalk._queues = {}

    def tearDown(self):
        self.mock_sqs.stop()

    def test_fetch_batch(self):
        c = consumer.Consumer()

        self.assertIsNotNone(c.fetch_batch_by_name(
            settings.QUEUE_CLASS, 10, 60))
        self.assertIsNotNone(c.fetch_batch_by_name(
            settings.QUEUE_CLASS, 10, 60, 2))
