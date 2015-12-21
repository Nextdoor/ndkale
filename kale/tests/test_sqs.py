"""Module testing the kale.sqs module."""
from __future__ import absolute_import

import mock
import unittest

from kale import exceptions
from kale import sqs


class SQSTestCase(unittest.TestCase):
    """Test SQSTalk logic"""

    @staticmethod
    def mock_all_queues(prefix=''):
        mock_queue = mock.MagicMock()
        if prefix in [None, '']:
            return [mock_queue, mock_queue]
        else:
            return [mock_queue]

    mock_queue = mock.MagicMock()

    def _get_sqs_connection(self):
        with mock.patch('boto.sqs') as mock_sqs:
            mock_connection = mock.MagicMock()
            mock_sqs.connection.SQSConnection.return_value = mock_connection
            mock_sqs.connect_to_region.return_value = mock_connection

            mock_connection.lookup.return_value = self.mock_queue
            mock_connection.create_queue.return_value = self.mock_queue
            mock_connection.get_all_queues = self.mock_all_queues

            sqs_inst = sqs.SQSTalk()
            sqs_inst._connection = mock_connection
            return sqs_inst

    def test_create_queue(self):
        sqs_inst = self._get_sqs_connection()

        sqs_inst._get_or_create_queue('LowPriorityTest')
        self.assertEqual(sqs_inst._queues['LowPriorityTest'], self.mock_queue)

        sqs_inst._get_or_create_queue('HighPriorityTest')
        self.assertEqual(sqs_inst._queues['HighPriorityTest'], self.mock_queue)

    def test_get_queues(self):
        sqs_inst = self._get_sqs_connection()
        queues = sqs_inst.get_all_queues()
        self.assertEqual(len(queues), 2)

        queues = sqs_inst.get_all_queues('Low')
        self.assertEqual(len(queues), 1)

        sqs_inst.get_all_queues('High')
        self.assertEqual(len(queues), 1)

    def test_get_improperly_configured(self):
        with mock.patch('kale.sqs.settings') as mock_settings:
            mock_settings.PROPERLY_CONFIGURED = False
            with self.assertRaises(exceptions.ImproperlyConfiguredException):
                sqs.SQSTalk()
