"""Module testing the kale.sqs module."""
from __future__ import absolute_import

import unittest

import boto3
import mock
from moto import mock_sqs

from kale import exceptions
from kale import sqs


class SQSTestCase(unittest.TestCase):
    """Test SQSTalk logic"""

    _previous_region = None

    def setUp(self):
        self.mock_sqs = mock_sqs()
        self.mock_sqs.start()
        sqs.SQSTalk._queues = {}

    def tearDown(self):
        self.mock_sqs.stop()

    def test_create_queue(self):
        boto3.setup_default_session()

        sqs_inst = sqs.SQSTalk()

        sqs_inst._get_or_create_queue('LowPriorityTest1')
        sqs_inst._get_or_create_queue('HighPriorityTest2')
        sqs_inst._get_or_create_queue('HighPriorityTest2-dlq')

        expected_low_queue = sqs_inst._sqs.Queue('https://queue.amazonaws.com/123456789012/'
                                                 'LowPriorityTest1')
        expected_hi_queue = sqs_inst._sqs.Queue('https://queue.amazonaws.com/123456789012/'
                                                'HighPriorityTest2')

        expected_hi_dlq_queue = sqs_inst._sqs.Queue('https://queue.amazonaws.com/123456789012/'
                                                'HighPriorityTest2-dlq')


        self.assertEqual(len(sqs_inst._queues), 3)

        self.assertEqual(expected_low_queue, sqs_inst._queues['LowPriorityTest1'])
        self.assertEqual(expected_hi_queue, sqs_inst._queues['HighPriorityTest2'])

    def test_get_queues(self):
        boto3.setup_default_session()
        sqs_inst = sqs.SQSTalk()

        sqs_inst._get_or_create_queue('LowPriorityTest3')
        sqs_inst._get_or_create_queue('HighPriorityTest4')

        queues = sqs_inst.get_all_queues()
        print(queues)
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
