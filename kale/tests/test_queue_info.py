"""Tests queue_info.py"""

import tempfile
import unittest
import mock

from kale import queue_info
from kale import sqs


class QueueInfoTest(unittest.TestCase):
    """Tests for QueueInfo class."""

    test_string = ('default: \n'
                   '    name: default\n'
                   '    priority: 10\n'
                   '    batch_size: 10\n'
                   '    visibility_timeout_sec: 5\n'
                   '    long_poll_time_sec: 5\n'
                   '    num_iterations: 2\n'
                   'digest:\n'
                   '    name: digest\n'
                   '    priority: 22\n'
                   '    batch_size: 11\n'
                   '    visibility_timeout_sec: 55\n'
                   '    long_poll_time_sec: 65\n'
                   '    num_iterations: 13\n'
                   'lowp:\n'
                   '    name: lowp\n'
                   '    priority: 1\n'
                   '    batch_size: 10\n'
                   '    visibility_timeout_sec: 5\n'
                   '    long_poll_time_sec: 5\n'
                   '    num_iterations: 2\n')

    def test_get_queues_from_config(self):
        """Success case for get_queues_from_config.
        Don't have failure case. If fails, fails loudly.
        """
        queue_config = tempfile.NamedTemporaryFile(delete=True)
        queue_config.write(self.test_string)
        queue_config.seek(0)
        queues = queue_info.QueueInfo._get_queues_from_config(
            queue_config.name, queue_info.TaskQueue)
        queue_config.close()
        self.assertEquals(len(queues), 3)
        self.assertEquals(queues[0].name, 'digest')
        self.assertEquals(queues[0].priority, 22)
        self.assertEquals(queues[0].batch_size, 11)
        self.assertEquals(queues[0].visibility_timeout_sec, 55)
        self.assertEquals(queues[0].long_poll_time_sec, 65)
        self.assertEquals(queues[0].num_iterations, 13)
        self.assertEquals(queues[1].name, 'default')
        self.assertEquals(queues[2].name, 'lowp')

    def test_queues(self):
        with mock.patch('boto.sqs') as mock_sqs:
            mock_sqs_connection = mock.MagicMock()
            mock_queue = mock.MagicMock()

            mock_sqs_connection.lookup.return_value = mock_queue
            mock_sqs_connection.create_queue.return_value = mock_queue

            conn = mock_sqs.connection
            conn.SQSConnection.return_value = mock_sqs_connection
            mock_sqs.connect_to_region.return_value = mock_sqs_connection

            sqs_inst = sqs.SQSTalk()
            sqs_inst._connection = mock_sqs_connection

            queue_config = tempfile.NamedTemporaryFile(delete=True)
            queue_config.write(self.test_string)
            queue_config.seek(0)
            queue_info.QueueInfo._queues = None
            queue_info.QueueInfo._simple_name_queues_map = None
            qinfo = queue_info.QueueInfo(queue_config.name, sqs_inst,
                                         queue_info.TaskQueue)
            queues = qinfo.get_queues()
            self.assertEquals(len(queues), 3)

            # TODO (wenbin): add a separate test case for
            # get_highest_priority_non_empty_queue.

    def test_not_implemented_ops(self):
        queue_info_base = queue_info.QueueInfoBase()

        with self.assertRaises(NotImplementedError):
            queue_info_base.get_queues()

        with self.assertRaises(NotImplementedError):
            queue_info_base.get_highest_priority_non_empty_queue()

        with self.assertRaises(NotImplementedError):
            queue_info_base.is_queue_empty(mock.MagicMock())
