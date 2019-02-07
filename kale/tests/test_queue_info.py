"""Tests queue_info.py"""
from __future__ import absolute_import

import tempfile
import unittest
import mock
from boto import exception as boto_exception

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
        queue_config.write(self.test_string.encode('utf8'))
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

    def _build_queue_info(self):
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
            queue_config.write(self.test_string.encode('utf8'))
            queue_config.seek(0)
            queue_info.QueueInfo._queues = None
            queue_info.QueueInfo._simple_name_queues_map = None
            qinfo = queue_info.QueueInfo(queue_config.name, sqs_inst,
                                         queue_info.TaskQueue)
        return qinfo

    def test_queues(self):
        qinfo = self._build_queue_info()
        queues = qinfo.get_queues()
        self.assertEquals(len(queues), 3)

        # TODO (wenbin): add a separate test case for
        # get_highest_priority_non_empty_queue.

    def test_not_implemented_ops(self):
        queue_info_base = queue_info.QueueInfoBase()

        with self.assertRaises(NotImplementedError):
            queue_info_base.get_queues()

        with self.assertRaises(NotImplementedError):
            queue_info_base.get_highest_priority_queue_that_needs_work()

        with self.assertRaises(NotImplementedError):
            queue_info_base.is_queue_empty(mock.MagicMock())

        with self.assertRaises(NotImplementedError):
            queue_info_base.does_queue_need_work(mock.MagicMock())

    def test_does_queue_need_work_empty(self):
        with mock.patch.object(queue_info.QueueInfo, 'is_queue_empty', return_value=True):
            qinfo = self._build_queue_info()
            self.assertFalse(qinfo.does_queue_need_work(None))

    def test_does_queue_need_work_non_empty(self):
        with mock.patch.object(queue_info.QueueInfo, 'is_queue_empty', return_value=False):
            qinfo = self._build_queue_info()
            self.assertTrue(qinfo.does_queue_need_work(None))

    def test_does_queue_need_work_rate_limited(self):
        rate_limit_exception = boto_exception.SQSError(None, None)
        rate_limit_exception.code = 'RequestThrottled'
        with mock.patch.object(
                queue_info.QueueInfo, 'is_queue_empty', side_effect=rate_limit_exception):
            qinfo = self._build_queue_info()
            self.assertTrue(qinfo.does_queue_need_work(None))
