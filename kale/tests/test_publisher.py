"""Module testing the kale.publisher module."""
from __future__ import absolute_import

import mock
import unittest

from kale import exceptions
from kale import message
from kale import publisher
from kale import settings
from kale import sqs
from kale import test_utils


class PublisherTestCase(unittest.TestCase):
    """Test publisher logic."""

    def test_publish(self):
        """Test publisher logic."""

        sqs_inst = sqs.SQSTalk()
        sqs_inst._connection = mock.MagicMock()

        with mock.patch(
                'kale.queue_info.QueueInfo.get_queue') as mock_get_queue:
            mock_queue = mock.MagicMock()
            mock_queue.visibility_timeout_sec = 10
            mock_get_queue.return_value = mock_queue
            mock_publisher = publisher.Publisher(sqs_inst)
            mock_publisher._get_or_create_queue = mock.MagicMock()
            payload = {'args': [], 'kwargs': {}}
            mock_task_class = mock.MagicMock()
            mock_task_class.time_limit = 2
            mock_task_class.__name__ = 'task'
            with mock.patch('kale.message.KaleMessage') as mock_message:
                mock_message.create_message.return_value = mock.MagicMock()
                mock_publisher.publish(mock_task_class, 1, payload)

    def test_publish_with_app_data(self):
        """Test publisher logic."""

        sqs_inst = sqs.SQSTalk()
        sqs_inst._connection = mock.MagicMock()

        with mock.patch(
                'kale.queue_info.QueueInfo.get_queue') as mock_get_queue:
            mock_queue = mock.MagicMock()
            mock_queue.visibility_timeout_sec = 10
            mock_get_queue.return_value = mock_queue
            mock_publisher = publisher.Publisher(sqs_inst)
            mock_publisher._get_or_create_queue = mock.MagicMock()
            payload = {'args': [], 'kwargs': {}, 'app_data': {}}
            mock_task_class = mock.MagicMock()
            mock_task_class.time_limit = 2
            mock_task_class.__name__ = 'task'
            with mock.patch('kale.message.KaleMessage') as mock_message:
                mock_message.create_message.return_value = mock.MagicMock()
                mock_publisher.publish(mock_task_class, 1, payload)

    def test_publish_messages_to_dead_letter_queue(self):
        """Test publisher to DLQ logic."""

        sqs_inst = sqs.SQSTalk()
        sqs_inst._connection = mock.MagicMock()
        mock_publisher = publisher.Publisher(sqs_inst)
        mock_queue = mock.MagicMock()
        mock_publisher._get_or_create_queue = mock.MagicMock(
            return_value=mock_queue)

        payload = {'args': [], 'kwargs': {}}
        sqs_msg = message.KaleMessage.create_message(
            task_class=test_utils.MockTask,
            task_id=test_utils.MockTask._get_task_id(),
            payload=payload,
            queue='queue',
            current_retry_num=5)
        sqs_msg.id = 'test-id'
        test_body = 'test-body'
        sqs_msg.get_body_encoded = mock.MagicMock(return_value=test_body)
        mock_messages = [sqs_msg]

        with mock.patch.object(mock_queue, 'write_batch') as mock_write:
            mock_publisher.publish_messages_to_dead_letter_queue(
                'dlq_name', mock_messages)
            expected_args = [(sqs_msg.id, test_body, 0)]
            mock_write.assert_called_once_with(expected_args)

    def test_publish_bad_time_limit_equal(self):
        """Test publish with bad time limit (equal to timeout)."""

        sqs_inst = sqs.SQSTalk()
        sqs_inst._connection = mock.MagicMock()

        with mock.patch(
                'kale.queue_info.QueueInfo.get_queue') as mock_get_queue:
            mock_queue = mock.MagicMock()
            mock_queue.visibility_timeout_sec = 600
            mock_get_queue.return_value = mock_queue
            mock_publisher = publisher.Publisher(sqs_inst)
            mock_publisher._get_or_create_queue = mock.MagicMock()
            payload = {'args': [], 'kwargs': {}}
            mock_task_class = mock.MagicMock()
            mock_task_class.time_limit = 600

            with mock.patch('kale.message.KaleMessage') as mock_message:
                mock_message.create_message.return_value = mock.MagicMock()
                with self.assertRaises(
                        exceptions.InvalidTimeLimitTaskException):
                    mock_publisher.publish(mock_task_class, 1, payload)

    def test_publish_bad_time_limit_greater(self):
        """Test publish with bad time limit (greater than timeout)."""

        sqs_inst = sqs.SQSTalk()
        sqs_inst._connection = mock.MagicMock()

        with mock.patch(
                'kale.queue_info.QueueInfo.get_queue') as mock_get_queue:
            mock_queue = mock.MagicMock()
            mock_queue.visibility_timeout_sec = 600
            mock_get_queue.return_value = mock_queue
            mock_publisher = publisher.Publisher(sqs_inst)
            mock_publisher._get_or_create_queue = mock.MagicMock()
            payload = {'args': [], 'kwargs': {}}
            mock_task_class = mock.MagicMock()
            mock_task_class.time_limit = 601
            with mock.patch('kale.message.KaleMessage') as mock_message:
                mock_message.create_message.return_value = mock.MagicMock()
                with self.assertRaises(
                        exceptions.InvalidTimeLimitTaskException):
                    mock_publisher.publish(mock_task_class, 1, payload)

    def test_publish_invalid_delay_sec(self):
        """Test publish with invalid delay_sec value."""

        sqs_inst = sqs.SQSTalk()
        sqs_inst._connection = mock.MagicMock()

        mock_publisher = publisher.Publisher(sqs_inst)
        mock_publisher._get_or_create_queue = mock.MagicMock()
        payload = {'args': [], 'kwargs': {}}

        mock_task_class = mock.MagicMock()
        mock_task_class.time_limit = 2

        delay_sec = settings.SQS_MAX_TASK_DELAY_SEC + 1
        with mock.patch('kale.message.KaleMessage') as mock_message:
            mock_message.create_message.return_value = mock.MagicMock()
            with self.assertRaises(exceptions.InvalidTaskDelayException):
                mock_publisher.publish(mock_task_class, 1, payload,
                                       delay_sec=delay_sec)
