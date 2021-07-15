"""Module containing task consumption functionality."""
from __future__ import absolute_import

import logging

from kale import sqs
from kale import exceptions
from kale.message import KaleMessage

logger = logging.getLogger(__name__)


class Consumer(sqs.SQSTalk):
    """SQS utility class for consuming tasks."""


    # This method is separate only to allow consumption by queue name without extra metadata to support republishing
    # use cases.
    def fetch_batch_by_name(self, queue_name, batch_size, visibility_timeout_sec,
                    long_poll_time_sec=None):
        """Fetches a batch of messages from a queue.
        :param str queue_name: queue name.
        :param int batch_size: batch size.
        :param int visibility_timeout_sec: visibility timeout in seconds.
        :param int long_poll_time_sec: long poll time in seconds.
        :returns: a list of KaleMessage objects, or None if not message found.
        :rtype: list[KaleMessage]
        """
        sqs_queue = self._get_or_create_queue(queue_name)

        sqs_messages = sqs_queue.receive_messages(
            MaxNumberOfMessages=batch_size,
            VisibilityTimeout=visibility_timeout_sec,
            WaitTimeSeconds=long_poll_time_sec or 20
        )

        if sqs_messages is None:
            return None

        return [KaleMessage.decode_sqs(msg) for msg in sqs_messages]


    def fetch_batch(self, task_queue, batch_size, visibility_timeout_sec,
                    long_poll_time_sec=None):
        """Fetches a batch of messages from a queue.

        :param kale.queue_info.TaskQueue task_queue: the task queue to fetch from
        :param int batch_size: batch size.
        :param int visibility_timeout_sec: visibility timeout in seconds.
        :param int long_poll_time_sec: long poll time in seconds.
        :returns: a list of KaleMessage objects, or None if not message found.
        :rtype: list[KaleMessage]
        """
        sqs_queue = self._get_or_create_queue(task_queue.name)

        sqs_messages = sqs_queue.receive_messages(
            MaxNumberOfMessages=batch_size,
            VisibilityTimeout=visibility_timeout_sec,
            WaitTimeSeconds=long_poll_time_sec or 20
        )

        if sqs_messages is None:
            return None

        return [KaleMessage.decode_sqs(msg) for msg in sqs_messages]

    def delete_messages(self, messages, task_queue):
        """Remove messages from the queue.

        :param list[KaleMessage] messages: messages to delete.
        :param kale.queue_info.TaskQueue task_queue: the queue from which to delete.
        :raises: DeleteMessagesException: SQS responded with a partial success. Some
        messages were not deleted.
        """
        if not messages:
            return
        queue = self._get_or_create_queue(task_queue.name)

        response = queue.delete_messages(
            Entries=[{
                'Id': message.id,
                'ReceiptHandle': message.sqs_receipt_handle
            } for message in messages]
        )

        failures = response.get('Failed', [])
        for failure in failures:
            logger.warning('delete of %s failed with code %s due to %s',
                           failure['Id'],
                           failure['Code'],
                           failure['Message']
                           )

        if len(failures) > 0:
            raise exceptions.DeleteMessagesException(len(failures))

    def release_messages(self, messages, task_queue):
        """Releases messages to SQS queues so other workers can pick them up.

        :param list[KaleMessage] messages: messages to release to SQS.
        :param kale.queue_info.TaskQueue task_queue: the queue from which to release.
        :raises: ChangeMessagesVisibilityException: SQS responded with a partial success. Some
        messages were not released.
        """
        if not messages:
            return

        queue = self._get_or_create_queue(task_queue.name)

        response = queue.change_message_visibility_batch(
            Entries=[{
                'Id': message.id,
                'ReceiptHandle': message.sqs_receipt_handle,
                'VisibilityTimeout': 0
            } for message in messages]
        )

        failures = response.get('Failed', [])
        for failure in failures:
            logger.warning('change visibility of %s failed with code %s due to %s',
                           failure['Id'],
                           failure['Code'],
                           failure['Message']
                           )

        if len(failures) > 0:
            raise exceptions.ChangeMessagesVisibilityException(len(failures))
