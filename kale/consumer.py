"""Module containing task consumption functionality."""
from __future__ import absolute_import

import logging

from kale import sqs
from kale import exceptions
from kale.message import KaleMessage

logger = logging.getLogger(__name__)


class Consumer(sqs.SQSTalk):
    """SQS utility class for consuming tasks."""

    def fetch_batch(self, queue_name, batch_size, visibility_timeout_sec,
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
            WaitTimeSeconds=long_poll_time_sec or 1
        )

        if sqs_messages is None:
            return None

        kale_messages = []
        for sqs_message in sqs_messages:
            kale_messages.append(KaleMessage.decode(sqs_message))

        return kale_messages

    def delete_messages(self, messages, queue_name):
        """Remove messages from the queue.

        :param list[KaleMessage] messages: messages to delete.
        :param str queue_name: queue name.
        :raises: DeleteMessagesException: SQS responded with a partial success. Some
        messages were not deleted.
        """
        if not messages:
            return
        queue = self._get_or_create_queue(queue_name)

        entries = []
        for message in messages:
            entries.append({
                'Id': message.sqs_message_id,
                'ReceiptHandle': message.sqs_receipt_handle
            })

        response = queue.delete_messages(
            Entries=entries
        )

        failures = response.get('Failed', [])
        for failure in failures:
            logger.warning('delete of %s failed with code %s due to %s'.format(
                failure['Id'],
                failure['Code'],
                failure['Message']
            ))

        if len(failures) > 0:
            raise exceptions.DeleteMessagesException('%d messages failed to be deleted '
                                                     'from SQS'.format(len(failures)))

    def release_messages(self, messages, queue_name):
        """Releases messages to SQS queues so other workers can pick them up.

        :param list[KaleMessage] messages: messages to release to SQS.
        :param str queue_name: queue name.
        :raises: ChangeMessagesVisibilityException: SQS responded with a partial success. Some
        messages were not released.
        """
        if not messages:
            return

        queue = self._get_or_create_queue(queue_name)

        entries = []
        for message in messages:
            entries.append({
                'Id': message.sqs_message_id,
                'ReceiptHandle': message.sqs_receipt_handle,
                'VisibilityTimeout': 0
            })

        response = queue.change_message_visibility_batch(
            Entries=entries
        )

        failures = response.get('Failed', [])
        for failure in failures:
            logger.warning('change visibility of %s failed with code %s due to %s'.format(
                failure['Id'],
                failure['Code'],
                failure['Message']
            ))

        if len(failures) > 0:
            raise exceptions.ChangeMessagesVisibilityException('%d messages failed to '
                                                               'change visibility in '
                                                               'SQS'.format(len(failures)))
