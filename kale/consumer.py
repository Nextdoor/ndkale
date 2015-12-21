"""Module containing task consumption functionality."""
from __future__ import absolute_import

from kale import sqs


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
        queue = self._get_or_create_queue(queue_name)
        extra_options = {}
        if long_poll_time_sec:
            extra_options['wait_time_seconds'] = long_poll_time_sec
        messages = queue.get_messages(
            num_messages=batch_size,
            visibility_timeout=visibility_timeout_sec,
            **extra_options)
        if messages:
            # Call get_body on all of the messages to decrypt and instantiate
            # each task, this allows us to track each task's dequeue time.
            [message.get_body() for message in messages]
            return messages
        else:
            return None

    def delete_messages(self, messages, queue_name):
        """Remove messages from the queue.

        :param list[KaleMessage] messages: messages to delete.
        :param str queue_name: queue name.
        """
        if not messages:
            return
        queue = self._get_or_create_queue(queue_name)
        self._connection.delete_message_batch(queue, messages)

    def release_messages(self, messages, queue_name):
        """Releases messages to SQS queues so other workers can pick them up.

        :param list[KaleMessage] messages: messages to release to SQS.
        :param str queue_name: queue name.
        """
        if not messages:
            return
        queue = self._get_or_create_queue(queue_name)
        message_tups = [(msg, 0) for msg in messages]
        self._connection.change_message_visibility_batch(queue, message_tups)
