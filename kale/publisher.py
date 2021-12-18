"""Module containing task publishing functionality."""
from __future__ import absolute_import

import logging

from kale import exceptions
from kale import message
from kale import settings
from kale import sqs
from kale import queue_info
from kale import utils

logger = logging.getLogger(__name__)


class Publisher(sqs.SQSTalk):
    """Class to manage publishing SQS tasks."""

    def publish(self, task_class, task_id, payload,
                current_retry_num=None, current_failure_num=None, delay_sec=None):
        """Publish the given task type to the queue with the provided payload.

        :param obj task_class: class of the task that we are publishing.
        :param str task_id: unique identifying string for this task.
        :param dict payload: dictionary for the task payload.
        :param int current_retry_num: current task retry count. If 0, this is
            the first attempt to run the task.
        :param int current_failure_num: current task failure count.
        :param int delay_sec: time (in seconds) that a task should stay
                in the queue before being released to consumers.
        :raises: TaskTooChubbyException: This task is outrageously chubby.
                The publisher of the task should handle this exception and
                determine how to proceed.
        """

        if delay_sec is not None and delay_sec > settings.SQS_MAX_TASK_DELAY_SEC:
            raise exceptions.InvalidTaskDelayException(
                'Invalid task delay_sec (%d > %d).' % (
                    delay_sec, settings.SQS_MAX_TASK_DELAY_SEC))

        queue_class = utils.class_import_from_path(settings.QUEUE_CLASS)
        q_info = queue_info.QueueInfo(settings.QUEUE_CONFIG, self, queue_class)
        queue_obj = q_info.get_queue(task_class.queue)

        if task_class.time_limit >= queue_obj.visibility_timeout_sec:
            raise exceptions.InvalidTimeLimitTaskException(
                'Invalid task time limit: %d >= %d from %s' % (
                    task_class.time_limit, queue_obj.visibility_timeout_sec,
                    settings.QUEUE_CONFIG))

        sqs_queue = self._get_or_create_queue(queue_obj.name)

        kale_msg = message.KaleMessage(
            task_class=task_class,
            task_id=task_id,
            payload=payload,
            current_retry_num=current_retry_num,
            current_failure_num=current_failure_num)

        sqs_queue.send_message(
            MessageBody=kale_msg.encode(),
            DelaySeconds=delay_sec or 0
        )

        logger.debug('Published task. Task id: %s; Task name: %s' % (
            task_id, '%s.%s' % (task_class.__module__, task_class.__name__)))

    def publish_messages_to_dead_letter_queue(self, dlq_name, messages):
        """Sends a batch of messages to the dead letter queue.

        :param str dlq_name: dead-letter-queue name to send these messages to.
        :param list[KaleMessage] messages: a list of KaleMessage instances that
            have permanently failed.
        :raises: SendMessagesException: SQS responded with a partial success. Some
        messages were not delivered.
        """
        sqs_dead_letter_queue = self._get_or_create_queue(dlq_name)

        response = sqs_dead_letter_queue.send_messages(
            Entries=[{
                'Id': m.id,
                'MessageBody': m.encode(),
                'DelaySeconds': 0
            } for m in messages]
        )

        failures = response.get('Failed', [])
        for failure in failures:
            logger.warning('failed to send %s with code %s due to %s',
                           failure['Id'],
                           failure['Code'],
                           failure['Message']
                           )

        if len(failures) > 0:
            raise exceptions.SendMessagesException(len(failures))
