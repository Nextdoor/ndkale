"""Module for running the worker process.

It's an infinite loop.
"""
from __future__ import absolute_import

import logging
import resource
import signal
import sys
import time

from kale import consumer
from kale import publisher
from kale import queue_info
from kale import settings
from kale import utils
from six.moves import range

logger = logging.getLogger(__name__)

SIGNALS_TO_HANDLE = (
    signal.SIGABRT,
    # This will catch Ctrl-C interrupts.
    signal.SIGINT,
    signal.SIGQUIT,
    signal.SIGTERM,
    # Handle Ctrl-Z/suspend.
    signal.SIGTSTP)

# Logging constants.
LOG_TASK_RESULT_DEFERRED = 'deferred'
LOG_TASK_RESULT_ERROR = 'error'
LOG_TASK_RESULT_SUCCESS = 'success'


class Worker(object):

    def __init__(self):
        """Initialize a worker instance."""

        self._consumer = consumer.Consumer()

        queue_class = utils.class_import_from_path(settings.QUEUE_CLASS)
        q_info = queue_info.QueueInfo(
            config_file=settings.QUEUE_CONFIG,
            sqs_talk=self._consumer,
            queue_cls=queue_class)

        queue_selector_class = utils.class_import_from_path(
            settings.QUEUE_SELECTOR)
        self._queue_selector = queue_selector_class(q_info)

        # The worker will publish permanently failed tasks to a
        # dead-letter-queue.
        self._publisher = publisher.Publisher()

        # Track total messages processed.
        self._total_messages_processed = 0

        # Intialize queue variables used by each batch.
        self._incomplete_messages = []
        self._successful_messages = []
        self._failed_messages = []
        self._permanent_failures = []
        self._batch_stop_time = time.time()
        self._batch_queue = None
        # Monitors whether the worker has been exposed to tasks and may
        # have bloated in memory.
        self._dirty = False

        # Setup signal handling for cleanup.
        for sig in SIGNALS_TO_HANDLE:
            signal.signal(sig, self._cleanup_worker)

        # Allow the client of this library to do any setup before
        # starting the worker.
        settings.ON_WORKER_STARTUP()

    def _on_pre_run_worker(self):
        """Callback function right at the beginning of starting the worker. """
        logger.info('Starting run loop for task worker.')

    def _on_exceeding_memory_limit(self, ru_maxrss):
        """Callback function when the process exceeds memory limit.

        :param int ru_maxrss: maximum resident set size used (in kilobytes).
        """
        logger.info('Memory usages exceeds max: %d. Exiting.' % ru_maxrss)

    def _on_sigtstp(self, num_completed, num_incomplete):
        """Callback function when SIGTSTP is triggered.

        :param int num_completed: the number of tasks completed in this batch.
        :param int num_incomplete: the number of tasks incomplete in the batch.
        """
        logger.info(
            ('Taskworker process suspended. Completed tasks: %d;'
             ' Incomplete: %d') % (
                num_completed, num_incomplete))

    def _on_shutdown(self, num_completed, num_incomplete):
        """Callback function when we shutdown the worker process.

        :param int num_completed: the number of tasks completed in this batch.
        :param int num_incomplete: the number of tasks incomplete in the batch.
        """
        logger.info(('Taskworker process shutting down. Completed tasks: %d;'
                     ' Incomplete: %d') % (num_completed, num_incomplete))

    def _on_pre_batch_run(self, messages):
        """Callback function before running a batch of tasks.

        :param list[KaleMessage] messages: a list of task messages to process.
        """
        logger.info('Start processing %d tasks in a batch in queue %s ...' % (
            len(messages), self._batch_queue.name))

    def _on_post_batch_run(self, num_completed, num_incomplete, messages):
        """Callback function after running a batch of tasks.

        :param int num_completed: the number of tasks completed in this batch.
        :param int num_incomplete: the number of tasks incomplete in the batch.
        :param list[KaleMessage] messages: a list of messages in this batch.
        """
        logger.info(('Finish processing message batch. Completed tasks: %d;'
                     ' Incomplete: %d') % (
            num_completed, num_incomplete))

    def _on_permanent_failure_batch(self):
        """Callback when there are permanently failed tasks in this batch."""
        logger.info(('Moving permamently %d failed tasks to the '
                     'dead-letter-queue %s.') % (
            len(self._permanent_failures), self._batch_queue.dlq_name))

    def _on_task_deferred(self, message, time_remaining_sec):
        """Callback function when a task is deferred.

        :param list[KaleMessage] message: a list of task messages.
        :param int time_remaining_sec: integer for seconds remained in the
            budget of running this batch of tasks.
        """
        task = message.task_inst
        logger.info(('Task deferred. Task id: %s; Queue: %s; '
                     'Time remaining: %d sec') % (
            task.task_id, self._batch_queue.name, time_remaining_sec))

    def _on_task_failed(self, message, time_remaining_sec, err,
                        permanent_failure):
        """Callback function when a task fails.

        :param KaleMessage message: an object of kale.message.KaleMessage.
        :param int time_remaining_sec: integer for seconds remained in the
            budget of running this batch of tasks.
        :param Exception err: object of Exception.
        :param bool permanent_failure: whether this task permanently fails.
        """
        task = message.task_inst
        logger.info(('Task failed. Task id: %s; Queue: %s; '
                     'Time remaining: %d sec') % (
            task.task_id, self._batch_queue.name, time_remaining_sec))

    def _on_task_succeeded(self, message, time_remaining_sec):
        """Callback function when a task succeeds.

        :param KaleMessage message: an object of kale.message.KaleMessage.
        :param int time_remaining_sec: integer for seconds remained in the
            budget of running this batch of tasks.
        """
        task = message.task_inst
        logger.info(('Task succeeded. Task id: %s; Queue: %s; '
                     'Time remaining: %d sec') % (
            task.task_id, self._batch_queue.name, time_remaining_sec))

    def run(self):
        """This method starts the task processing loop."""

        self._on_pre_run_worker()

        while self._check_process_resources():
            self._batch_queue = self._queue_selector.get_queue()
            for i in range(self._batch_queue.num_iterations):
                if not self._run_single_iteration():
                    # If the iteration didn't process any tasks break out
                    # of this loop and move on to another queue.
                    break

    def _check_process_resources(self):
        """Check if the process is still is abusing resources & should continue

        This will check the processes memory usage and gracefully exit if
        it exceeds the maximum value in settings.

        :return: True if still operational, otherwise it will exit the process.
        :rtype: bool
        """
        resource_data = resource.getrusage(resource.RUSAGE_SELF)

        if resource_data.ru_maxrss < (
                settings.DIE_ON_RESIDENT_SET_SIZE_MB * 1024):

            if self._dirty:
                # We only log when the worker has been infected by  tasks.
                logger.info('Worker process data.')
            return True

        # Allow the client of this library to do any setup before
        # shutting down the worker.
        settings.ON_WORKER_SHUTDOWN()

        self._on_exceeding_memory_limit(resource_data.ru_maxrss)

        # Use non-zero exit code.
        sys.exit(1)

    def _cleanup_worker(self, signum, frame):
        """Handle cleanup when the process is sent a signal.

        This will handle releasing tasks in flight and deleting tasks that have
        been completed.
        """
        logger.info('Process sent signal %d. Cleaning up tasks...' % signum)

        num_completed, num_incomplete = self._release_batch()

        # When the process is suspended we release tasks and then return to the
        # main loop.
        if signum == signal.SIGTSTP:
            self._on_sigtstp(num_completed, num_incomplete)
            return
        else:
            # Allow the client of this library to do any setup before
            # shutting down the worker.
            settings.ON_WORKER_SHUTDOWN()
            self._on_shutdown(num_completed, num_incomplete)
            sys.exit(0)

    def _run_single_iteration(self):
        """Run a single iteration of the task processing loop.

        :return: True if we were able to process a batch, False is there were
            no messages.
        """
        message_batch = self._consumer.fetch_batch(
            self._batch_queue.name,
            self._batch_queue.batch_size,
            self._batch_queue.visibility_timeout_sec,
            long_poll_time_sec=self._batch_queue.long_poll_time_sec)

        self._dirty = bool(message_batch)
        if not message_batch:
            # No any messages in this queue. Let's re-select queue again.
            return False

        self._on_pre_batch_run(message_batch)

        self._batch_stop_time = time.time() + \
            self._batch_queue.visibility_timeout_sec

        self._run_batch(message_batch)
        num_completed, num_incomplete = self._release_batch()

        self._on_post_batch_run(num_completed, num_incomplete, message_batch)
        return True

    def _release_batch(self):
        """Release the most recent batch back to SQS.
        This will delete tasks that succeeded and reset the
        visibility timeout for incomplete tasks.

        :return: A two-tuple of the count of tasks that were completed and
            the count of tasks that were incomplete.
        :rtype: tuple
        """
        # Delete from queues (failed messages are re-published as new tasks)
        messages_to_be_deleted = self._successful_messages + \
            self._failed_messages
        # Set timeout to 0 (if there is time left)
        # As an enhancement, we reset the timeout on tasks that didn't get
        # attempted if the remaining timeout is greater than some threshold.
        # and example of this being helpful is if a 5 minute task was given the
        # opportunity to run with 4 minutes left (and declines). This will
        # release the task 4 minutes before it previously would have.

        if (self._batch_stop_time - time.time()) > \
                settings.RESET_TIMEOUT_THRESHOLD:
            messages_to_be_released = self._incomplete_messages
        else:
            messages_to_be_released = []

        if messages_to_be_deleted:
            # Note: This includes failed tasks.
            self._consumer.delete_messages(messages_to_be_deleted,
                                           self._batch_queue.name)

        if messages_to_be_released:
            # This is only tasks that we didn't get the chance to attempt.
            self._consumer.release_messages(messages_to_be_released,
                                            self._batch_queue.name)

        # Send permanently failed tasks to the dead-letter-queue.
        if self._permanent_failures:
            self._publisher.publish_messages_to_dead_letter_queue(
                self._batch_queue.dlq_name, self._permanent_failures)
            self._on_permanent_failure_batch()

        # All messages start as incomplete.
        self._incomplete_messages = []
        self._successful_messages = []
        self._failed_messages = []
        self._permanent_failures = []

        return len(messages_to_be_deleted), len(messages_to_be_released)

    def _run_batch(self, message_batch):
        """Consume as many tasks as possible in visibility timeout.

        :param list[KaleMessage] message_batch: list of consumable messages.
        """

        # All messages start as incomplete.
        self._incomplete_messages = message_batch
        self._successful_messages = []
        self._failed_messages = []

        # These messages will be sent to the dead-letter-queue.
        self._permanent_failures = []

        # Set visibility timeout start time.
        for message in message_batch:
            task_inst = message.task_inst
            time_remaining_sec = self._batch_stop_time - time.time()

            if task_inst.time_limit >= time_remaining_sec:
                # Greedily continue to process tasks, this task
                # is already in the incomplete_messages list
                self._on_task_deferred(message, time_remaining_sec)
                continue

            # Add cleanup method when tasks are timed out?
            try:
                message.task_inst.run(*message.task_args, **message.task_kwargs)
                self._successful_messages.append(message)
                self._incomplete_messages.remove(message)
            except Exception as err:
                # Re-publish failed tasks.
                # As an optimization we could run all of the failures from a
                # batch together.
                permanent_failure = not task_inst.__class__.handle_failure(
                    message, err)
                if permanent_failure and settings.USE_DEAD_LETTER_QUEUE:
                    self._permanent_failures.append(message)

                self._failed_messages.append(message)
                self._incomplete_messages.remove(message)

                self._on_task_failed(message, time_remaining_sec, err,
                                     permanent_failure)
            else:
                self._on_task_succeeded(message, time_remaining_sec)

            # Increment total messages counter.
            self._total_messages_processed += 1
