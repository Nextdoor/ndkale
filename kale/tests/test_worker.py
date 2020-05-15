"""Module testing the kale.worker module."""
from __future__ import absolute_import

import mock
import signal
import unittest

from kale import exceptions
from kale import test_utils
from kale import worker

from six.moves import range


class WorkerTestCase(unittest.TestCase):
    """Test worker logic."""

    def _create_patch(self, name):
        """Helper method for creating scoped mocks."""
        patcher = mock.patch(name)
        patch = patcher.start()
        self.addCleanup(patcher.stop)
        return patch

    def testRun(self):
        """Test an iteration that has tasks."""
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None

        startup_handler = self._create_patch('kale.settings.ON_WORKER_STARTUP')

        worker_inst = worker.Worker()

        self.assertTrue(worker_inst is not None)
        startup_handler.assert_called_once_with()

    def testRunIterationWithTasks(self):
        """Test an iteration that has tasks."""
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None

        fetch_batch = self._create_patch('kale.consumer.Consumer.fetch_batch')
        message = test_utils.new_mock_message()
        fetch_batch.return_value = [message]

        run_batch = self._create_patch('kale.worker.Worker._run_batch')
        run_batch.return_value = (1, 1)

        worker_inst = worker.Worker()
        mock_consumer.assert_called_once_with()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()

        self.assertTrue(worker_inst._run_single_iteration())

        self.assertEqual(fetch_batch.called, 1)
        self.assertTrue(worker_inst._dirty)
        run_batch.assert_called_once_with([message])

    def testRunIterationWithoutTasks(self):
        """Test an iteration that does not have tasks."""
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None

        fetch_batch = self._create_patch('kale.consumer.Consumer.fetch_batch')
        fetch_batch.return_value = []

        run_batch = self._create_patch('kale.worker.Worker._run_batch')

        worker_inst = worker.Worker()
        mock_consumer.assert_called_once_with()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()

        self.assertFalse(worker_inst._run_single_iteration())
        self.assertFalse(worker_inst._dirty)
        self.assertEqual(fetch_batch.called, 1)
        self.assertFalse(run_batch.called)

    def testCleanupWorkerStop(self):
        """Test cleanup worker."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        release_batch = self._create_patch('kale.worker.Worker._release_batch')
        shutdown_handler = self._create_patch(
            'kale.settings.ON_WORKER_SHUTDOWN')
        sys_exit = self._create_patch('sys.exit')
        worker_inst = worker.Worker()
        mock_consumer.assert_called_once_with()
        release_batch.return_value = (0, 0)
        worker_inst._cleanup_worker(signal.SIGABRT, None)
        release_batch.assert_called_once_with()
        sys_exit.assert_called_once_with(0)
        shutdown_handler.assert_called_once_with()

    def testCleanupWorkerSuspend(self):
        """Test cleanup worker."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        release_batch = self._create_patch('kale.worker.Worker._release_batch')
        sys_exit = self._create_patch('sys.exit')
        worker_inst = worker.Worker()
        mock_consumer.assert_called_once_with()
        release_batch.return_value = (0, 0)
        worker_inst._cleanup_worker(signal.SIGTSTP, None)
        release_batch.assert_called_once_with()
        assert not sys_exit.called, 'System should not have exited.'

    def testReleaseBatchWithTimeToSpare(self):
        """Test releasing a batch where the spare time is over the threshold.
        """
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None
        mock_release = self._create_patch(
            'kale.consumer.Consumer.release_messages')
        mock_delete = self._create_patch(
            'kale.consumer.Consumer.delete_messages')
        mock_publish_dlq = self._create_patch(
            'kale.publisher.Publisher.publish_messages_to_dead_letter_queue')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()
        worker_inst._incomplete_messages = [
            test_utils.new_mock_message() for i in range(2)]
        worker_inst._successful_messages = [
            test_utils.new_mock_message() for i in range(3)]
        worker_inst._failed_messages = [
            test_utils.new_mock_message() for i in range(4)]
        worker_inst._batch_stop_time = 20
        # _batch_stop_time - get_time > RESET_TIMEOUT_THRESHOLD (20 - 10 > 1)
        get_time.return_value = 10

        releasable_messages = worker_inst._incomplete_messages
        deletable_messages = (
            worker_inst._successful_messages + worker_inst._failed_messages)
        num_deleted, num_released = worker_inst._release_batch()
        mock_release.assert_called_once_with(
            releasable_messages, worker_inst._batch_queue.name)
        mock_delete.assert_called_once_with(
            deletable_messages, worker_inst._batch_queue.name)
        assert not mock_publish_dlq.called, ('No messages should have been '
                                             'moved to dlq.')
        self.assertEqual(num_deleted, len(deletable_messages))
        self.assertEqual(num_released, len(releasable_messages))

        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testReleaseBatchWithPermanent(self):
        """Test releasing a batch where the spare time is over the threshold.
        """
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None
        mock_release = self._create_patch(
            'kale.consumer.Consumer.release_messages')
        mock_delete = self._create_patch(
            'kale.consumer.Consumer.delete_messages')
        mock_publish_dlq = self._create_patch(
            'kale.publisher.Publisher.publish_messages_to_dead_letter_queue')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()
        worker_inst._incomplete_messages = [
            test_utils.new_mock_message() for i in range(2)]
        worker_inst._successful_messages = [
            test_utils.new_mock_message() for i in range(3)]
        worker_inst._failed_messages = [
            test_utils.new_mock_message() for i in range(4)]
        # Permanent failures should be a subset of failures.
        worker_inst._permanent_failures = worker_inst._failed_messages[:2]

        worker_inst._batch_stop_time = 20
        # _batch_stop_time - get_time > RESET_TIMEOUT_THRESHOLD (20 - 10 > 1)
        get_time.return_value = 10

        releasable_messages = worker_inst._incomplete_messages
        permament_failures = worker_inst._permanent_failures
        deletable_messages = (
            worker_inst._successful_messages + worker_inst._failed_messages)
        num_deleted, num_released = worker_inst._release_batch()
        mock_release.assert_called_once_with(
            releasable_messages, worker_inst._batch_queue.name)
        mock_delete.assert_called_once_with(
            deletable_messages, worker_inst._batch_queue.name)
        mock_publish_dlq.assert_called_once_with(
            worker_inst._batch_queue.dlq_name, permament_failures)
        self.assertEqual(num_deleted, len(deletable_messages))
        self.assertEqual(num_released, len(releasable_messages))

        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testReleaseBatchWithNoSuccessfulAndNoTimeLeft(self):
        """Test releasing a batch where the spare time is over the threshold.
        """
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None
        mock_release = self._create_patch(
            'kale.consumer.Consumer.release_messages')
        mock_delete = self._create_patch(
            'kale.consumer.Consumer.delete_messages')
        mock_publish_dlq = self._create_patch(
            'kale.publisher.Publisher.publish_messages_to_dead_letter_queue')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()
        worker_inst._successful_messages = []
        worker_inst._incomplete_messages = [
            test_utils.new_mock_message() for i in range(2)]
        worker_inst._failed_messages = [
            test_utils.new_mock_message() for i in range(4)]

        worker_inst._batch_stop_time = 20
        # _batch_stop_time - get_time > RESET_TIMEOUT_THRESHOLD (20 - 19.5 < 1)
        get_time.return_value = 19.5

        deletable_messages = worker_inst._failed_messages
        num_deleted, num_released = worker_inst._release_batch()
        assert not mock_release.called, ('No messages should have '
                                         'been released.')
        # Failed messages should have been deleted.
        mock_delete.assert_called_once_with(
            deletable_messages, worker_inst._batch_queue.name)
        assert not mock_publish_dlq.called, ('No messages should have'
                                             'been moved to dlq.')
        self.assertEqual(num_deleted, len(deletable_messages))
        self.assertEqual(num_released, 0)

        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testReleaseBatchWithNoDeletableAndNoTimeLeft(self):
        """Test releasing a batch where the spare time is over the threshold.
        """
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None
        mock_release = self._create_patch(
            'kale.consumer.Consumer.release_messages')
        mock_delete = self._create_patch(
            'kale.consumer.Consumer.delete_messages')
        mock_publish_dlq = self._create_patch(
            'kale.publisher.Publisher.publish_messages_to_dead_letter_queue')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()
        worker_inst._successful_messages = []
        worker_inst._failed_messages = []
        worker_inst._incomplete_messages = [
            test_utils.new_mock_message() for i in range(2)]

        worker_inst._batch_stop_time = 20
        # _batch_stop_time - get_time > RESET_TIMEOUT_THRESHOLD (20 - 19.5 < 1)
        get_time.return_value = 19.5

        num_deleted, num_released = worker_inst._release_batch()
        assert not mock_release.called, ('No messages should have '
                                         'been released.')
        assert not mock_delete.called, 'No messages should have been deleted.'
        assert not mock_publish_dlq.called, ('No messages should have'
                                             ' been moved to dlq.')
        self.assertEqual(num_deleted, 0)
        self.assertEqual(num_released, 0)

        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testReleaseBatchWithNoDeletableAndWithTimeLeft(self):
        """Test releasing a batch where the spare time is over the threshold.
        """
        mock_consumer = self._create_patch('kale.consumer.Consumer.__init__')
        mock_consumer.return_value = None
        mock_release = self._create_patch(
            'kale.consumer.Consumer.release_messages')
        mock_delete = self._create_patch(
            'kale.consumer.Consumer.delete_messages')
        mock_publish_dlq = self._create_patch(
            'kale.publisher.Publisher.publish_messages_to_dead_letter_queue')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()
        worker_inst._successful_messages = []
        worker_inst._failed_messages = []
        worker_inst._incomplete_messages = [
            test_utils.new_mock_message() for i in range(2)]

        worker_inst._batch_stop_time = 20
        # _batch_stop_time - get_time > RESET_TIMEOUT_THRESHOLD (20 - 19.5 < 1)
        get_time.return_value = 10

        releasable_messages = worker_inst._incomplete_messages

        num_deleted, num_released = worker_inst._release_batch()

        mock_release.assert_called_once_with(
            releasable_messages, worker_inst._batch_queue.name)
        assert not mock_delete.called, 'No messages should have been deleted.'
        assert not mock_publish_dlq.called, ('No messages should have '
                                             'been moved to dlq.')
        self.assertEqual(num_deleted, 0)
        self.assertEqual(num_released, len(releasable_messages))

        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testRunBatchSuccessful(self):
        """Test a successful batch."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()

        worker_inst._batch_stop_time = 100
        # _batch_stop_time - (get_time + task.time_limit) > 0
        # (100 - (10 + 60)) > 0)
        get_time.return_value = 10

        message_batch = [test_utils.new_mock_message()]
        num_messages = len(message_batch)

        worker_inst._run_batch(message_batch)

        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(num_messages, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testRunBatchNoTimeRemaining(self):
        """Test a batch where there is not enough time remaining."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        get_time = self._create_patch('time.time')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()

        worker_inst._batch_stop_time = 50
        # _batch_stop_time - (get_time + task.time_limit) > 0
        # (100 - (10 + 60)) < 0)
        get_time.return_value = 10

        message_batch = [test_utils.new_mock_message()]
        num_messages = len(message_batch)

        worker_inst._run_batch(message_batch)

        self.assertEqual(num_messages, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._failed_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))

    def testRunBatchTaskTimeout(self):
        """Test batch with a task timeout."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        get_time = self._create_patch('time.time')
        mock_failure = self._create_patch(
            'kale.test_utils.TimeoutTask.handle_failure')
        mock_failure.return_value = True

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()

        worker_inst._batch_stop_time = 100
        # _batch_stop_time - (get_time + task.time_limit) > 0
        # (100 - (10 + 60)) > 0)
        get_time.return_value = 10

        message = test_utils.new_mock_message(
            task_class=test_utils.TimeoutTask)
        message_batch = [message]
        num_messages = len(message_batch)

        worker_inst._run_batch(message_batch)

        fail_msg, fail_exc = mock_failure.call_args[0]
        self.assertEqual(fail_msg, message)
        self.assertTrue(type(fail_exc) == exceptions.TimeoutException)
        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))
        self.assertEqual(num_messages, len(worker_inst._failed_messages))

    def testRunBatchTaskException(self):
        """Test batch with a task exception."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        get_time = self._create_patch('time.time')
        mock_failure = self._create_patch(
            'kale.test_utils.FailTask.handle_failure')
        mock_failure.return_value = True

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()

        worker_inst._batch_stop_time = 100
        # _batch_stop_time - (get_time + task.time_limit) > 0
        # (100 - (10 + 60)) > 0)
        get_time.return_value = 10

        message = test_utils.new_mock_message(task_class=test_utils.FailTask)
        message_batch = [message]
        num_messages = len(message_batch)

        worker_inst._run_batch(message_batch)

        fail_msg, fail_exc = mock_failure.call_args[0]
        self.assertEqual(fail_msg, message)
        self.assertTrue(type(fail_exc) == exceptions.TaskException)
        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))
        self.assertEqual(num_messages, len(worker_inst._failed_messages))

    def testRunBatchTaskShouldNotRun(self):
        """Test batch with a task which should not run."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        get_time = self._create_patch('time.time')
        mock_republish = self._create_patch(
            'kale.test_utils.ShouldNotRunTask.republish')

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()

        worker_inst._batch_stop_time = 100
        # _batch_stop_time - (get_time + task.time_limit) > 0
        # (100 - (10 + 60)) > 0)
        get_time.return_value = 10

        message = test_utils.new_mock_message(task_class=test_utils.ShouldNotRunTask)
        message_batch = [message]

        worker_inst._run_batch(message_batch)

        message_republished, failure_count = mock_republish.call_args[0]
        self.assertEqual(message, message_republished)
        self.assertEqual(0, failure_count)
        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(len(message_batch), len(worker_inst._successful_messages))
        self.assertEqual(0, len(worker_inst._permanent_failures))
        self.assertEqual(0, len(worker_inst._failed_messages))

    def testRunBatchTaskExceptionPermanentFailure(self):
        """Test batch with a task exception."""
        mock_consumer = self._create_patch('kale.consumer.Consumer')
        get_time = self._create_patch('time.time')
        mock_failure = self._create_patch(
            'kale.test_utils.FailTask.handle_failure')
        mock_failure.return_value = False

        worker_inst = worker.Worker()
        worker_inst._batch_queue = worker_inst._queue_selector.get_queue()
        mock_consumer.assert_called_once_with()

        worker_inst._batch_stop_time = 100
        # _batch_stop_time - (get_time + task.time_limit) > 0
        # (100 - (10 + 60)) > 0)
        get_time.return_value = 10

        message = test_utils.new_mock_message(task_class=test_utils.FailTask)
        message_batch = [message]
        num_messages = len(message_batch)

        worker_inst._run_batch(message_batch)

        fail_msg, fail_exc = mock_failure.call_args[0]
        self.assertEqual(fail_msg, message)
        self.assertTrue(type(fail_exc) == exceptions.TaskException)
        self.assertEqual(0, len(worker_inst._incomplete_messages))
        self.assertEqual(0, len(worker_inst._successful_messages))
        self.assertEqual(1, len(worker_inst._permanent_failures))
        self.assertEqual(num_messages, len(worker_inst._failed_messages))

    def testCheckProcessExceedingMemory(self):
        """Test process resources method."""
        mock_resource = self._create_patch('resource.getrusage')
        sys_exit = self._create_patch('sys.exit')
        self._create_patch('kale.consumer.Consumer')

        worker_inst = worker.Worker()
        mock_resource.return_value = mock.MagicMock(ru_maxrss=1000000000)

        worker_inst._check_process_resources()
        sys_exit.assert_called_once_with(1)

    def testCheckProcessDirty(self):
        """Test process resources method."""
        mock_resource = self._create_patch('resource.getrusage')
        mock_resource.return_value = mock.MagicMock(ru_maxrss=10)
        sys_exit = self._create_patch('sys.exit')
        self._create_patch('kale.consumer.Consumer')
        worker_inst = worker.Worker()
        worker_inst._dirty = True

        self.assertTrue(worker_inst._check_process_resources())
        self.assertFalse(sys_exit.called)

    def testCheckProcessNotDirty(self):
        """Test process resources method."""
        mock_logger = self._create_patch('kale.worker.logger.info')
        mock_resource = self._create_patch('resource.getrusage')
        mock_resource.return_value = mock.MagicMock(ru_maxrss=10)
        sys_exit = self._create_patch('sys.exit')
        self._create_patch('kale.consumer.Consumer')
        worker_inst = worker.Worker()
        worker_inst._dirty = False

        self.assertTrue(worker_inst._check_process_resources())
        self.assertFalse(mock_logger.called)
        self.assertFalse(sys_exit.called)

    def testRemoveMessageOrExitSuccess(self):
        """Test remove_message_or_exit method."""
        sys_exit = self._create_patch('sys.exit')

        worker_inst = worker.Worker()
        worker_inst._incomplete_messages = [1, 2]
        worker_inst.remove_message_or_exit(1)

        self.assertEqual(worker_inst._incomplete_messages, [2])
        sys_exit.assert_not_called()

    def testRemoveMessageOrExitFailure(self):
        """Test remove_message_or_exit method."""
        sys_exit = self._create_patch('sys.exit')

        worker_inst = worker.Worker()
        worker_inst._incomplete_messages = [1, 2]
        worker_inst.remove_message_or_exit(3)

        self.assertEqual(worker_inst._incomplete_messages, [1, 2])
        sys_exit.assert_called()
