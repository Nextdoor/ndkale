"""Module testing the kale.task module."""
from __future__ import absolute_import

import mock
import unittest

from kale import exceptions
from kale import task
from kale import test_utils

from six.moves import range


class TaskFailureTestCase(unittest.TestCase):
    """Test handle_failure logic."""

    def _create_patch(self, name):
        """Helper method for creating scoped mocks."""
        patcher = mock.patch(name)
        patch = patcher.start()
        self.addCleanup(patcher.stop)
        return patch

    def testRunWorker(self):
        """Test running a task."""
        setup_env = self._create_patch(
            'kale.task.Task._setup_task_environment')
        pre_run = self._create_patch('kale.task.Task._pre_run')
        post_run = self._create_patch('kale.task.Task._post_run')
        clean_env = self._create_patch(
            'kale.task.Task._clean_task_environment')

        task_inst = test_utils.new_mock_task(task_class=test_utils.MockTask)
        task_args = [1, 'a']
        task_inst.run(*task_args)

        setup_env.assert_called_once_with()
        pre_run.assert_called_once_with(*task_args)
        post_run.assert_called_once_with(*task_args)
        clean_env.assert_called_once_with(
            task_id='mock_task', task_name='kale.test_utils.MockTask')

    def testRunWorkerFailTask(self):
        """Test running a task."""
        setup_env = self._create_patch(
            'kale.task.Task._setup_task_environment')
        pre_run = self._create_patch('kale.task.Task._pre_run')
        post_run = self._create_patch('kale.task.Task._post_run')
        clean_env = self._create_patch(
            'kale.task.Task._clean_task_environment')

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        task_inst._start_time = 1
        task_args = [1, 'a']

        with self.assertRaises(exceptions.TaskException) as exc_ctxt_mngr:
            task_inst.run(*task_args)

        setup_env.assert_called_once_with()
        pre_run.assert_called_once_with(*task_args)
        assert not post_run.called, '_post_run should not have been called.'
        clean_env.assert_called_once_with(
            task_id='fail_task', task_name='kale.test_utils.FailTask',
            exc=exc_ctxt_mngr.exception)
        self.assertTrue(task_inst._end_time > 0)
        self.assertTrue(task_inst._task_latency_sec > 0)

    def testTaskUnrecoverableException(self):
        """Task task failing with unrecoverable exception."""

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        message = test_utils.MockMessage(task_inst)

        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = SyntaxError('Unrecoverable Error')
            retried = test_utils.FailTask.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_UNRECOVERABLE, True)

    def testDelayedPublish(self):
        task_inst = test_utils.new_mock_task(task_class=test_utils.MockTask)
        delay_sec = 60
        random_arg = 99
        random_kwarg = 100
        payload = {
            'args': (random_arg,),
            'kwargs': {'random_kwarg': random_kwarg, 'delay_sec': delay_sec},
            'app_data': {}}
        with mock.patch(
                'kale.publisher.Publisher.publish') as publish_func:
            task_inst.publish({}, random_arg, delay_sec=delay_sec, random_kwarg=random_kwarg)
            message = test_utils.MockMessage(task_inst)

            publish_func.assert_called_once_with(test_utils.MockTask, message.task_id, payload,
                                                 delay_sec=delay_sec)

    def testTaskNoRetries(self):
        """Task task failing with retries disabled."""

        task_inst = test_utils.new_mock_task(
            task_class=test_utils.FailTaskNoRetries)
        message = test_utils.MockMessage(task_inst)

        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = exceptions.TaskException('Exception')
            retried = test_utils.FailTaskNoRetries.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_NO_RETRY, True)

    def testTaskRetriesExceeded(self):
        """Task task failing with retries exceeded."""

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        message = test_utils.MockMessage(
            task_inst, failure_num=test_utils.FailTask.max_retries)

        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = exceptions.TaskException('Exception')
            retried = test_utils.FailTask.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_RETRIES_EXCEEDED, False)

    def testTaskRetries(self):
        """Task task failing with retries exceeded."""

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        message = test_utils.MockMessage(
            task_inst, failure_num=test_utils.FailTask.max_retries)

        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = exceptions.TaskException('Exception')
            retried = test_utils.FailTask.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_RETRIES_EXCEEDED, False)

    def testTaskRetryDelayWithoutFailure(self):
        """Task task failing with delay without failure"""

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        sample_values = [
            (i, test_utils.FailTask._get_delay_sec_for_retry(i)) for i in
            range(task_inst.max_retries)]
        payload = {
            'args': [],
            'kwargs': {},
            'app_data': {}}

        for retry, delay_sec in sample_values:
            with mock.patch(
                    'kale.publisher.Publisher.publish') as publish_func:
                message = test_utils.MockMessage(task_inst, retry_num=retry)

                retried = test_utils.FailTask.handle_failure(
                    message, exceptions.TaskException('Exception'), increment_failure_num=False)
                self.assertTrue(retried)
                publish_func.assert_called_once_with(
                    test_utils.FailTask, message.task_id, payload,
                    current_failure_num=0, current_retry_num=(retry + 1),
                    delay_sec=delay_sec)

    def testTaskRetryDelayWithFailure(self):
        """Task task retrying with delay with failure"""

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        sample_values = [
            (i, test_utils.FailTask._get_delay_sec_for_retry(i)) for i in
            range(task_inst.max_retries)]
        payload = {
            'args': [],
            'kwargs': {},
            'app_data': {}}

        for failure, delay_sec in sample_values:
            with mock.patch(
                    'kale.publisher.Publisher.publish') as publish_func:
                message = test_utils.MockMessage(task_inst, failure_num=failure, retry_num=failure)

                retried = test_utils.FailTask.handle_failure(
                    message, exceptions.TaskException('Exception'), increment_failure_num=True)
                self.assertTrue(retried)
                publish_func.assert_called_once_with(
                    test_utils.FailTask, message.task_id, payload,
                    current_failure_num=(failure + 1), current_retry_num=(failure + 1),
                    delay_sec=delay_sec)

    def testTaskRuntimeExceeded(self):
        """Task task failing from timeout."""

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)

        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = exceptions.TaskException('Exception')
            message = test_utils.MockMessage(task_inst, retry_num=0,
                                             failure_num=task_inst.max_retries + 1)
            retried = test_utils.FailTask.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_RETRIES_EXCEEDED, False)

    def testTargetRuntimeExceeded(self):
        """Task task target runtime exceeded."""

        task_inst = test_utils.new_mock_task(
            task_class=test_utils.SlowButNotTooSlowTask)

        with mock.patch(
                'kale.task.Task._alert_runtime_exceeded') as time_exceeded:
            task_inst.run()
            self.assertTrue(time_exceeded.called)

    def testBlacklistedTaskFails(self):
        """Test that a blacklisted task raises an exception."""
        setup_env = self._create_patch(
            'kale.task.Task._setup_task_environment')
        pre_run = self._create_patch('kale.task.Task._pre_run')
        run_task = self._create_patch('kale.task.Task.run_task')
        clean_env = self._create_patch(
            'kale.task.Task._clean_task_environment')
        check_blacklist = self._create_patch('kale.task.Task._check_blacklist')
        raised_exc = exceptions.BlacklistedException()
        check_blacklist.side_effect = raised_exc

        task_inst = test_utils.new_mock_task(task_class=test_utils.MockTask)
        task_inst._start_time = 1
        task_args = [1, 'a']

        with self.assertRaises(exceptions.BlacklistedException):
            task_inst.run(*task_args)

        setup_env.assert_called_once_with()
        pre_run.assert_called_once_with(*task_args)
        self.assertFalse(run_task.called)
        clean_env.assert_called_once_with(
            task_id='mock_task', task_name='kale.test_utils.MockTask',
            exc=raised_exc)

    def testBlacklistedTaskNoRetries(self):
        """Test that a blacklisted task raises an exception."""
        setup_env = self._create_patch(
            'kale.task.Task._setup_task_environment')
        pre_run = self._create_patch('kale.task.Task._pre_run')
        run_task = self._create_patch('kale.task.Task.run_task')
        clean_env = self._create_patch(
            'kale.task.Task._clean_task_environment')
        check_blacklist = self._create_patch('kale.task.Task._check_blacklist')
        raised_exc = exceptions.BlacklistedException()
        check_blacklist.side_effect = raised_exc

        mock_message = test_utils.new_mock_message(
            task_class=test_utils.MockTask)
        task_inst = mock_message.task_inst
        task_inst._start_time = 1
        task_args = [1, 'a']

        with self.assertRaises(exceptions.BlacklistedException):
            task_inst.run(*task_args)

        setup_env.assert_called_once_with()
        pre_run.assert_called_once_with(*task_args)
        self.assertFalse(run_task.called)
        clean_env.assert_called_once_with(
            task_id='mock_task', task_name='kale.test_utils.MockTask',
            exc=raised_exc)

        # Check that task
        permanent_failure = not task_inst.__class__.handle_failure(
            mock_message, raised_exc)
        self.assertTrue(permanent_failure)
