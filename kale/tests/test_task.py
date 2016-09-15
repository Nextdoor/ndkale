"""Module testing the kale.task module."""
from __future__ import absolute_import
from __future__ import print_function

import mock
import sys
import traceback
import unittest

from kale import exceptions
from kale import task
from kale import test_utils
from kale import settings
from six.moves import range


class TaskFailureTestCase(unittest.TestCase):
    """Test handle_failure logic."""

    def _create_patch(self, name):
        """Helper method for creating scoped mocks."""
        patcher = mock.patch(name)
        patch = patcher.start()
        self.addCleanup(patcher.stop)
        return patch

    def setUp(self):
        # When using a child process, clear subprocess pool to reload mocked methods.
        task.Task._terminate_task_process_pool()

    def testRunWorker(self):
        """Test running a task."""
        setup_env = self._create_patch(
            'kale.task.Task._setup_task_environment')
        pre_run = self._create_patch('kale.task.Task._pre_run')
        post_run = self._create_patch('kale.task.Task._post_run')
        clean_env = self._create_patch(
            'kale.task.Task._clean_task_environment')

        settings.RUN_TASK_AS_CHILD = False

        task_inst = test_utils.new_mock_task(task_class=test_utils.MockTask)
        task_args = [1, 'a']

        task_inst.run(*task_args)

        setup_env.assert_called_once_with()
        pre_run.assert_called_once_with(*task_args)
        post_run.assert_called_once_with(*task_args)
        clean_env.assert_called_once_with(
            task_id='mock_task', task_name='kale.test_utils.MockTask')

    def testRunWorkerChildProcess(self):
        """Test running a task in the child process.."""
        settings.RUN_TASK_AS_CHILD = True

        task_inst = test_utils.new_mock_task(task_class=test_utils.MockTask)
        task_args = [1, 'a']

        task_inst.run(*task_args)

    def testRunWorkerFailTask(self):
        """Test running a task."""
        setup_env = self._create_patch(
            'kale.task.Task._setup_task_environment')
        pre_run = self._create_patch('kale.task.Task._pre_run')
        post_run = self._create_patch('kale.task.Task._post_run')
        clean_env = self._create_patch(
            'kale.task.Task._clean_task_environment')

        settings.RUN_TASK_AS_CHILD = False

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

    def testRunWorkerFailTaskChildProcess(self):
        """Test running a task in the child process.."""
        settings.RUN_TASK_AS_CHILD = True

        task_inst = test_utils.new_mock_task(task_class=test_utils.FailTask)
        task_inst._start_time = 1
        task_args = [1, 'a']

        with self.assertRaises(exceptions.TaskException):
            task_inst.run(*task_args)

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
            task_inst, retry_num=test_utils.FailTask.max_retries)

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
            task_inst, retry_num=test_utils.FailTask.max_retries)

        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = exceptions.TaskException('Exception')
            retried = test_utils.FailTask.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_RETRIES_EXCEEDED, False)

    def testTaskRuntimeExceeded(self):
        """Task task failing from timeout."""

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
                    message, exceptions.TaskException('Exception'))
                self.assertTrue(retried)
                publish_func.assert_called_once_with(
                    test_utils.FailTask, message.task_id, payload,
                    current_retry_num=(retry + 1), delay_sec=delay_sec)

        retry = retry + 1
        with mock.patch(
                'kale.task.Task._report_permanent_failure') as fail_func:
            exc = exceptions.TaskException('Exception')
            message = test_utils.MockMessage(task_inst, retry_num=retry)
            retried = test_utils.FailTask.handle_failure(message, exc)
            self.assertFalse(retried)
            fail_func.assert_called_once_with(
                message, exc, task.PERMANENT_FAILURE_RETRIES_EXCEEDED, False)

    def testTargetRuntimeExceeded(self):
        """Task task target runtime exceeded."""

        task_inst = test_utils.new_mock_task(
            task_class=test_utils.SlowerThanExpectedTask)

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

        settings.RUN_TASK_AS_CHILD = False

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

    def testBlacklistedTaskFailsChildProcess(self):
        """Test that a blacklisted task raises an exception in the child process.."""
        check_blacklist = self._create_patch('kale.task.Task._check_blacklist')
        raised_exc = exceptions.BlacklistedException()
        check_blacklist.side_effect = raised_exc

        settings.RUN_TASK_AS_CHILD = True

        task_inst = test_utils.new_mock_task(task_class=test_utils.MockTask)
        task_inst._start_time = 1
        task_args = [1, 'a']

        with self.assertRaises(exceptions.BlacklistedException):
            task_inst.run(*task_args)

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

        settings.RUN_TASK_AS_CHILD = False

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

    def testBlacklistedTaskNoRetriesChildProcess(self):
        """Test that a blacklisted task raises an exception in the child process.."""
        check_blacklist = self._create_patch('kale.task.Task._check_blacklist')
        raised_exc = exceptions.BlacklistedException()
        check_blacklist.side_effect = raised_exc

        settings.RUN_TASK_AS_CHILD = True

        mock_message = test_utils.new_mock_message(
            task_class=test_utils.MockTask)
        task_inst = mock_message.task_inst
        task_inst._start_time = 1
        task_args = [1, 'a']

        with self.assertRaises(exceptions.BlacklistedException):
            task_inst.run(*task_args)

        # Check that task
        permanent_failure = not task_inst.__class__.handle_failure(
            mock_message, raised_exc)
        self.assertTrue(permanent_failure)

    def testExceptionStacktrace(self):
        """Test that the exception stacktrace is perserved."""
        settings.RUN_TASK_AS_CHILD = False

        task_inst = test_utils.new_mock_task(task_class=test_utils.MultiFunctionTask)
        task_inst._start_time = 1
        expected_stack_trace_lines = [
            'task_inst.run()',
            'self._run_task(*args, **kwargs)',
            'self.run_task(*args, **kwargs)',
            'self._a()',
            'self._b()',
            'self._c()',
            "raise exceptions.TaskException('Task failed.')"]

        # Run in try/except instead of assertRaises to use sys.exc_info.
        try:
            task_inst.run()
            self.fail('Should have raised TaskException')
        except exceptions.TaskException:
            stack_trace = ''.join(traceback.format_exception(*sys.exc_info())).split('\n')
            # Remove all lines that aren't the actual function calls.
            stack_trace_lines = [line.strip() for line in stack_trace if line and
                                 'File' not in line and
                                 'Traceback' not in line and
                                 'TaskException: ' not in line]
            self.assertEqual(stack_trace_lines, expected_stack_trace_lines)

        self.assertTrue(task_inst._end_time > 0)
        self.assertTrue(task_inst._task_latency_sec > 0)

    def testExceptionStacktraceChildProcess(self):
        """Test that the exception stacktrace is perserved when running as a child process."""
        settings.RUN_TASK_AS_CHILD = True

        task_inst = test_utils.new_mock_task(task_class=test_utils.MultiFunctionTask)
        task_inst._start_time = 1
        expected_stack_trace_lines = [
            'task_inst.run()',
            'self._run_task_as_child(*args, **kwargs)',
            'exc.reraise()',
            'utils.raise_(*self.exc_info)',
            'task_inst.run_task(*args, **kwargs)',
            'self._a()',
            'self._b()',
            'self._c()',
            "raise exceptions.TaskException('Task failed.')"]

        # Run in try/except instead of assertRaises to use sys.exc_info.
        try:
            task_inst.run()
            self.fail('Should have raised TaskException')
        except exceptions.TaskException:
            stack_trace = ''.join(traceback.format_exception(*sys.exc_info())).split('\n')
            # Remove all lines that aren't the actual function calls.
            # Note: exc.with_traceback is the function call added by future.utils.raise_ if py3.
            stack_trace_lines = [line.strip() for line in stack_trace if line and
                                 'File' not in line and
                                 'Traceback' not in line and
                                 'TaskException: ' not in line and
                                 'exc.with_traceback' not in line]
            self.assertEqual(stack_trace_lines, expected_stack_trace_lines)

        self.assertTrue(task_inst._end_time > 0)
        self.assertTrue(task_inst._task_latency_sec > 0)
