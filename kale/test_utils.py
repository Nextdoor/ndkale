"""Helpful tools for testing kale tasks."""
from __future__ import absolute_import

from kale import exceptions
from kale import message
from kale import queue_info
from kale import queue_selector
from kale import settings
from kale import task


class MockTask(task.Task):

    @classmethod
    def _get_task_id(self, *args, **kwargs):
        return 'mock_task'

    def run_task(self, *args, **kwargs):
        pass


class FailTask(task.Task):

    @classmethod
    def _get_task_id(self, *args, **kwargs):
        return 'fail_task'

    def run_task(self, *args, **kwargs):
        raise exceptions.TaskException('Task failed.')


class ShouldNotRunTask(task.Task):

    @classmethod
    def _get_task_id(cls, *args, **kwargs):
        return "should_not_run_task"

    def should_run_task(self, *args, **kwargs):
        return False

    def run_task(self, *args, **kwargs):
        pass


class TimeoutTask(task.Task):

    @classmethod
    def _get_task_id(self, *args, **kwargs):
        return 'fail_task'

    def run_task(self, *args, **kwargs):
        raise exceptions.TimeoutException('Task failed.')


class SlowButNotTooSlowTask(task.Task):

    time_limit = 100
    target_runtime = 90

    @classmethod
    def _get_task_id(self, *args, **kwargs):
        return 'moderately_slow_task'

    def run_task(self, *args, **kwargs):
        # Ugly trick.
        self._start_time = self._start_time - self.target_runtime


class FailTaskNoRetries(FailTask):

    max_retries = None


class MockMessage(message.KaleMessage):

    def __init__(self, task_inst, task_args=None, task_kwargs=None, app_data=None,
                 retry_num=0, failure_num=0):
        """Instantiate a mock KaleMessage.

        Args:
            task: An instance of a task.
        """
        self.id = 'id'
        self.task_name = task_inst.task_name
        self.task_id = task_inst.task_id
        self.task_args = task_args or []
        self.task_kwargs = task_kwargs or {}
        self.task_app_data = app_data or {}
        self.task_retry_num = retry_num
        self.task_failure_num = failure_num
        self.task_inst = task_inst


class MockConsumer(object):

    def consume(*args, **kwargs):
        return []


class TestQueueClass(queue_info.TaskQueue):
    pass


class TestQueueSelector(queue_selector.SelectQueueBase):

    def __init__(self, queue_info):
        self.queue_info = queue_info

    def get_queue(self, *args, **kwargs):
        """Returns a TaskQueue object."""
        return self.queue_info.get_queues()[0]


def new_mock_message(task_class=None):
    """Create a new mock message.

    Args:
        task_class: Task class to use in message (default is MockTask).
    """
    task_inst = new_mock_task(task_class)
    message = MockMessage(task_inst)
    return message


def new_mock_task(task_class=None):
    """Create a new mock task instance.

    Args:
        task_class: Task class to use in message (default is MockTask).
    """
    task_class = task_class if task_class else MockTask

    mock_payload = {
        'id': task_class._get_task_id(),
        '_enqueued_time': settings.TIMESTAMP_FUNC(),
        '_publisher': settings.PUBLISHER_STR_FUNC()}
    return task_class(mock_payload)
