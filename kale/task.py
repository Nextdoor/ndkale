"""Module containing the base class for tasks."""
from __future__ import absolute_import

import logging
import time
import uuid

from kale import exceptions
from kale import publisher
from kale import settings

logger = logging.getLogger(__name__)

PERMANENT_FAILURE_UNRECOVERABLE = 'unrecoverable'
PERMANENT_FAILURE_NO_RETRY = 'no_retries'
PERMANENT_FAILURE_RETRIES_EXCEEDED = 'max_retries'


class Task(object):
    """Base class for kale tasks."""

    # Exponential retry settings. Overridable on a per-task basis based on
    # the task type, but these are our defaults.
    # Each time a task is retried its given a timeout before it will be
    # visible to consumers.
    # This is calculated with the following:
    # timeout = (1 << current_retries) * retry_delay_multiple
    _retry_delay_multiple = settings.RETRY_DELAY_MULTIPLE_SEC

    # Number of times a task can be retried after it fails the first time.
    # Set to None or 0 for no retries.
    max_retries = 4

    # Alert on tasks that exceed this time.
    target_runtime = 50

    # Default time until task is killed.
    time_limit = 60

    # Blacklist of exceptions to never
    # retry on (unlikely to be transient).
    EXCEPTION_LIST = (
        exceptions.BlacklistedException,
        KeyError,
        NotImplementedError,
        SyntaxError,
        UnboundLocalError)

    # String representation the a task queue, each task must override
    # this value.
    queue = None

    def __init__(self, message_body=None, *args, **kwargs):
        """Initialize an instance of a task.

        :param message_payload: Payload this task is being created from
                (optionally None for testability).
        """
        # global mocking
        message_body = message_body or {}
        self.task_id = message_body.get('id')
        self.task_name = '%s.%s' % (self.__class__.__module__,
                                    self.__class__.__name__)
        self.app_data = None
        payload = message_body.get('payload')
        if payload:
            self.app_data = payload.get('app_data', None)

        # Used for tracking/diagnostics.
        self._publisher_data = message_body.get('_publisher')
        self._enqueued_time = message_body.get('_enqueued_time', 0)
        self._dequeued_time = None
        self._start_time = None
        self._end_time = None
        self._task_latency_sec = 0

        self._dequeued()

    @classmethod
    def _get_task_id(cls, *args, **kwargs):
        """Return a unique task identifier.

        This can be overridden in sub-classes to create task ids that
        are more descriptive.

        :return: the unique identifier for a task.
        :rtype: str
        """
        return '%s_uuid_%s' % (cls.__name__, uuid.uuid1())

    @classmethod
    def publish(cls, app_data, *args, **kwargs):
        """Class method to publish a task given instance specific arguments."""
        delay_sec = kwargs.get('delay_sec')
        task_id = cls._get_task_id(*args, **kwargs)
        payload = {
            'args': args,
            'kwargs': kwargs,
            'app_data': app_data}
        pub = publisher.Publisher()
        pub.publish(cls, task_id, payload, delay_sec=delay_sec)
        return task_id

    @classmethod
    def _get_delay_sec_for_retry(cls, current_retry_num):
        """Generate a delay based on the number of times the task has failed.

        :param int current_retry_num: Task retry count for the task that is
            about to be published.
        :return: Number of seconds this task should wait before running again.
        :rtype: int
        """
        # Exponentially backoff the wait time for task attempts.
        return min(((1 << current_retry_num) * cls._retry_delay_multiple),
                   settings.SQS_MAX_TASK_DELAY_SEC)

    @classmethod
    def handle_failure(cls, message, raised_exception):
        """Logic to respond to task failure.

        :param KaleMessage message: instance of KaleMessage containing the
            task that failed.
        :param Exception raised_exception: exception that the failed task
            raised.
        :return: True if the task will be retried, False otherwise.
        :rtype: boolean
        """

        logger.warning('Task %s failed: %s.' % (message.task_id,
                                                raised_exception))

        if isinstance(raised_exception, exceptions.TimeoutException):
            message.task_inst._kill_runtime_exceeded()

        # If our exception falls into a specific list, we bail out completely
        # and do not retry.
        if isinstance(raised_exception, cls.EXCEPTION_LIST):
            cls._report_permanent_failure(
                message, raised_exception,
                PERMANENT_FAILURE_UNRECOVERABLE, True)
            return False

        # See if retries are enabled at all. If is <= 0, then just return.
        if cls.max_retries is None or cls.max_retries <= 0:
            cls._report_permanent_failure(
                message, raised_exception, PERMANENT_FAILURE_NO_RETRY, True)
            return False

        # Monitor retries and dropped tasks
        if message.task_retry_num >= cls.max_retries:
            cls._report_permanent_failure(
                message, raised_exception,
                PERMANENT_FAILURE_RETRIES_EXCEEDED, False)
            return False

        payload = {
            'args': message.task_args,
            'kwargs': message.task_kwargs,
            'app_data': message.task_app_data}
        retry_count = message.task_retry_num + 1
        delay_sec = cls._get_delay_sec_for_retry(message.task_retry_num)
        pub = publisher.Publisher()
        pub.publish(
            cls, message.task_id, payload,
            current_retry_num=retry_count, delay_sec=delay_sec)
        return True

    def run(self, *args, **kwargs):
        """Wrap the run_task method of tasks.

        We use this instead of a decorator to protect against the case where
        a subclass may not call super().

        The order of operations in a subclass should look like this:
        1) Subclass's override _pre_run logic.
        2) call super() at the end of the override.
        3) run_task() willrun.
        4) call super() at the start of _post_run override.
        5) Subclass's override _post_run logic.
        """

        self._setup_task_environment()
        self._pre_run(*args, **kwargs)

        try:
            # This raises an exception if the task should not be attempted.
            # This enables tasks to be blacklisted by ID or type.
            self._check_blacklist(*args, **kwargs)
            self.run_task(*args, **kwargs)
        except Exception as exc:
            # Record latency here.
            self._end_time = time.time()
            self._task_latency_sec = self._end_time - self._start_time

            # Cleanup the environment this task was running in right away.
            self._clean_task_environment(task_id=self.task_id,
                                         task_name=self.task_name, exc=exc)
            raise

        self._post_run(*args, **kwargs)
        self._clean_task_environment(task_id=self.task_id,
                                     task_name=self.task_name)

    def run_task(self, *args, **kwargs):
        """Run the task, this must be implemented by subclasses."""
        raise NotImplementedError()

    def _check_blacklist(self, *args, **kwargs):
        """Raises an exception if a task should not run.

        This enables a subclass to blacklist tasks by ID or type. This needs
        to be handled outside of kale since it requires a datastore.

        Ex:
        if task_id in cache.get(BLACKLISTED_IDS) or task_name in
            cache.get(BLACKLISTED_TASK_TYPES):
                raise exceptions.BlacklistedException()
        """
        return

    def _dequeued(self, *args, **kwargs):
        """Method called when a task is pulled from the queue and instantiated.

        This does not mean that this task instance  will necessarily run. It
        was most likely pulled from the queue in a batch and will still sit
        idle until its turn, if it's visibility timeout runs out before that
        point it will be released back to the queue.

        Note: We do funny things in celery to determine time spent enqueued,
        in SQS we may have alternative options here that require
        the message but no args/kwargs.
        """
        self._dequeued_time = time.time()

    @staticmethod
    def _setup_task_environment():
        """Setup the environment for this task."""
        pass

    @staticmethod
    def _clean_task_environment(task_id=None, task_name=None, exc=None):
        """Cleans the environment for this task.

        Args:
            task_id: string of task id.
            task_name: string of task name.
            exc: The exception raised by the task, None if the task succeeded.
        """
        pass

    def _pre_run(self, *args, **kwargs):
        """Called immediately prior to a task running."""
        self._start_time = time.time()

    def _post_run(self, *args, **kwargs):
        """Called immediately after a task finishes.

        This will not be called if the task fails.
        """
        self._end_time = time.time()
        self._task_latency_sec = self._end_time - self._start_time

        if self.target_runtime:
            if self._task_latency_sec >= self.target_runtime:
                self._alert_runtime_exceeded()

    def _alert_runtime_exceeded(self, *args, **kwargs):
        """Handle the case where a task exceeds its alert runtime.

        This will be called during task post_run.
        """
        pass

    def _kill_runtime_exceeded(self, *args, **kwargs):
        """Handle the case where a task is killed due to timing out.

        This will be called by the task's onfailure
        """
        pass

    @classmethod
    def _report_permanent_failure(cls, message, exception, failure_type,
                                  log_exception):
        """Handles reporting of a permanent failure.

        :param KaleMessage message: A kale.message instance that contains the
            task.
        :param Exception exception: The error info that contains the stacktrace
        :param str failure_type:  A string used for logging to denote the type
            of permanent failure.
        :param bool log_exception: A bool denoting if we should include the
                exception stacktrace.
        """
        message_str = ('PERMANENT_TASK_FAILURE: FAILURE_TYPE=%s TASK_TYPE=%s '
                       'TASK_ID=%s, TRACEBACK=%s' %
                       (failure_type, message.task_name, message.task_id,
                        exception))
        logger.error(message_str)
