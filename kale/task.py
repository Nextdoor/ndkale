"""Module containing the base class for tasks."""
from __future__ import absolute_import

import errno
import logging
import os
import multiprocessing
import signal
import sys
import time
import resource
import uuid
from future import utils

# Support pickling tracebacks from a subprocess.
from tblib import pickling_support
pickling_support.install()

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

    # Pool to run tasks in.
    _task_process_pool = None

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
        task_id = cls._get_task_id(*args, **kwargs)
        payload = {
            'args': args,
            'kwargs': kwargs,
            'app_data': app_data}
        pub = publisher.Publisher()
        pub.publish(cls, task_id, payload)
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
        if not settings.should_run_task_as_child():
            self._run_task(*args, **kwargs)
        else:
            self._run_task_as_child(*args, **kwargs)

    def _run_task(self, *args, **kwargs):
        """Runs the task in single process mode.

        This will use SIGALRM to handle timeouts.
        """
        self._record_start()
        try:
            def _handle_timeout(signum, frame):
                raise exceptions.TimeoutException(os.strerror(errno.ETIME))

            # Use sigalrm to handle timeout.
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(self.time_limit)

            self._setup_task_environment()
            self._pre_run(*args, **kwargs)

            try:
                # This raises an exception if the task should not be attempted.
                self._check_blacklist(*args, **kwargs)
                self.run_task(*args, **kwargs)
            except Exception as exc:
                # Cleanup the environment this task was running in right away.
                self._clean_task_environment(task_id=self.task_id,
                                             task_name=self.task_name, exc=exc)
                raise

            self._post_run(*args, **kwargs)
            self._clean_task_environment(task_id=self.task_id,
                                         task_name=self.task_name)
        finally:
            self._record_end()
            # Remove handler.
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            # Remove signal.
            signal.alarm(0)

    def _run_task_as_child(self, *args, **kwargs):
        """Runs the task in a subprocess.

        The subprocess runs _task_wrapper, which handles all task logic.

        Because the start and end time must be known to the worker,
        _record_start() and _record_end() are run in the context of the parent process.
        """
        pool = Task._get_task_process_pool()
        self._record_start()
        terminate = False
        try:
            process = pool.apply_async(_task_wrapper, args=(self, args, kwargs))
            exc = process.get(self.time_limit)
            if exc is not None:
                exc.reraise()
        except multiprocessing.TimeoutError:
            # Task took too long: hard-terminate the subprocess.
            terminate = True
            # Raise as an exception class that's expected in worker._handle_failures.
            raise exceptions.TimeoutException(os.strerror(errno.ETIME))
        finally:
            self._record_end()
            # Check the worker's memory usage.
            is_memory_ok = pool.apply_async(_is_memory_ok).get()
            if terminate or not is_memory_ok:
                Task._terminate_task_process_pool()

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

    def _record_start(self):
        """Records that the task has started."""
        self._start_time = time.time()

    def _record_end(self):
        """Records that the task has ended."""
        self._end_time = time.time()
        self._task_latency_sec = self._end_time - self._start_time

        if self.target_runtime:
            if self._task_latency_sec >= self.target_runtime:
                self._alert_runtime_exceeded()

    def _pre_run(self, *args, **kwargs):
        """Called immediately prior to a task running."""
        pass

    def _post_run(self, *args, **kwargs):
        """Called immediately after a task finishes.

        This will not be called if the task fails.
        """
        pass

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

    @classmethod
    def _get_task_process_pool(cls):
        if Task._task_process_pool is None:
            Task._task_process_pool = multiprocessing.Pool(1)
        return Task._task_process_pool

    @classmethod
    def _terminate_task_process_pool(cls):
        if Task._task_process_pool is not None:
            Task._task_process_pool.terminate()
        Task._task_process_pool = None


class RemoteException(object):
    """Utility class for reraising an exception from a remote process.

    Keeps the original stack trace.
    """
    def __init__(self):
        self.exc_info = sys.exc_info()

    def reraise(self):
        utils.raise_(*self.exc_info)


def _task_wrapper(task_inst, args, kwargs):
    """Task wrapper function.

    In single-process mode, this will return any exception raised.

    In multi-process mode, this will return a RemoteException for any exceptions raised.
    """
    task_inst._setup_task_environment()
    task_inst._pre_run(*args, **kwargs)

    # Remove SIGTERM signal handler; the parent process handles all the cleanup.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    try:
        # This raises an exception if the task should not be attempted.
        task_inst._check_blacklist(*args, **kwargs)
        task_inst.run_task(*args, **kwargs)
    except Exception as exc:
        # Cleanup the environment this task was running in right away.
        task_inst._clean_task_environment(task_id=task_inst.task_id,
                                          task_name=task_inst.task_name, exc=exc)
        return RemoteException()

    task_inst._post_run(*args, **kwargs)
    task_inst._clean_task_environment(task_id=task_inst.task_id,
                                      task_name=task_inst.task_name)


def _is_memory_ok():
    resource_data = resource.getrusage(resource.RUSAGE_SELF)
    if resource_data.ru_maxrss < settings.DIE_ON_RESIDENT_SET_SIZE_MB * 1024:
        return True

    logger.info('Worker memory usages exceeds max: %d. Restarting worker.' %
                resource_data.ru_maxrss)
    return False
