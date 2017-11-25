"""Get information of queues.

We can have different ways of getting queue information, e.g., from Zookeeper,
 from hardcoded data, from config file, ...

Thus, we need to extend the base class QueueInfoBase for different
implementations and use the right implementation in different cases.
"""
from __future__ import absolute_import

import six
import yaml
from boto import exception as boto_exception


class TaskQueue(object):
    """Represents a task queue. Always created via QueueInfo.


    :param str name: string for queue name.
    :param int priority: integer for queue priority.
    :param int num_iterations: integer for number of iterations to process
        tasks for a select_queue() call. Number of iterations to process
        tasks for a select_queue() call. Two reasons:
            1. Calling select_queue() has overhead, so we want to reuse
               the chosen queue for several rounds of tasks processing.
            2. SQS has limit of 10 tasks per batch. Using multiple
               iterations here simulates a larger batch, e.g., 20.
    :param int long_poll_time_sec: integer for number of seconds waiting for
        long poll. For SQS, if a queue is empty when task worker wants to
        fetch tasks from it, the task worker can wait for quite a while,
        hoping tasks would appear in the queue. This is to avoid costly
        connection reestablishment.
    :param int batch_size: integer for number of tasks to fetch in a batch.
    :param int visibility_timeout_sec: integer for the max time for a task that
            are invisible in an SQS queue. How to decide this property?
            - T: Average running time of a task.
            - N: Number of tasks in a batch that are fetched at once by
                 the worker.
            - F: Just to give us more headroom. E.g., 1.2
            => visibility_timeout = F * (T * N)
    """

    def __init__(self, name='default', priority=5, num_iterations=2,
                 long_poll_time_sec=5, batch_size=5,
                 visibility_timeout_sec=600):
        self.simple_name = name
        (self.name, self.dlq_name) = self._decorate_name(name)
        self.priority = priority
        self.num_iterations = num_iterations
        self.batch_size = batch_size
        self.long_poll_time_sec = long_poll_time_sec
        self.visibility_timeout_sec = visibility_timeout_sec

    def _decorate_name(self, name):
        """Decorate queue name.

        For example, we want to prefix RELEASE version on each queue name.
        The default implementation is to do nothing.

        :param str name: string for queue name.
        :return: A 2-tuple (queue name, dead letter queue name).
        :rtype: tuple
        """
        return (name, 'dlq-' + name)


class QueueInfoBase(object):
    """Base class to represent Queue information.

    Any concrete class should implement get_queues method.
    """

    def get_queues(self):
        """Returns a list of TaskQueue objects."""
        raise NotImplementedError('Base class cannot be used directly.')

    def get_highest_priority_queue_that_needs_work(self):
        """Returns the highest-priority non-empty queue."""
        raise NotImplementedError('Base class cannot be used directly.')

    def is_queue_empty(self, queue):
        """Check if a queue is empty.

        :param TaskQueue queue: A TaskQueue object.
        :return: True if the queue is empty; otherwise, queue is non-empty.
        :rtype: bool
        """
        raise NotImplementedError('Base class cannot be used directly.')

    def does_queue_need_work(self, queue):
        """Checks if a queue should be worked on.

        :param TaskQueue queue: a TaskQueue object.
        :return: True if the queue needs work; False otherwise.
        :rtype: bool
        """
        raise NotImplementedError('Base class cannot be used directly.')


class QueueInfo(QueueInfoBase):
    """Provides task queue info."""

    _queues = None
    _simple_name_queues_map = None

    def __init__(self, config_file, sqs_talk, queue_cls=TaskQueue):
        """Instantiate new QueueInfo object.

        :param str config_file: String of config file path.
        :param SQSTalk sqs_talk: An SQSTalk object.
        :param TaskQueue queue_cls: Class (or subclass) of TaskQueue.
        """

        # Initialize singleton if needed
        if not QueueInfo._queues:
            QueueInfo._queues = self._get_queues_from_config(config_file,
                                                             queue_cls)

        if not QueueInfo._simple_name_queues_map:
            QueueInfo._simple_name_queues_map = {}
            for queue in QueueInfo._queues:
                QueueInfo._simple_name_queues_map[queue.simple_name] = queue

        self._sqs_talk = sqs_talk

    def get_queue(self, simple_name):
        """Get queue object by simple name.

        :param str simple_name: string of queue simple name.
        :return: a TaskQueue object.
        :rtype: TaskQueue
        """
        return self._simple_name_queues_map[simple_name]

    def get_queues(self):
        """Returns a list of TaskQueue objs sorted by priority (highest first).
        """
        return self._queues

    def get_highest_priority_queue_that_needs_work(self):
        """Returns the highest-priority queue that needs work.

        If all queues are empty, then None is returned.
        """
        for queue in self._queues:
            if self.does_queue_need_work(queue):
                return queue
        return None

    def is_queue_empty(self, queue):
        """Check if a queue is empty.

        :param TaskQueue queue: a TaskQueue object.
        :return: True if the queue is empty; otherwise, queue is non-empty.
        :rtype: bool
        """
        sqs_queue = self._sqs_talk._get_or_create_queue(queue.name)
        if sqs_queue.count() > 0:
            return False
        return True

    def does_queue_need_work(self, queue):
        """Checks if a queue should be worked on.

        This basically checks whether the queue is empty. However,
        if we hit a SQS rate limit, this will assume the queue needs work.

        :param TaskQueue queue: a TaskQueue object.
        :return: True if the queue needs work; False otherwise.
        :rtype: bool
        """
        try:
            return not self.is_queue_empty(queue)
        except boto_exception.SQSError as e:
            if e.code == 'RequestThrottled':
                return True
            raise e

    @classmethod
    def _get_queues_from_config(cls, config_file, queue_cls):
        """Parses config file and returns queues.

        :param str config_file: String for the path of yaml config file for
            queues.
        :param TaskQueue queue_cls: Class (or subclass) of TaskQueue.
        :return: A list of TaskQueue queue objects sorted by priority.
            Highest priority first.
        :rtype: list[TaskQueue]
        """
        with open(config_file, 'r') as fp:
            queues_from_config = yaml.load(fp)
            queues = []
            for queue_name, queue in six.iteritems(queues_from_config):
                q = queue_cls(name=queue['name'], priority=queue['priority'],
                              num_iterations=queue['num_iterations'],
                              long_poll_time_sec=queue['long_poll_time_sec'],
                              batch_size=queue['batch_size'],
                              visibility_timeout_sec=queue[
                                  'visibility_timeout_sec'])
                queues.append(q)
            # Sort by priority, highest priority first.
            queues.sort(key=lambda x: x.priority, reverse=True)
            return queues
