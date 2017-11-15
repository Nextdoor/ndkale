"""Module containing queue selection algorithms.


How to implement your own queue selection algorithm?

    class MyQueueSelector(SelectQueueBase):

        def get_queue(self):

            # Get a list of all queues defined in the YAML file that is
            # specified at QUEUE_CONFIG in settings file.
            #
            # You may use these two properties of a queue object to select
            # a queue:
            #
            # - name: string of queue name
            # - priority: integer of queue priority; larger value,
            #       higher priority
            queues = self.queue_info.get_queues()

            # Implement your algorithm here
            # ...

            # Eventually, return one of queue object from queues
            return queue
"""
from __future__ import absolute_import

import random

from six.moves import range


class SelectQueueBase(object):
    """Base class for selecting a queue.

    The only method that needs to be implemented:

    get_queue: it's called for each task processing cycle on task worker.
    """

    def __init__(self, queue_info):
        self.queue_info = queue_info

    def get_queue(self, *args, **kwargs):
        """Returns a TaskQueue object."""
        raise NotImplementedError('Base class cannot be used directly.')


class Random(SelectQueueBase):
    """Randomly selects a queue without considering priority."""

    def get_queue(self):
        queues = self.queue_info.get_queues()
        return random.choice(queues)


class Lottery(SelectQueueBase):
    """Use lottery scheduling algorithm to select a queue based on priority."""

    @staticmethod
    def _run_lottery(queues):
        """Draw lottery from a list of candidate queues.

        :param list[TaskQueue] queues: a list of candidate queues.

        :return: A TaskQueue object that wins lottery. If it fails (e.g.,
            invalid priority of queues), it returns None.
        :rtype: TaskQueue
        """
        tickets = {}
        total_tickets = 0
        for queue in queues:
            # Queue priority should be within 1 to 100.
            if queue.priority < 1 or queue.priority > 100:
                continue
            priority = queue.priority
            low = total_tickets
            total_tickets += priority
            high = total_tickets
            tickets[queue.name] = (low, high)

        # [0, total_tickets)
        try:
            number = random.randrange(0, total_tickets)
            for queue in queues:
                if number >= tickets[
                        queue.name][0] and number < tickets[queue.name][1]:
                    return queue
        except ValueError:
            return None

        # Something wrong happens
        return None

    def get_queue(self, *args, **kwargs):
        return self._run_lottery(self.queue_info.get_queues())


class HighestPriorityFirst(SelectQueueBase):
    """Highest priority first.

    Get the highest priority non-empty queue first.
    If all queues are empty, get the highest priority empty queue.
    """

    def get_queue(self, *args, **kwargs):
        queue = self.queue_info.get_highest_priority_queue_that_needs_work()
        if queue:
            return queue
        queues = self.queue_info.get_queues()
        queues.sort(key=lambda x: x.priority, reverse=True)
        return queues[0]


class HighestPriorityLottery(Lottery):
    """Highest priority first  + lottery.

    Get highest priority non-empty queue first.
    If all queues are empty, run lottery on empty queues.
    """

    def get_queue(self, *args, **kwargs):
        queue = self.queue_info.get_highest_priority_queue_that_needs_work()
        if queue:
            return queue

        return self._run_lottery(self.queue_info.get_queues())


class LotteryLottery(Lottery):
    """Run lottery on both non-empty and empty queues.

    Run lottery on all queues. When we get an non-empty queue, return
    immediately. If we get 10 empty queues in a row, run lottery again,
    and long poll on whatever queue we get.
    """

    def get_queue(self, *args, **kwargs):
        retry_empty_queue_count = 10

        for i in range(retry_empty_queue_count):
            queue = self._run_lottery(self.queue_info.get_queues())
            if self.queue_info.does_queue_need_work(queue):
                return queue
        return self._run_lottery(self.queue_info.get_queues())


class ReducedLottery(Lottery):
    """Improved lottery scheduling.

    Limiting the lottery pool by removing known empty queues. When we get an
    non-empty queue, return immediately. If we get an empty queue, we'll remove
    this empty queue out of the lottery pool and rerun lottery again. If all
    queues are empty, run lottery on all queues, and long poll on whatever
    queue we get.
    """

    def get_queue(self, *args, **kwargs):
        # Make a new copy of list, so no side effect on queue_info.queues
        candidate_queues = self.queue_info.get_queues()[:]

        while len(candidate_queues) > 0:
            queue = self._run_lottery(candidate_queues)
            if self.queue_info.does_queue_need_work(queue):
                return queue
            else:
                candidate_queues.remove(queue)
        return self._run_lottery(self.queue_info.get_queues())
