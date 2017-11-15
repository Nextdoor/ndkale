"""Tests queue_selector.py"""
from __future__ import absolute_import

import unittest

from kale import queue_info
from kale import queue_selector


class MultiQueueInfo(queue_info.QueueInfoBase):
    def get_queues(self):
        return [queue_info.TaskQueue(name='queue1', priority=100),
                queue_info.TaskQueue(name='queue2', priority=50),
                queue_info.TaskQueue(name='queue3', priority=1)]

    def does_queue_need_work(self, queue):
        return not self.is_queue_empty(queue)

    def is_queue_empty(self, queue):
        if queue.name == 'queue2':
            return False
        return True

    def get_highest_priority_queue_that_needs_work(self):
        return self.get_queues()[0]


class SingleQueueInfo(queue_info.QueueInfoBase):
    def get_queues(self):
        return [queue_info.TaskQueue(name='queue1', priority=99)]


class NoQueueInfo(queue_info.QueueInfoBase):
    def get_queues(self):
        return []


class MultiQueueNoPriorityInfo(MultiQueueInfo):
    def get_highest_priority_queue_that_needs_work(self):
        return None


class BadQueueInfo(queue_info.QueueInfoBase):
    def get_queues(self):
        return [queue_info.TaskQueue(name='queue1', priority=101),
                queue_info.TaskQueue(name='queue2', priority=0)]


class SelectQueueBaseTest(unittest.TestCase):
    """Tests for SelectQueueBase class."""

    def get_queue_test(self):
        # Get any one queue from multiple queues
        selector = queue_selector.SelectQueueBase(MultiQueueInfo())
        with self.assertRaises(NotImplementedError):
            selector.get_queue()


class RandomQueueTest(unittest.TestCase):
    """Tests for Random class."""

    def get_queue_test(self):
        # Get any one queue from multiple queues
        queue = queue_selector.Random(MultiQueueInfo()).get_queue()
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])


class HighestPriorityFirstTest(unittest.TestCase):
    """Tests for HighestPriorityFirst class."""

    def get_queue_test(self):
        # Get any one queue from multiple queues
        queue = queue_selector.HighestPriorityFirst(
            MultiQueueInfo()).get_queue()
        self.assertEqual(queue.name, 'queue1')

    def get_queue_test_no_priority(self):
        # Get any one queue from multiple queues
        queue = queue_selector.HighestPriorityFirst(
            MultiQueueNoPriorityInfo()).get_queue()
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])


class LotteryTest(unittest.TestCase):
    """Tests for Lottery class."""

    def run_lottery_test(self):
        queue = queue_selector.Lottery._run_lottery(
            MultiQueueInfo().get_queues())
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])

        queue = queue_selector.Lottery._run_lottery(
            SingleQueueInfo().get_queues())
        self.assertEqual(queue.name, 'queue1')

        queue = queue_selector.Lottery._run_lottery(NoQueueInfo().get_queues())
        self.assertIsNone(queue)

        queue = queue_selector.Lottery._run_lottery(
            BadQueueInfo().get_queues())
        self.assertIsNone(queue)

    def get_queue_test(self):
        # Get any one queue from multiple queues
        selector = queue_selector.Lottery(MultiQueueInfo())
        queue = selector.get_queue()
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])


class ReducedLotteryTest(unittest.TestCase):
    """Tests for ReducedLottery class."""

    def get_queue_test(self):
        selector = queue_selector.ReducedLottery(MultiQueueInfo())
        queue = selector.get_queue()
        self.assertEqual(queue.name, 'queue2')


class HighestPriorityLotteryTest(unittest.TestCase):
    """Tests for HighestPriorityLottery class."""

    def run_lottery_test(self):
        queue = queue_selector.HighestPriorityLottery._run_lottery(
            MultiQueueInfo().get_queues())
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])

        queue = queue_selector.HighestPriorityLottery._run_lottery(
            SingleQueueInfo().get_queues())
        self.assertEqual(queue.name, 'queue1')

        queue = queue_selector.HighestPriorityLottery._run_lottery(
            NoQueueInfo().get_queues())
        self.assertIsNone(queue)

        queue = queue_selector.HighestPriorityLottery._run_lottery(
            BadQueueInfo().get_queues())
        self.assertIsNone(queue)

    def get_queue_test(self):
        # Get any one queue from multiple queues
        selector = queue_selector.HighestPriorityLottery(MultiQueueInfo())
        queue = selector.get_queue()
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])


class LotteryLotteryTest(unittest.TestCase):
    """Tests for LotteryLottery class."""

    def run_lottery_test(self):
        queue = queue_selector.LotteryLottery._run_lottery(
            MultiQueueInfo().get_queues())
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])

        queue = queue_selector.LotteryLottery._run_lottery(
            SingleQueueInfo().get_queues())
        self.assertEqual(queue.name, 'queue1')

        queue = queue_selector.LotteryLottery._run_lottery(
            NoQueueInfo().get_queues())
        self.assertIsNone(queue)

        queue = queue_selector.LotteryLottery._run_lottery(
            BadQueueInfo().get_queues())
        self.assertIsNone(queue)

    def get_queue_test(self):
        # Get any one queue from multiple queues
        selector = queue_selector.LotteryLottery(MultiQueueInfo())
        queue = selector.get_queue()
        self.assertTrue(queue.name in ['queue1', 'queue2', 'queue3'])
