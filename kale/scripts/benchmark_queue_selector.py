"""Benchmarks queue_selector implementations.

queue_selector is used to decide which queue a task worker should process
in a task processing cycle.

This provides a framework to simulate production task logs and to evaluate
different queue_selector implementation.

How it works
------------
It spawns N threads to simulate N tasks workers, and 1 thread to simulate
a task publisher.

Tasks load
----------
It needs a csv file with the following format to simulate tasks load:

QUEUE_NAME,running_time_in_seconds
QUEUE_NAME,running_time_in_seconds
...

For example:

default,0.24
default,0.02
highp,2.46
highp,0.11
...

The path of the tasks load file is specified via --tasks_load_file argument.

Example
-------
python benchmark_queue_selector.py --tasks_load_file ~/sample_logs \
     --speedup 1 \
     --publish_interval 0.01 \
     --queue_selector_class HighestPriorityLottery

python benchmark_queue_selector.py --tasks_load_file ~/sample_logs \
     --speedup 1 \
     --publish_interval 0.01 \
     --queue_selector_class Lottery

python benchmark_queue_selector.py --tasks_load_file ~/sample_logs \
     --speedup 1 \
     --publish_interval 0.01 \
     --queue_selector_class Random
"""
from __future__ import absolute_import

import argparse
import csv
import logging
import os
import threading
import time

from six.moves import range
import six
import six.moves.queue

from kale import queue_info
from kale import queue_selector


parser = argparse.ArgumentParser()
parser.add_argument('queue_selector_class', type=str, default='Random',
                    help='The class for implementing queue_selector.')
parser.add_argument('tasks_load_file', type=str, default='',
                    help='The tasks load file path.')
parser.add_argument('workers', type=int, default=5 * 8,
                    help='Number of task workers.')
parser.add_argument('speedup', type=int, default=1,
                    help='Speedup task processing by [speedup] times.')
parser.add_argument('publish_interval', type=float, default=0.01,
                    help='Interval (seconds) between two task publishings.')


logging.basicConfig(level='INFO')
log = logging.getLogger('kale.benchmark')

PRINT_STATS_INTERVAL = 2  # seconds

all_done = False


class StaticTaskQueue(queue_info.TaskQueue):
    """The queue class used to assist benchmarking."""
    default_priority = 5

    def __init__(self, name='default', priority=5, num_iterations=2,
                 long_poll_time_sec=1, batch_size=5,
                 visibility_timeout_sec=600, default_priority=5):
        super(StaticTaskQueue, self).__init__(
            name=name, priority=priority,
            num_iterations=num_iterations,
            long_poll_time_sec=long_poll_time_sec,
            batch_size=batch_size,
            visibility_timeout_sec=visibility_timeout_sec)

        # We want to experiment dynamic priorities here, so should have a
        # variable to keep track of the initial priority
        self.default_priority = default_priority

        # What tasks are left in this queue for processing
        self.tasks = six.moves.queue.Queue(maxsize=0)

        # What tasks are finished
        self.finished_tasks = six.moves.queue.Queue(maxsize=0)

        # How many times we need to wait for long polling.
        # That is, how often we hit an empty queue.
        self.long_polling_count = 0


class StaticQueueInfo(queue_info.QueueInfoBase):
    """Hardcoded queue information."""

    def __init__(self):
        self.queues = {
            'default': StaticTaskQueue(name='default', priority=75,
                                       default_priority=75),
            'large': StaticTaskQueue(name='large', priority=75,
                                     default_priority=75),
            'highp': StaticTaskQueue(name='highp', priority=100,
                                     default_priority=100),
            'lowp': StaticTaskQueue(name='lowp', priority=1,
                                    default_priority=1),
            'digest': StaticTaskQueue(name='digest', priority=70,
                                      default_priority=70)}

    def get_queues(self):
        """Returns all queues."""
        return list(self.queues.values())

    def is_empty(self):
        """Are all queues empty?"""
        for (queue_name, queue) in six.iteritems(self.queues):
            if not self.is_queue_empty(queue):
                return False
        return True

    def get_highest_priority_queue_that_needs_work(self):
        """Returns a list of non-empty queues."""
        non_empty_queues = []
        for (queue_name, queue) in six.iteritems(self.queues):
            if not self.is_queue_empty(queue):
                non_empty_queues.append(self.queues[queue_name])
        if len(non_empty_queues) == 0:
            return None
        non_empty_queues.sort(key=lambda x: x.priority, reverse=True)
        return non_empty_queues[0]

    def is_queue_empty(self, queue):
        """Check if a queue is empty."""
        the_queue = self.queues[queue.name]
        if not the_queue.tasks.empty():
            return False
        return True

    def does_queue_need_work(self, queue):
        """Checks if a queue should be worked on."""
        return not self.is_queue_empty(queue)


class WorkerThread(threading.Thread):
    """Consuming tasks."""

    def __init__(self, speedup, queue_selector_class, queue_info):
        """
        Args:
            speedup: Integer for how much faster we want to simulate tasks.
                If speedup is 10, then we process tasks at 10x slower speed.
            queue_selector_class: String for queue_selector class in the
                nd.kale.queue_selector module.
        """
        super(WorkerThread, self).__init__()
        SelectQueueClass = getattr(queue_selector, queue_selector_class)
        self.queue_info = queue_info
        self.queue_selector = SelectQueueClass(self.queue_info)
        self.speedup = speedup

    def run(self):
        """The main class for emulating a task worker."""
        log.info('Running worker thread %d ...' % self.ident)

        while not all_done:
            queue = self.queue_selector.get_queue()
            try:
                task_entry = self.queue_info.queues[queue.name].tasks.get(
                    block=True, timeout=queue.long_poll_time_sec)
                task_entry['queue'] = queue.name
                task_running_time = task_entry['running_time']
                task_entry['start_consumption_time'] = time.time()
                time.sleep(float(task_running_time) / self.speedup)
                self.queue_info.queues[queue.name].finished_tasks.put(
                    task_entry)
            except six.moves.queue.Empty:
                # We want to keep track of long polling occurrences, which is
                # a waste of compute resource.
                self.queue_info.queues[queue.name].long_polling_count += 1


class PublisherThread(threading.Thread):
    """Publishing tasks."""

    def __init__(self, tasks, publish_interval, queue_info):
        """
        Args:
            tasks: A list of tasks, each is a 2-tuple
                (queue_name, running_time).
            publish_interval: Integer for task publishing interval in secs.
        """
        self.tasks = tasks
        self.queue_info = queue_info
        self.publish_interval = publish_interval
        super(PublisherThread, self).__init__()

    def run(self):
        """The main function for publisher."""
        global all_done
        log.info('Running publisher thread %d ...' % self.ident)
        while len(self.tasks) > 0:
            task = self.tasks.pop()
            task_entry = {
                'publish_time': time.time(),
                'finished_consumption_time': 0.0,
                'running_time': task[1]}
            self.queue_info.queues[task[0]].tasks.put(task_entry)
            time.sleep(self.publish_interval)
        log.info('Finished publishing tasks.')

        # Use global variale to signal all workers we finish task publishing.
        all_done = True


class PrintStatsThread(threading.Thread):
    """Print queue stats."""

    def __init__(self, tasks, queue_info):
        self.tasks = tasks
        self.total_num_tasks = len(tasks)
        self.queue_info = queue_info
        super(PrintStatsThread, self).__init__()

    def run(self):
        """Main function for printing stats and benchmark results."""
        while not all_done:
            time.sleep(PRINT_STATS_INTERVAL)
            self._print_queue_stats()
        self._print_benchmark_results()

    def _print_queue_stats(self):
        """Print out queue stats."""
        string = ''
        for queue_name in six.iterkeys(self.queue_info.queues):
            string += '%s=%d; ' % (
                queue_name, self.queue_info.queues[queue_name].tasks.qsize())
        log.info(string)

    def _print_benchmark_results(self):
        """Print final benchmark results.

        Metrics we care about:
        1. Task-in-queue time for each queue.
        2. Number of tasks processed within limited time.
        """
        log.info('=== Benchmark Results ===')
        total_processed_tasks = 0
        finished_count_breakdown = {}
        for queue_name in six.iterkeys(self.queue_info.queues):
            finished_tasks = self.queue_info.queues[queue_name].finished_tasks
            all_queue_latencies = []
            while not finished_tasks.empty():
                task_entry = finished_tasks.get()
                all_queue_latencies.append(
                    task_entry[
                        'start_consumption_time'] - task_entry['publish_time'])
                total_processed_tasks += 1
                if task_entry['queue'] not in finished_count_breakdown:
                    finished_count_breakdown[task_entry['queue']] = 1
                else:
                    finished_count_breakdown[task_entry['queue']] += 1

            if len(all_queue_latencies) > 0:
                total_latency = sum(all_queue_latencies)
                num_tasks = len(all_queue_latencies)
                max_latency = max(all_queue_latencies)
                log.info(('Queue %s: average latency %f secs, median '
                          'latency %f secs, max latency %f secs, '
                          'long_polling count %d') % (
                    queue_name,
                    total_latency / num_tasks,
                    sorted(all_queue_latencies)[num_tasks / 2],
                    max_latency,
                    self.queue_info.queues[queue_name].long_polling_count))

        log.info('Total processed tasks: %d / %d' % (
            total_processed_tasks, self.total_num_tasks))
        for queue_name in six.iterkeys(finished_count_breakdown):
            log.info('Queue %s: %d tasks finished.' % (
                queue_name, finished_count_breakdown[queue_name]))


class Benchmark(object):
    """Manages benchmarking and emulates tasks workers."""

    def __init__(self, workers, speedup, tasks_load_file,
                 queue_selector_class, publish_interval):
        log.info('To terminate this process:')
        log.info('\tkill -9 %d' % os.getpid())
        log.info(('workers=[%d], speedup=[%d], tasks_load_file=[%s], '
                  'queue_selector_class=[%s]') % (
            workers, speedup, tasks_load_file, queue_selector_class))
        self.workers = workers
        self.speedup = speedup
        self.tasks_load_file = tasks_load_file
        self.queue_selector_class = queue_selector_class
        self.publish_interval = publish_interval

    def run(self):
        """Main function for doing benchmark.

        Three types of threads are running concurrently:
        1. A publisher thread: publishing tasks periodically.
        2. Multiple Worker threads: consuming tasks.
        3. A print-stats thread: print out queue stats and benchmark results.
        """
        tasks = self._load_tasks()

        queue_info = StaticQueueInfo()
        # Start a publisher thread
        publisher_thread = PublisherThread(tasks, self.publish_interval,
                                           queue_info)
        publisher_thread.setDaemon(True)
        publisher_thread.start()

        # Start a print-stats thread
        print_stats_thread = PrintStatsThread(tasks, queue_info)
        print_stats_thread.setDaemon(True)
        print_stats_thread.start()

        # Start worker threads
        worker_threads = []
        for i in range(self.workers):
            worker_thread = WorkerThread(
                self.speedup, self.queue_selector_class, queue_info)
            worker_threads.append(worker_thread)
            worker_thread.setDaemon(True)
        for worker_thread in worker_threads:
            worker_thread.start()

        # Wait for all threads
        for worker_thread in worker_threads:
            worker_thread.join()
        publisher_thread.join()
        print_stats_thread.join()

    def _load_tasks(self):
        """Load tasks from tasks_load_file."""
        tasks = []
        with open(self.tasks_load_file, 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                try:
                    queue_name = row[0].strip()
                    running_time = row[1].strip()
                    if queue_name and running_time:
                        tasks.append((queue_name, running_time))
                except IndexError:
                    continue
        return tasks


def main():
    """Main function for this script."""
    args = parser.parse_args()

    benchmark = Benchmark(args.workers, args.speedup, args.tasks_load_file,
                          args.queue_selector_class, args.publish_interval)
    benchmark.run()


if __name__ == '__main__':
    main()
