"""Benchmarks queue_info implementations."""

import gflags
import logging
import Queue
import os
import sys
import threading
import time

# Set this environment variable before importing kale module
os.environ['KALE_SETTINGS_MODULE'] = 'benchmark_settings'

from kale import queue_info
from kale import sqs

gflags.DEFINE_string('config_file', 'sample_queue_config.yaml',
                     'The tasks load file path.')
gflags.DEFINE_integer('workers', 5 * 8, 'Number of task workers.')
gflags.DEFINE_integer('iterations', 5,
                      'Number of iterations for a worker to check sqs queues.')
FLAGS = gflags.FLAGS

logging.basicConfig(level='INFO')
log = logging.getLogger('kale.benchmark')

checking_sqs_time = Queue.Queue()


class WorkerThread(threading.Thread):
    """Consuming tasks."""

    def __init__(self, queue_info_obj, iterations):
        """
        Args:
            speedup: Integer for how much faster we want to simulate tasks.
                If speedup is 10, then we process tasks at 10x slower speed.
            select_queue_class: String for select_queue class in the
                nd.kale.select_queue module.
        """
        super(WorkerThread, self).__init__()
        self.queue_info_obj = queue_info_obj
        self.iterations = iterations

    def run(self):
        for i in range(self.iterations):
            start_time = time.time()
            self.queue_info_obj.get_highest_priority_non_empty_queue()
            end_time = time.time()
            checking_sqs_time.put(end_time - start_time)


class Benchmark(object):
    """Manages entire benchmark."""

    def __init__(self, config_file, workers, iterations):
        self.config_file = config_file
        self.workers = workers
        self.iterations = iterations

    def run(self):
        """Main function of benchmarking."""
        log.info('Start benchmarking ...')
        log.info('Spawning %d worker threads ...' % self.workers)
        sqs_talk = sqs.SQSTalk()
        queue_info_obj = queue_info.QueueInfo(self.config_file, sqs_talk)
        worker_threads = []
        for i in range(self.workers):
            worker_thread = WorkerThread(queue_info_obj, self.iterations)
            worker_threads.append(worker_thread)
            worker_thread.setDaemon(True)
        for worker_thread in worker_threads:
            worker_thread.start()
        for worker_thread in worker_threads:
            worker_thread.join()

        all_check_time = []
        while not checking_sqs_time.empty():
            check_time = checking_sqs_time.get()
            all_check_time.append(check_time)
        log.info('=== Benchmark results ===')
        count = len(all_check_time)
        sum_time = sum(all_check_time)
        avg_time = sum_time / count
        median_time = sorted(all_check_time)[count / 2]
        max_time = max(all_check_time)
        log.info('Average Check Time: %f' % avg_time)
        log.info('Median Check time: %f' % median_time)
        log.info('Max Check Time: %f' % max_time)


def main():
    """Main function for this script."""
    try:
        FLAGS(sys.argv)
    except gflags.FlagsError as e:
        log.error('%s\\nUsage: %s ARGS\\n%s' % (e, sys.argv[0], FLAGS))
        sys.exit(1)
    benchmark = Benchmark(FLAGS.config_file, FLAGS.workers, FLAGS.iterations)
    benchmark.run()

if __name__ == '__main__':
    main()
