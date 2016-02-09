from __future__ import absolute_import
from __future__ import print_function

import logging

from kale import task

logger = logging.getLogger(__name__)


class FibonacciTask(task.Task):

    # How many times should taskworker retry if it fails.
    # If this task shouldn't be retried, set it to None
    max_retries = 3

    # The hard limit for max task running time.
    # This value should be set between max actual running time and
    # queue visibility timeout.
    time_limit = 5  # seconds

    # The queue name
    queue = 'default'

    @staticmethod
    def fibonacci(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return FibonacciTask.fibonacci(n - 1) + FibonacciTask.fibonacci(n - 2)

    def run_task(self, n, *args, **kwargs):
        print('fibonacci(%d) = %d' % (n, self.fibonacci(n)))
