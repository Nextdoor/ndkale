from __future__ import absolute_import
from __future__ import print_function

import argparse

from taskworker import tasks


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, default=10,
                        help='The input of fibonacci task. Default: 10')
    args = parser.parse_args()

    tasks.FibonacciTask.publish(None, args.n)
    print('A FibonacciTask is scheduled to run. With input %d.' % args.n)
