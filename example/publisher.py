import gflags
import os
import sys

from taskworker import tasks


gflags.DEFINE_integer(
    'n', 10, 'The input of fibonacci task. Default: 10')

FLAGS = gflags.FLAGS


if __name__ == '__main__':
    FLAGS(sys.argv)
    tasks.FibonacciTask.publish(FLAGS.n)
    print 'A FibonacciTask is scheduled to run. With input %d.' % FLAGS.n
