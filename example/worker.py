from __future__ import absolute_import
from __future__ import print_function

from kale import worker


if __name__ == '__main__':
    print('Task worker is running ...')
    worker.Worker().run()
