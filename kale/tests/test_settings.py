"""Module for kale settings for unit tests."""
from __future__ import absolute_import

import os

QUEUE_CONFIG = os.path.join(os.path.split(os.path.abspath(__file__))[0],
                            'test_queue_config.yaml')
QUEUE_CLASS = 'kale.test_utils.TestQueueClass'
QUEUE_SELECTOR = 'kale.test_utils.TestQueueSelector'
