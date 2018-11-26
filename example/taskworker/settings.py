from __future__ import absolute_import

import os

#
# Production settings
#

# MESSAGE_QUEUE_USE_PROXY = False
# AWS_ACCESS_KEY_ID = 'KEY_ID'
# AWS_SECRET_ACCESS_KEY = 'ACCESS_KEY'
# AWS_REGION = 'us-west-2'

#
# Development settings
#

# Using elasticmq to emulate SQS locally
MESSAGE_QUEUE_USE_PROXY = True
MESSAGE_QUEUE_PROXY_PORT = 9324
MESSAGE_QUEUE_PROXY_HOST = os.getenv('MESSAGE_QUEUE_PROXY_HOST', '0.0.0.0')
AWS_ACCESS_KEY_ID = 'x'
AWS_SECRET_ACCESS_KEY = 'x'

# Queue config file path
QUEUE_CONFIG = 'taskworker/queue_config.yaml'

# SQS limits per message size, bytes
# It can be set anywhere from 1024 bytes (1KB), up to 262144 bytes (256KB).
# See http://aws.amazon.com/sqs/faqs/
SQS_TASK_SIZE_LIMIT = 256000

# The class for queue selction algorithm
QUEUE_SELECTOR = 'kale.queue_selector.ReducedLottery'
