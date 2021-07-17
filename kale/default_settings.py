"""Default settings for kale."""
from __future__ import absolute_import

import os
import pickle
import platform
import time
import zlib

import six

# The default settings are inadequate for actual use
# due to the need for a queue. When the settings are
# overridden this will be set to True.
# Note: You are not supposed to overwrite this property.
PROPERLY_CONFIGURED = False

# Optional functions to be called when the worker is
# started and stopped.
ON_WORKER_STARTUP = lambda: None
ON_WORKER_SHUTDOWN = lambda: None

# Note: This makes it possible to move modules and remain
# backwards compatible.
TASK_MAPPER = {}

# The function to (de)compress a message string, which takes a string as input.
COMPRESSOR = zlib.compress
DECOMPRESSOR = zlib.decompress

# Set to a specific endpoint url to change how the Boto3 client will do the connection.
# For example, use http://{ELASTICMQ_HOST}:{ELASTICMQ_PORT} to use ElasticMQ in Dev.
# Or, use these AWS docs to pass a VPC-compatible endpoint url to send SQS traffic through a VPC:
# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-sending-messages-from-vpc.html
MESSAGE_QUEUE_ENDPOINT_URL = ''

# AWS credential for connecting to SQS
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = ''

# Use max size (in bytes) as of March 2014 as the default.
SQS_TASK_SIZE_LIMIT = 256000

SQS_QUEUE_TAG_FUNCTION = lambda queue_name: {}

RETRY_DELAY_MULTIPLE_SEC = 60
SQS_MAX_TASK_DELAY_SEC = 900
TIMESTAMP_FUNC = time.time
PUBLISHER_STR_FUNC = lambda: '%s[%d]' % (platform.node(), os.getpid())

RESET_TIMEOUT_THRESHOLD = 1

USE_DEAD_LETTER_QUEUE = True

# Path for queue config
QUEUE_CONFIG = 'queue_config.yaml'
QUEUE_CLASS = 'kale.queue_info.TaskQueue'
QUEUE_SELECTOR = 'kale.queue_selector.ReducedLottery'

# We will gracefully stop this process if memory usage
# exceeds this amount (in MB).
DIE_ON_RESIDENT_SET_SIZE_MB = 256

# CIPHER used by kale.crypt, must be 16-, 24-, or 36-byte string
UTIL_CRYPT_CIPHER = '1234567890123456'

ENABLE_DEAD_LETTER_QUEUE = True

# Manually specify pickle protocol used for writing Pickle files
# Py2/Py3 have different default protocols. Reproduce those defaults
# here for backwards compatibility.
# Note: If Python 2 & Python 3 need to co-exist, override PICKLE_PROTOCOL=2
if six.PY2:
    PICKLE_PROTOCOL = 0
else:
    PICKLE_PROTOCOL = pickle.DEFAULT_PROTOCOL
