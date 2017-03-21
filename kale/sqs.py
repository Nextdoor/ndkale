"""Base class for SQS utility classes."""
from __future__ import absolute_import

import logging

import boto.sqs
import boto.sqs.connection

from kale import exceptions
from kale import message
from kale import settings

logger = logging.getLogger(__name__)


class SQSTalk(object):
    """Base class for SQS utility classes."""

    # Class attribute for storing connections.
    _connections = {}
    _queues = {}

    def __init__(self, *args, **kwargs):
        """Constructor.
        :raises: exceptions.ImproperlyConfiguredException: Raised if the
            settings are not adequately configured.
        """

        if not settings.PROPERLY_CONFIGURED:
            raise exceptions.ImproperlyConfiguredException(
                'Settings are not properly configured.')

        aws_region = settings.AWS_REGION
        aws_access_key_id = settings.AWS_ACCESS_KEY_ID
        aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY

        conn_str = '%s:%s:%s' % (aws_region, aws_access_key_id,
                                 aws_secret_access_key)

        if conn_str not in self._connections:
            if settings.MESSAGE_QUEUE_USE_PROXY:
                # Used only in Dev environment which uses ElasticMQ instead of
                # SQS. Hence it uses a different boto api to connect to a proxy
                conn = boto.sqs.connection
                self._connections[conn_str] = conn.SQSConnection(
                    proxy_port=settings.MESSAGE_QUEUE_PROXY_PORT,
                    proxy=settings.MESSAGE_QUEUE_PROXY_HOST, is_secure=False,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
            else:
                # Used in staging and production
                self._connections[conn_str] = boto.sqs.connect_to_region(
                    aws_region,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

        self._connection = self._connections[conn_str]

    def _get_or_create_queue(self, queue_name):
        """Fetch or create a queue.

        :param str queue_name: string for queue name.
        :return: object of boto.sqs.queue.Queue.
        :rtype: Queue
        """
        # Check local cache first.
        if queue_name in self._queues:
            return self._queues[queue_name]

        queue = self._connection.lookup(queue_name)
        # If queue doesn't exist, create it.
        if queue is None:
            logger.info('Creating new SQS queue: %s' % queue_name)
            queue = self._connection.create_queue(queue_name)
        # Set the message class to be RawMessage so that
        # messages aren't decoded when fetched.
        queue.set_message_class(message.KaleMessage)
        self._queues[queue_name] = queue
        return queue

    def get_all_queues(self, prefix=''):
        """Returns all queues, filtered by prefix.

        :param str prefix: string for queue prefix.
        :return: a list of queue objects.
        :rtype: list[Queue]
        """
        return self._connection.get_all_queues(prefix)

    def delete_queue(self, queue_name):
        """Deletes a queue.

        :param str queue_name: string for queue name.
        :return: True if delete succeeded, False otherwise.
        :rtype: bool
        """
        # Safety - Make sure queue is referenced locally.
        if queue_name not in self._queues:
            logger.error('Tried to delete unknown queue: %s' % queue_name)
            return False

        logger.info('Deleting queue %s' % queue_name)
        return self._connection.delete_queue(queue_name)
