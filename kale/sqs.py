"""Base class for SQS utility classes."""
from __future__ import absolute_import

import logging

import boto3
import botocore
from kale import exceptions
from kale import settings

logger = logging.getLogger(__name__)


class SQSTalk(object):
    """Base class for SQS utility classes."""

    _client = None
    _session = None
    _sqs = None

    # queue name to SQS.Queue object mapping
    _queues = {}

    def __init__(self, *args, **kwargs):
        """Constructor.
        :raises: exceptions.ImproperlyConfiguredException: Raised if the
            settings are not adequately configured.
        """

        if not settings.PROPERLY_CONFIGURED:
            raise exceptions.ImproperlyConfiguredException(
                'Settings are not properly configured.')

        aws_region = None
        if settings.AWS_REGION != '':
            aws_region = settings.AWS_REGION

        aws_access_key_id = None
        if settings.AWS_ACCESS_KEY_ID != '':
            aws_access_key_id = settings.AWS_ACCESS_KEY_ID

        aws_secret_access_key = None
        if settings.AWS_SECRET_ACCESS_KEY != '':
            aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY

        # If None is passed, Boto3 uses its default behavior to determine the URL
        endpoint_url = None
        if settings.MESSAGE_QUEUE_ENDPOINT_URL:
            endpoint_url = settings.MESSAGE_QUEUE_ENDPOINT_URL

        self._session = boto3.Session(region_name=aws_region,
                                      aws_access_key_id=aws_access_key_id,
                                      aws_secret_access_key=aws_secret_access_key)

        self._client = self._session.client('sqs', endpoint_url=endpoint_url)
        self._sqs = self._session.resource('sqs', endpoint_url=endpoint_url)

        self._sqs_queue_name_to_tag = settings.SQS_QUEUE_TAG_FUNCTION

    def _get_or_create_queue(self, queue_name):
        """Fetch or create a queue.

        :param str queue_name: string for queue name.
        :return: Queue
        :rtype: boto3.resources.factory.sqs.Queue
        """

        # Check local cache first.
        if queue_name in self._queues:
            return self._queues[queue_name]

        # get or create queue
        try:
            resp = self._client.get_queue_url(QueueName=queue_name)
            queue_url = resp.get('QueueUrl')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'AWS.SimpleQueueService.NonExistentQueue':
                raise e
            tags = self._get_sqs_queue_tags(queue_name)

            logger.info('Creating new SQS queue: %s' % queue_name)
            queue = self._client.create_queue(QueueName=queue_name, tags=tags)
            queue_url = queue.get('QueueUrl')

        # create queue object
        queue = self._sqs.Queue(queue_url)

        self._queues[queue_name] = queue
        return queue

    def get_all_queues(self, prefix=''):
        """Returns all queues, filtered by prefix.

        :param str prefix: string for queue prefix.
        :return: a list of queue objects.
        :rtype: list[boto3.resources.factory.sqs.Queue]
        """

        # QueueNamePrefix is optional and can not be None.
        resp = self._client.list_queues(QueueNamePrefix=prefix)

        queue_urls = resp.get('QueueUrls', [])

        queues = []
        for queue_url in queue_urls:
            queues.append(self._sqs.Queue(queue_url))

        return queues

    def _get_sqs_queue_tags(self, queue_name):
        try:
            tags = self._sqs_queue_name_to_tag(queue_name) or {}
            if tags:
                # Tags must be a Dict[str, str]
                tags = {
                    str(k): str(v)
                    for k, v in tags.items()
                }
            return tags
        except Exception as e:
            logger.warning('Failed to extract SQS Queue tags %s' % queue_name, exc_info=True)
            return {}
