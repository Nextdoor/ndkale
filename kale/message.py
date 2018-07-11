"""Custom message type for SQS messages."""
from __future__ import absolute_import

import pickle

from boto.sqs import message
import six

from kale import crypt
from kale import exceptions
from kale import settings
from kale import utils

_compressor = settings.COMPRESSOR
_decompressor = settings.DECOMPRESSOR
_task_size_limit = settings.SQS_TASK_SIZE_LIMIT
_get_current_timestamp = settings.TIMESTAMP_FUNC
_get_publisher_data = settings.PUBLISHER_STR_FUNC


class KaleMessage(message.RawMessage):
    """Kale message representing the data stored in an SQS queue."""

    # _task_mapper is a class cache mapping task paths to classes.
    # It will initially be populated with keys provided and will lazily
    # create other mappings.
    _task_mapper = None

    def __init__(self, queue=None, body=''):
        """Constructor.

        :param queue:
        :param body: string for message body
        """
        message.RawMessage.__init__(self, queue=queue, body=body)

        # Lazily instantiate the task mapper.
        if not self._task_mapper:
            self._task_mapper = {k: utils.class_import_from_path(v)
                                 for k, v in six.iteritems(settings.TASK_MAPPER)}

    @classmethod
    def create_message(cls, task_class=None, task_id=None, payload=None,
                       queue=None, current_retry_num=None):
        """Factory method to craete a message.

        :param task_class: class of a task.
        :param task_id: string for task id.
        :param payload: dictionary for message payload.
        :param queue: object of boto.sqs.queue.Queue.
        :param current_retry_num: integer for how many retries so far.
        :return:
        """
        # Consider moving the queue to be a task_class
        # attr (needs to be fetched from SQS though).
        KaleMessage._validate_task_payload(payload)
        retry_count = current_retry_num or 0
        message_data = {
            'id': task_id,
            # This represents the path to the task. The consumer will have a
            # dictionary mapping these values to task classes.
            'task': '%s.%s' % (task_class.__module__, task_class.__name__),
            # Payload holds the data that the task's run_task method will be
            # called with.
            # Ex: mytask.ThisTask().run_task(
            # *payload['args'], **payload['kwargs'])
            'payload': payload,
            # These are task attributes used for tracking/analytics/debugging
            # Time when the task publisher published this task.
            '_enqueued_time': _get_current_timestamp(),
            # Publishing host (potentially include caller function)
            '_publisher': _get_publisher_data(),
            # Current task retry. This will be 0 from new tasks and will be
            # incremented for each retry.
            'retry_num': retry_count,
        }
        return KaleMessage(queue=queue, body=message_data)

    @staticmethod
    def _validate_task_payload(payload):
        """Validate that this is a valid task.

        :param payload: dictionary that will be submitted to the queue.
        :raises: AssertionError if payload is invalid.
        """

        assert 'args' in payload, 'args is required to be in the payload'
        assert 'kwargs' in payload, 'kwargs is required to be in the payload'

    def encode(self, msg):
        """Custom encoding for Kale tasks.

        :param dict msg: message to decode.
        :return: string for encoded message.
        :rtype: str
        """

        compressed_msg = _compressor(pickle.dumps(msg, protocol=settings.PICKLE_PROTOCOL))
        compressed_msg = crypt.encrypt(compressed_msg)
        # Check compressed task size.
        if len(compressed_msg) >= _task_size_limit:
            task_id = msg.get('task_id')
            raise exceptions.ChubbyTaskException(
                'Task %s is over the limit of %d bytes.' % (task_id,
                                                            _task_size_limit))

        return compressed_msg

    def decode(self, msg):
        """Custom decoding for Kale tasks.

        :param str msg: message to decode.
        :return: dictionary for decoded message.
        :rtype: dict
        """

        message_body = crypt.decrypt(msg)
        message_body = pickle.loads(_decompressor(message_body))
        self.task_name = message_body['task']
        self.task_id = message_body['id']
        self.task_args = message_body['payload']['args']
        self.task_kwargs = message_body['payload']['kwargs']
        self.task_retry_num = message_body['retry_num']
        self.task_app_data = message_body['payload'].get('app_data')

        # This will instantiate the task.
        self.task_inst = self._class_from_path(self.task_name)(message_body)

        return message_body

    def _class_from_path(self, task_path):
        """Return the task class given a task's path.

        :param task_path: string for a class, e.g., mytask.MyTask
        :return:
        """
        if task_path not in self._task_mapper:
            task_class = utils.class_import_from_path(task_path)
            self._task_mapper[task_path] = task_class
        return self._task_mapper[task_path]
