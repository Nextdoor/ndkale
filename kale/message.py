"""Custom message type for SQS messages."""
from __future__ import absolute_import

import pickle

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


class KaleMessage:
    """Kale message representing the data stored in an SQS queue."""

    # _task_mapper is a class cache mapping task paths to classes.
    # It will initially be populated with keys provided and will lazily
    # create other mappings.
    _task_mapper = None

    def __init__(self,
                 sqs_message_id=None,
                 sqs_receipt_handle=None,
                 sqs_queue_name=None,
                 task_class=None,
                 task_name=None,
                 task_id=None,
                 payload=None,
                 current_retry_num=None,
                 current_failure_num=None,
                 enqueued_time=None,
                 publisher_data=None,
                 instantiate_task=False,
                 delete_func=None
                 ):
        """Constructor.

        :param task_class: Class of task.
        :param task_name: Name of task. Required if task_class is not set.
        :param task_id: Id of task.
        :param payload: Payload holds the data that the task's run_task method will be called with.
        :param current_retry_num: Current task retry. This will be 0 from new tasks and will be
        incremented for each retry.
        :param current_failure_num: Current task failure. This will be 0 from new tasks and will be
        incremented for each failure.
        :param enqueued_time: Timestamp of when message was queued. If not provided then value set
        from setting's timestamp function.
        :param publisher_data: Str containing information about the publisher. If not provided the
        value from settings used.
        :param instantiate_task: Whether create instance of task_class. Default is false.
        :param delete_func: Delete function from the SQS message.

        """

        self._validate_task_payload(payload)
        retry_count = current_retry_num or 0
        failure_count = current_failure_num or 0

        self.id = sqs_message_id
        self.sqs_queue_name = sqs_queue_name
        self.sqs_receipt_handle = sqs_receipt_handle

        # This represents the path to the task. The consumer will have a
        # dictionary mapping these values to task classes.
        if task_class is not None:
            self.task_name = '.'.join([task_class.__module__, task_class.__name__])
        else:
            self.task_name = task_name

        self.task_id = task_id
        self.task_args = payload.get('args')
        self.task_kwargs = payload.get('kwargs')
        self.task_app_data = payload.get('app_data')
        self.task_retry_num = retry_count
        self.task_failure_num = failure_count
        self._enqueued_time = enqueued_time or _get_current_timestamp()
        self._publisher_data = publisher_data or _get_publisher_data()

        # Lazily instantiate the task mapper.
        if not self._task_mapper:
            self._task_mapper = {k: utils.class_import_from_path(v)
                                 for k, v in six.iteritems(settings.TASK_MAPPER)}

        # This will instantiate the task.
        if instantiate_task:
            self.task_inst = self._class_from_path(self.task_name)(self._get_message_body())

        self.delete_func = delete_func

    @staticmethod
    def _validate_task_payload(payload):
        """Validate that this is a valid task.

        :param payload: dictionary that will be submitted to the queue.
        :raises: AssertionError if payload is invalid.
        """

        assert 'args' in payload, 'args is required to be in the payload'
        assert 'kwargs' in payload, 'kwargs is required to be in the payload'

    def _get_message_body(self):
        message_body = {
            'id': self.task_id,
            'task': self.task_name,
            # Payload holds the data that the task's run_task method will be
            # called with.
            # Ex: mytask.ThisTask().run_task(
            # *payload['args'], **payload['kwargs'])
            'payload': {
                'args': self.task_args,
                'kwargs': self.task_kwargs,
                'app_data': self.task_app_data,
            },
            '_enqueued_time': self._enqueued_time,
            '_publisher': self._publisher_data,
            'retry_num': self.task_retry_num,
            'failure_num': self.task_failure_num,
        }
        return message_body

    def encode(self):
        """Custom encoding for Kale tasks.

        :return: string for encoded message.
        :rtype: str
        """

        compressed_msg = _compressor(
            pickle.dumps(self._get_message_body(), protocol=settings.PICKLE_PROTOCOL))
        compressed_msg = crypt.encrypt(compressed_msg)
        # Check compressed task size.
        if len(compressed_msg) >= _task_size_limit:
            raise exceptions.ChubbyTaskException(
                'Task %s is over the limit of %d bytes.' % (self.task_id,
                                                            _task_size_limit))

        return compressed_msg.decode("utf-8")

    @classmethod
    def decode_sqs(cls, sqs_message):
        """Custom decoding for Kale tasks from sqs messages

        :param boto3.resources.factory.sqs.Message sqs_message: message to decode.

        :return: a kale message
        :rtype: KaleMessage
        """

        message_body = crypt.decrypt(sqs_message.body)
        message_body = pickle.loads(_decompressor(message_body))
        # queue_url format is https://queue.amazonaws.com/<account id>/<queue name>
        sqs_queue_name = sqs_message.queue_url.rsplit('/', 1)[1]

        msg = cls(
            sqs_queue_name=sqs_queue_name,
            sqs_message_id=sqs_message.message_id,
            sqs_receipt_handle=sqs_message.receipt_handle,
            task_id=message_body.get('id'),
            task_name=message_body.get('task'),
            payload=message_body.get('payload'),
            enqueued_time=message_body.get('_enqueued_time'),
            publisher_data=message_body.get('_publisher'),
            current_retry_num=message_body.get('retry_num'),
            current_failure_num=message_body.get('failure_num'),
            instantiate_task=True,
            delete_func=sqs_message.delete
        )

        return msg

    @classmethod
    def decode_str(cls, message_str):
        """Custom decoding for Kale tasks from strings

        :param str message_str: message to decode.

        :return: a kale message
        :rtype: KaleMessage
        """

        message_body = crypt.decrypt(message_str)
        message_body = pickle.loads(_decompressor(message_body))

        msg = cls(
            task_id=message_body.get('id'),
            task_name=message_body.get('task'),
            payload=message_body.get('payload'),
            enqueued_time=message_body.get('_enqueued_time'),
            publisher_data=message_body.get('_publisher'),
            current_retry_num=message_body.get('retry_num'),
            current_failure_num=message_body.get('failure_num')
        )

        return msg

    def _class_from_path(self, task_path):
        """Return the task class given a task's path.

        :param task_path: string for a class, e.g., mytask.MyTask
        :return:
        """
        if task_path not in self._task_mapper:
            task_class = utils.class_import_from_path(task_path)
            self._task_mapper[task_path] = task_class
        return self._task_mapper[task_path]

    def delete(self):
        if self.delete_func is not None:
            self.delete_func()
