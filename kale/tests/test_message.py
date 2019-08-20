"""Module testing the kale.message module."""
from __future__ import absolute_import

import unittest

from kale import message
from kale import task

import mock


def _time_function():
    return 123


def _get_publisher_data():
    return 'test_publisher'


class MessageTestCase(unittest.TestCase):
    """Test KaleMessage."""

    def test_validate_message(self):
        payload = {'args': [], 'kwargs': {}}
        message.KaleMessage._validate_task_payload(payload)

    def test_validate_message_with_appdata(self):
        payload = {'args': [], 'kwargs': {}, 'app_data': {}}
        message.KaleMessage._validate_task_payload(payload)

    def test_message(self):
        payload = {'args': [], 'kwargs': {}}

        # Test create
        kale_msg = message.KaleMessage(
            task_class=task.Task,
            task_id=1,
            payload=payload,
            current_retry_num=None)

        self.assertIsNotNone(kale_msg)
        self.assertEqual('kale.task.Task', kale_msg.task_name)
        self.assertEqual(123, kale_msg._enqueued_time)
        self.assertEqual(0, kale_msg.task_retry_num)
        self.assertEqual(1, kale_msg.task_id)
        self.assertEqual([], kale_msg.task_args)
        self.assertEqual({}, kale_msg.task_kwargs)

    def test_message_with_appdata(self):
        payload = {'args': [], 'kwargs': {}, 'app_data': {}}

        # Test create
        kale_msg = message.KaleMessage(
            task_class=task.Task,
            task_id=1,
            payload=payload,
            current_retry_num=None)
        self.assertIsNotNone(kale_msg)
        self.assertEqual({}, kale_msg.task_app_data)

    def test_encode(self):
        payload = {'args': [], 'kwargs': {}, 'app_data': {}}
        message._get_current_timestamp = _time_function
        message._get_publisher_data = _get_publisher_data

        kale_msg = message.KaleMessage(
            task_class=task.Task,
            task_id=1,
            payload=payload,
            current_retry_num=None)

        with mock.patch('kale.message.pickle') as pickle:
            pickle.dumps.return_value = b'\x80'
            actual = kale_msg.encode()

        expected = 'OKyZrDvbdIV4hnAi07xWGg=='
        self.assertEqual(expected, actual)

    def test_decode(self):
        message_body = {
            'id': 1,
            'task': 'kale.task.Task',
            'payload': {'args': [], 'kwargs': {}},
            '_enqueued_time': 123,
            'publisher_data': '',
            'current_retry_num': 0
        }

        with mock.patch('kale.message.pickle') as pickle:
            pickle.loads.return_value = message_body
            ciphertext = 'Qx2KhutzbmsCC8NaLkKMXjtMKox/HlpwGz+IM0jzMElyptGsyBQald2EL' \
                         'qADXqyiJCu0RvD6sDnOKYITIfHz1qSl5qeSZrbslvFJeVXTF4PYaEz69g' \
                         'ASICeunTWkCMNla0wnpiJvu4QMEWmubi+RFgFBkTYSnQXG5NtgUCB0ifD' \
                         'PDgoKDtzSIC354LxZjCBmRg1kpjfZ+zNGJ8DMw6YabQ=='
            kale_msg = message.KaleMessage.decode(ciphertext)

        self.assertIsNotNone(kale_msg)
        self.assertEqual('kale.task.Task', kale_msg.task_name)
        self.assertEqual(123, kale_msg._enqueued_time)
        self.assertEqual(0, kale_msg.task_retry_num)
        self.assertEqual(1, kale_msg.task_id)
        self.assertEqual([], kale_msg.task_args)
        self.assertEqual({}, kale_msg.task_kwargs)
