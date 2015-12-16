"""Module testing the kale.message module."""
from __future__ import absolute_import

import mock
import unittest

from kale import message
from kale import task


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
        kale_msg = message.KaleMessage.create_message(
            task_class=task.Task,
            task_id=1,
            payload=payload,
            queue=mock.MagicMock(),
            current_retry_num=None)
        self.assertIsNotNone(kale_msg)

    def test_message_with_appdata(self):
        payload = {'args': [], 'kwargs': {}, 'app_data': {}}

        # Test create
        kale_msg = message.KaleMessage.create_message(
            task_class=task.Task,
            task_id=1,
            payload=payload,
            queue=mock.MagicMock(),
            current_retry_num=None)
        self.assertIsNotNone(kale_msg)
