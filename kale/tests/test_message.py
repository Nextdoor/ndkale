"""Module testing the kale.message module."""

import mock
import unittest

from kale import message
from kale import task


class MessageTestCase(unittest.TestCase):
    """Test KaleMessage."""

    def test_validate_message(self):
        payload = {'args': [], 'kwargs': {}}
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
