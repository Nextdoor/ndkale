"""Module testing the kale.utils module."""
from __future__ import absolute_import
import resource
import sys

import mock
import unittest

from kale import utils


class TestRuMaxrssMb(unittest.TestCase):

    def test_linux(self):
        with mock.patch.object(sys, 'platform', 'posix'), \
            mock.patch.object(
                resource, 'getrusage', return_value=mock.Mock(ru_maxrss=2048)):
            self.assertEqual(utils.ru_maxrss_mb(), 2)

    def test_osx(self):
        with mock.patch.object(sys, 'platform', 'darwin'), \
            mock.patch.object(
                resource, 'getrusage', return_value=mock.Mock(ru_maxrss=1024 * 1024)):
            self.assertEqual(utils.ru_maxrss_mb(), 1)

    def test_other(self):
        with mock.patch.object(sys, 'platform', ''), \
            mock.patch.object(
                resource, 'getrusage', return_value=mock.Mock(ru_maxrss=2048)):
            self.assertEqual(utils.ru_maxrss_mb(), 2)
