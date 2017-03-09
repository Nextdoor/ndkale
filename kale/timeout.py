"""Module containing timeout methods for monitoring tasks.

Modeled after:
http://stackoverflow.com/a/601168/854976
"""
from __future__ import absolute_import

import errno
import os
import signal
from contextlib import contextmanager

from kale import exceptions


@contextmanager
def time_limit(seconds, error_message=os.strerror(errno.ETIME)):
    """Context manager for handling method timeouts.

    Usage:
        try:
            with time_limit(10):
                some_function()
        except TimeoutException:
            # error handling here

    :param int seconds: seconds before timeout.
    :param str error_message: error message.
    :raises: kale.exceptions.TimeoutException
    """

    def _handle_timeout(signum, frame):
        """Handle timout signal."""
        raise exceptions.TimeoutException(error_message)

    original_handler = signal.signal(signal.SIGALRM, _handle_timeout)
    signal.alarm(seconds)
    try:
        yield
    finally:
        # Reset original handler.
        signal.signal(signal.SIGALRM, original_handler)
        # Remove signal.
        signal.alarm(0)
