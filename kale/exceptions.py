"""Exceptions raised by the kale package."""


class ImproperlyConfiguredException(Exception):
    """Exception raised when Kale is improperly configured."""


class TaskException(Exception):
    """Base class for task exceptions."""


class ChubbyTaskException(TaskException):
    """Exception raised when a task is too chubby for SQS."""


class TimeoutException(TaskException):
    """Exception raised when a task exceeds its time limit."""
    pass


class InvalidTimeLimitTaskException(TaskException):
    """Exception raised when a task's time_limit exceeds its queue's
       visibility timeout.
    """


class InvalidTaskDelayException(TaskException):
    """Exception raised when a task is published with an invalid delay time."""


class BlacklistedException(TaskException):
    """Exception raised when a task has been blacklisted."""
