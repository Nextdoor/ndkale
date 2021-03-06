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


class QueueException(Exception):
    """Base class for queue exceptions."""


class SendMessagesException(QueueException):
    """Exception raised when a queue returns a non-zero number
    of failures on send.
    """
    def __init__(self, msg_count):
        super().__init__("{} messages failed to be delivered to SQS".format(msg_count))


class DeleteMessagesException(QueueException):
    """Exception raised when a queue returns a non-zero number
    of failures on delete.
    """
    def __init__(self, msg_count):
        super().__init__("{} messages failed to be deleted".format(msg_count))


class ChangeMessagesVisibilityException(QueueException):
    """Exception raised when a queue returns a non-zero number
    of failures on change message visibility.
    """
    def __init__(self, msg_count):
        super().__init__("{} messages failed to change visibility in SQS".format(msg_count))
