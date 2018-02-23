"""Module containing utility functions for kale."""
import resource
import sys


def class_import_from_path(path_to_class):
    """Import a class from a path string.

    :param str path_to_class: class path, e.g., kale.consumer.Consumer
    :return: class object
    :rtype: class
    """

    components = path_to_class.split('.')
    module = __import__('.'.join(components[:-1]))
    for comp in components[1:-1]:
        module = getattr(module, comp)
    return getattr(module, components[-1])


def ru_maxrss_mb():
    """Gets memory residence set size in megabytes.

    Returns:
        Integer, megabytes.
    """
    resource_data = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == 'darwin':
        return resource_data.ru_maxrss / (1024 * 1024)
    else:
        return resource_data.ru_maxrss / 1024
