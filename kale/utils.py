"""Module containing utility functions for kale."""


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
