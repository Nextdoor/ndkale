"""Simple package for encrypting/decrypting strings.

This package provides a simple mechanism for encrypting and decrypting strings
very quickly using a private key. For simplicity, this key is stored in the
settings module and is used globally here -- that is, every message encrypted
with these functions will be encrypted with the same key.

This package is very simple. If you choose to encrypt data one day, and not
encrypt it the next day, you need to handle the failure scenarios.

Usage ::

    from kale import crypt

    encrypted_message = crypt.encrypt("foo")
    decrypted_message = crypt.decrypt(encrypted_message)

"""
from __future__ import absolute_import

import base64
import builtins
import codecs
import logging
import struct

from Crypto.Cipher import AES
import six

from kale import settings

log = logging.getLogger(__name__)

# Set up our Crypto hashing object
BLOCK_SIZE = 16


def _set_cipher():
    global cipher
    cipher = AES.new(builtins.bytes(settings.UTIL_CRYPT_CIPHER, 'ascii'), AES.MODE_ECB)


# Get our cipher key from the settings file
cipher = None
if settings.UTIL_CRYPT_CIPHER:
    _set_cipher()


class CryptException(Exception):
    """General encryption/decryption failure exception."""


def encrypt(msg):
    """Encrypts a message.

    :param msg: byte string message to be encrypted.
    :return: bytes for an encrypted version of msg.
    :raises: ValueError if passed anything other than bytes.
        CryptException if the encryption fails.
    """
    if not cipher:
        return msg
    if not isinstance(msg, six.binary_type):
        raise ValueError('only bytes can be encrypted')
    if six.PY2:
        msg = builtins.bytes(msg)

    msg = _pad(msg)
    msg = cipher.encrypt(msg)
    msg = base64.b64encode(msg)

    return msg


def decrypt(msg):
    """Decrypts a message.

    :param bytes msg: string of message to be decrypted.
    :return: bytes for original message.
    :rtype: bytes
    """
    if not cipher:
        return msg

    if isinstance(msg, six.text_type):
        # This should be a base64 string, so it
        # should encode to ascii without any problems.
        msg = msg.encode('ascii')

    if not isinstance(msg, six.binary_type):
        raise ValueError('Only bytes(or unicodes) can be decrypted')

    if six.PY2:
        msg = builtins.bytes(msg)

    try:
        msg = _unpad(cipher.decrypt(base64.b64decode(msg)))
    except (ValueError, TypeError) as e:
        # We can get struct.error if we end up passing an empty string
        # to _unpad. We get
        raise CryptException(e)

    return msg


def urlsafe_encrypt(msg):
    """Urlsafe encrypts a message.

    :param bytes msg: string message to be encrypted.
    :return: string of encrypted version of msg.
    :rtype: bytes
    """
    if not cipher:
        return msg
    if not isinstance(msg, six.binary_type):
        raise ValueError('only bytes can be encrypted')

    msg = base64.urlsafe_b64encode(cipher.encrypt(_pad(msg)))

    return msg


def hex_encrypt(msg):
    """Hex encrypts a message.

    :param bytes msg: string message to be encrypted.
    :return: string for encrypted version of msg in hex.
    :rtype: bytes
    """
    if not cipher:
        return msg
    if not isinstance(msg, six.binary_type):
        raise ValueError('only bytes can be encrypted')

    msg = cipher.encrypt(_pad(msg))
    msg = codecs.encode(msg, 'hex')

    return msg


def hex_decrypt(msg):
    """Decrypts a message.

    :param bytes msg: string for the message to be decrypted.
    :return: string for the original message.
    :rtype: bytes
    """
    if not cipher:
        return msg

    if isinstance(msg, six.text_type):
        # This should be a hex encoded string, so it
        # should encode to ascii without any problems.
        msg = msg.encode('ascii')

    if not isinstance(msg, six.binary_type):
        raise ValueError('Only bytes or unicodes can be decrypted')

    try:
        msg = _unpad(cipher.decrypt(codecs.decode(msg, 'hex')))
    except (ValueError, TypeError) as e:
        raise CryptException(e)

    return msg


def urlsafe_decrypt(msg):
    """Urlsafe decrypts a message.

    :param bytes msg: string for the message to be decrypted.
    :return: string for the original message.
    :rtype: bytes
    """

    if not cipher:
        return msg

    if isinstance(msg, six.text_type):
        # This should be a base64 encoded string, so it
        # should encode to ascii without any problems.
        msg = msg.encode('ascii')

    if not isinstance(msg, six.binary_type):
        raise ValueError('Only bytes(or unicodes) can be decrypted')

    try:
        msg = _unpad(cipher.decrypt(base64.urlsafe_b64decode(msg)))
    except (ValueError, TypeError) as e:
        raise CryptException(e)

    return msg


def _pad(msg):
    """Pad the message with enough bytes to be a multiple of BLOCK_SIZE.

    :param bytes msg: bytes message to be padded.
    :return: the msg with padding added.
    :rtype: bytes
    """

    padding_bytes = _get_padding_bytes(len(msg))
    return msg + padding_bytes


def _get_padding_bytes(msg_length):
    """Gets the bytes to pad a message of a given length with.

    We pad out with null bytes until we are one short of being
    a multiple of BLOCK_SIZE. The last byte is always the number
    of bytes we padded out(including the last one). We always apply
    some padding in order to be able to consistently remove the padding,
    so if we would apply no padding, we add padding of
    BLOCK_SIZE.

    :param msg_length: the length of the message to pad.
    :return: the bytes that should be appended to the message to pad it.
    """

    bytes_to_pad = BLOCK_SIZE - msg_length % BLOCK_SIZE
    if bytes_to_pad == 0:
        bytes_to_pad = BLOCK_SIZE
    pack_format = '%sB' % ('x' * (bytes_to_pad - 1))
    msg = struct.pack(pack_format, bytes_to_pad)
    return builtins.bytes(msg)


def _unpad(msg):
    """Removes the padding we applied to the message with _pad.

    We read the last byte of the message as an integer
    and strip that many bytes off the end of the message. Note that
    reading as a regular integer(i.e. struct.unpack('i')) would
    read multiple bytes off the end which would be bad if,
    for instance, we only had one byte of padding.

    :param msg: bytes or unicode msg that needs to have padding stripped from it.
    :return: bytes for the message with the padding removed.
    """
    if not msg or len(msg) < BLOCK_SIZE:
        raise ValueError('decrypted message was not padded correctly')
    msg = builtins.bytes(msg)
    bytes_to_strip = int(msg[-1])
    # If we are trying to strip off more than BLOCK_SIZE bytes,
    # or more bytes than there are in the msg,
    # something has gone wrong, likely the msg was corrupt.
    if bytes_to_strip > BLOCK_SIZE or bytes_to_strip > len(msg):
        raise ValueError('decrypted message was not padded correctly')
    return msg[:-bytes_to_strip]
