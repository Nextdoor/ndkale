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

from Crypto.Cipher import AES
import base64
import logging
import struct

from kale import settings


log = logging.getLogger(__name__)

# Set up our Crypto hashing object
BLOCK_SIZE = 16

# Get our cipher key from the settings file
cipher = None
if settings.UTIL_CRYPT_CIPHER:
    cipher = AES.new(settings.UTIL_CRYPT_CIPHER)


class CryptException(Exception):
    """General encryption/decryption failure exception."""


def encrypt(msg):
    """Encrypts a message.

    :param msg: string message to be encrypted.
    :return: string for an encrypted version of msg.
    """
    if not cipher:
        return msg
    if not isinstance(msg, (str, unicode)):
        msg = str(msg)

    try:
        msg = base64.b64encode(cipher.encrypt(_pad(msg)))
    except Exception as e:
        raise CryptException(e)

    return msg


def decrypt(msg):
    """Decrypts a message.

    :param str msg: string of message to be decrypted.
    :return: string for original message.
    :rtype: str
    """
    if not cipher:
        return msg

    try:
        msg = _unpad(cipher.decrypt(base64.b64decode(msg)))
    except Exception as e:
        raise CryptException(e)

    return msg


def urlsafe_encrypt(msg):
    """Urlsafe encrypts a message.

    :param str msg: string message to be encrypted.
    :return: string of encrypted version of msg.
    :rtype: str
    """
    if not cipher:
        return msg
    if not isinstance(msg, (str, unicode)):
        msg = str(msg)

    try:
        msg = base64.urlsafe_b64encode(cipher.encrypt(_pad(msg)))
    except Exception as e:
        raise CryptException(e)

    return msg


def hex_encrypt(msg):
    """Hex encrypts a message.

    :param str msg: string message to be encrypted.
    :return: string for encrypted version of msg in hex.
    :rtype: str
    """
    if not cipher:
        return msg
    if not isinstance(msg, (str, unicode)):
        msg = str(msg)

    try:
        msg = cipher.encrypt(_pad(msg)).encode('hex')
    except Exception as e:
        raise CryptException(e)

    return msg


def hex_decrypt(msg):
    """Decrypts a message.

    :param str msg: string for the message to be decrypted.
    :return: string for the original message.
    :rtype: str
    """
    if not cipher:
        return msg

    try:
        msg = _unpad(cipher.decrypt(msg.decode('hex')))
    except Exception as e:
        raise CryptException(e)

    return msg


def urlsafe_decrypt(msg):
    """Urlsafe decrypts a message.

    :param str msg: string for the message to be decrypted.
    :return: string for the original message.
    :rtype: str
    """

    if not cipher:
        return msg

    try:
        msg = _unpad(cipher.decrypt(base64.urlsafe_b64decode(str(msg))))
    except Exception as e:
        raise CryptException(e)

    return msg


def _pad(msg):
    """Pad the message with enough bytes to be a multiple of BLOCK_SIZE.

    :param str msg: str or unicode message to be padded.
    :return: the msg with padding added.
    :rtype: str
    """

    padding_bytes = _get_padding_bytes(len(msg))
    return '%s%s' % (msg, padding_bytes)


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
    return struct.pack(pack_format, bytes_to_pad)


def _unpad(msg):
    """Removes the padding we applied to the message with _pad.

    We read the last byte of the message as an integer
    and strip that many bytes off the end of the message. Note that
    reading as a regular integer(i.e. struct.unpack('i')) would
    read multiple bytes off the end which would be bad if,
    for instance, we only had one byte of padding. We also convert the message
    to a bytearray for safety - the msg could be a unicode-encoded string,
    in which case msg[-1:] might not remove a single byte.

    :param msg: str or unicode msg that needs to have padding stripped from it.
    :return: string for the message with the padding removed.
    """
    msg_as_bytes = bytearray(msg)
    bytes_to_strip = int(struct.unpack('B', str(msg_as_bytes[-1:]))[0])
    return msg[:(-1 * bytes_to_strip)]
