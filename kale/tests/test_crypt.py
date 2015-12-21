# -*- coding: utf-8 -*-
import unittest

from Crypto.Cipher import AES

from kale import crypt
from kale import settings


__author__ = 'Aaron Webber (aaron@nextdoor.com)'


class Crypt2Test(unittest.TestCase):

    def setUp(self):
        self.TEST_MSGS = [b'12345',
                          b'adasdfasdfasdfasdf',
                          b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                          b's',
                          b'',
                          b'1616161616161616',
                          b'\x8A\x00\x34\xAF\xFF',
                          (b'\xFF\xFE\xFA\xEE\x01\x34\xAB\xFF\xFE\xFA\xEE\x01\x34' +
                           b'\xAB\xFF\xFE\xFA\xEE\x01\x34\xAB'),
                          u'這是一個Unicode測試'.encode('utf-8'),
                          b'\x00\x00\x00\x00\x00\x00\x00']

    def test_get_padding_bytes(self):
        padding_bytes = crypt._get_padding_bytes(3)
        self.assertEqual(13, len(padding_bytes))
        self.assertEqual(13, int(padding_bytes[12]))

        padding_bytes = crypt._get_padding_bytes(12)
        self.assertEqual(4, len(padding_bytes))
        self.assertEqual(4, int(padding_bytes[3]))

        padding_bytes = crypt._get_padding_bytes(24)
        self.assertEqual(8, len(padding_bytes))
        self.assertEqual(8, int(padding_bytes[7]))

        # msgs that are multiples of 16 long are padded with 16 bytes
        padding_bytes = crypt._get_padding_bytes(16)
        self.assertEqual(16, len(padding_bytes))
        self.assertEqual(16, int(padding_bytes[15]))

        padding_bytes = crypt._get_padding_bytes(32)
        self.assertEqual(16, len(padding_bytes))
        self.assertEqual(16, int(padding_bytes[15]))

    def test_pad(self):
        # This would probably be nicer with those nose test generators
        [self._assert_padded_right(msg) for msg in self.TEST_MSGS]

    def _assert_padded_right(self, test_msg):
        padded_msg = crypt._pad(test_msg)
        self.assertEqual(0, len(padded_msg) % crypt.BLOCK_SIZE)

    def test_unpad(self):
        for msg in self.TEST_MSGS:
            padded_msg = crypt._pad(msg)
            self.assertEqual(msg, crypt._unpad(padded_msg))

    def test_crypt(self):
        for msg in self.TEST_MSGS:
            self.assertNotEqual(msg, crypt.encrypt(msg))

    def test_decrypt(self):
        for msg in self.TEST_MSGS:
            crypted_msg = crypt.encrypt(msg)
            self.assertEqual(msg, crypt.decrypt(crypted_msg))

    def test_urlsafe_crypt(self):
        [self.assertNotEqual(msg, crypt.urlsafe_encrypt(msg)) for msg in self.TEST_MSGS]

    def test_urlsafe_decrypt(self):
        for msg in self.TEST_MSGS:
            crypted_msg = crypt.urlsafe_encrypt(msg)
            self.assertEqual(msg, crypt.urlsafe_decrypt(crypted_msg))

    def test_hex_crypt(self):
        [self.assertNotEqual(msg, crypt.hex_encrypt(msg)) for msg in self.TEST_MSGS]

    def text_hex_decrypt(self):
        for msg in self.TEST_MSGS:
            crypted_msg = crypt.hex_encrypt(msg)
            self.assertEqual(msg, crypt.hex_decrypt(crypted_msg))

    def test_crypt_order_doesnt_matter(self):
        # blockalgo.py has a concerning comment indicating that the cipher may be stateful,
        # and that we shouldn't be re-using the cipher object. However, empirical data
        # shows that re-using the cipher object does not cause any problems. This test
        # verifies that whatever internal state the cipher object may maintain does
        # not affect the cipher text.
        crypted_msgs = {msg: crypt.encrypt(msg) for msg in self.TEST_MSGS}
        # Reset to a new cipher
        crypt.cipher = AES.new(settings.UTIL_CRYPT_CIPHER)
        self.TEST_MSGS.reverse()
        crypted_in_reverse = {msg: crypt.encrypt(msg) for msg in self.TEST_MSGS}
        [self.assertEqual(crypted_msgs[msg],
                          crypted_in_reverse[msg]) for msg in self.TEST_MSGS]

    def test_bad_decryption_input(self):
        """Test that we get CryptException when we pass bad input to decrypt functions."""
        bad_messages = ['#$##$#$#$#$#',  # Just garbage
                        'abcdefABCDEF00==',  # base64 encoded garbage
                        'BYx3EzvPWTMLkBA5FF4xWw==',  # message with the padding byte > BLOCK_SIZE
                        'Hn+Ou2DpitXxWD47glRtJw==',  # message with the padding byte > message size
                        # more base64 encoded garbage
                        'QkJCQkJCQkJBQUFBQUFBQWFiY19lZkFCQ0RFRiUlAAE='
                        ]

        for f in [crypt.decrypt, crypt.hex_decrypt, crypt.urlsafe_decrypt]:
            for msg in bad_messages:
                with self.assertRaises(crypt.CryptException):
                    f(msg)

    def test_bad_encryption_input(self):
        """Test that we raise ValueError when we are passed bad input to encrypt functions."""
        bad_input = [crypt.CryptException(),  # not a str
                     123456,  # also not a str
                     u'測試測試',  # also not a str
                     ]

        for f in [crypt.encrypt, crypt.hex_encrypt, crypt.urlsafe_encrypt]:
            for msg in bad_input:
                with self.assertRaises(ValueError):
                    f(msg)
