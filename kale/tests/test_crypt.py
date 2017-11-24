# -*- coding: utf-8 -*-
import unittest

from kale import crypt

__author__ = 'Aaron Webber (aaron@nextdoor.com)'


class Crypt2Test(unittest.TestCase):

    def setUp(self):
        self.msgs_encrypt = [
            (b'12345', 'hU6aBS2mJgr0DUrUiHQouA=='),
            (b'adasdfasdfasdfasdf', 'Y/VR7vs4e2arRx8C6EBGxZanBnCrxVik3257tMlqomM='),
            (b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
             'C9pEG6g8ge76xt2q9XLbpwvaRBuoPIHu+sbdqvVy26ccNo37r4pWAD/7HiC77bI+'),
            (b's', 'gPm/2E+m/YMX16Yxg81wqA=='),
            (b'', '1lcHEDp3HufBy14CHkSlVw=='),
            (b'1616161616161616', 'wqx+H/v7iFtW7acYb9QoQ9ZXBxA6dx7nwcteAh5EpVc='),
            (b'\x8a\x004\xaf\xff', 'cVz2i16U1c3ztu2GwMhtvg=='),
            (b'\xff\xfe\xfa\xee\x014\xab\xff\xfe\xfa\xee\x014\xab\xff\xfe\xfa\xee\x014\xab',
             'I6XMloWUi3GQwjq5kP15Z3EI+LIV3Ygsda2FiSIgtM8='),
            (b'\xe9\x80\x99\xe6\x98\xaf\xe4\xb8\x80\xe5\x80\x8bUnicode\xe6\xb8\xac\xe8\xa9\xa6',
             'XwjwdL+rHdD6pHr/eUckPJUC7jBbc0LUr8g7f7FffBI='),
            (b'\x00\x00\x00\x00\x00\x00\x00', '/b/++aGOO0Kj2e2+YWadsQ==')
        ]

    @property
    def msgs(self):
        return [x[0] for x in self.msgs_encrypt]

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
        [self._assert_padded_right(msg) for msg in self.msgs]

    def _assert_padded_right(self, test_msg):
        padded_msg = crypt._pad(test_msg)
        self.assertEqual(0, len(padded_msg) % crypt.BLOCK_SIZE)

    def test_unpad(self):
        for msg in self.msgs:
            padded_msg = crypt._pad(msg)
            self.assertEqual(msg, crypt._unpad(padded_msg))

    def test_crypt(self):
        for msg in self.msgs:
            self.assertNotEqual(msg, crypt.encrypt(msg))

    def test_decrypt(self):
        for msg in self.msgs:
            crypted_msg = crypt.encrypt(msg)
            self.assertEqual(msg, crypt.decrypt(crypted_msg))

    def test_urlsafe_crypt(self):
        [self.assertNotEqual(msg, crypt.urlsafe_encrypt(msg)) for msg in self.msgs]

    def test_urlsafe_decrypt(self):
        for msg in self.msgs:
            crypted_msg = crypt.urlsafe_encrypt(msg)
            self.assertEqual(msg, crypt.urlsafe_decrypt(crypted_msg))

    def test_hex_crypt(self):
        [self.assertNotEqual(msg, crypt.hex_encrypt(msg)) for msg in self.msgs]

    def text_hex_decrypt(self):
        for msg in self.msgs:
            crypted_msg = crypt.hex_encrypt(msg)
            self.assertEqual(msg, crypt.hex_decrypt(crypted_msg))

    def test_crypt_order_doesnt_matter(self):
        # This is effectively a test that we are using ECB (or some other non-chained)
        # mode of operation, because as long as we are it's fine to keep using the same
        # cipher object over and over. If we switched to a chained mode, this test would
        # break and we would need to create a new cipher object (and IV and/or nonce,
        # depending on mode) for every message.
        crypted_msgs = {msg: crypt.encrypt(msg) for msg in self.msgs}
        # Reset to a new cipher
        crypt._set_cipher()
        self.msgs.reverse()
        crypted_in_reverse = {msg: crypt.encrypt(msg) for msg in self.msgs}
        [self.assertEqual(crypted_msgs[msg],
                          crypted_in_reverse[msg]) for msg in self.msgs]

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

    def test_backwards_compatible(self):
        """Test that we still decrypt any existing messages correctly."""
        for msg, encrypted in self.msgs_encrypt:
            self.assertEqual(msg, crypt.decrypt(encrypted))
