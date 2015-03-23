import struct
import unittest

from kale import crypt


class Crypt2Test(unittest.TestCase):

    TEST_MSGS = ['12345',
                 'adasdfasdfasdfasdf',
                 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 's',
                 '',
                 '1616161616161616',
                 '\x8A\x00\x34\xAF\xFF',
                 ('\xFF\xFE\xFA\xEE\x01\x34\xAB\xFF\xFE\xFA\xEE\x01\x34' +
                  '\xAB\xFF\xFE\xFA\xEE\x01\x34\xAB')]

    def test_get_padding_bytes(self):
        padding_bytes = crypt._get_padding_bytes(3)
        self.assertEqual(13, len(padding_bytes))
        self.assertEqual(13, int(struct.unpack('B', padding_bytes[12])[0]))

        padding_bytes = crypt._get_padding_bytes(12)
        self.assertEqual(4, len(padding_bytes))
        self.assertEqual(4, int(struct.unpack('B', padding_bytes[3])[0]))

        padding_bytes = crypt._get_padding_bytes(24)
        self.assertEqual(8, len(padding_bytes))
        self.assertEqual(8, int(struct.unpack('B', padding_bytes[7])[0]))

        # msgs that are multiples of 16 long are padded with 16 bytes
        padding_bytes = crypt._get_padding_bytes(16)
        self.assertEqual(16, len(padding_bytes))
        self.assertEqual(16, int(struct.unpack('B', padding_bytes[15])[0]))

        padding_bytes = crypt._get_padding_bytes(32)
        self.assertEqual(16, len(padding_bytes))
        self.assertEqual(16, int(struct.unpack('B', padding_bytes[15])[0]))

    def test_pad(self):
        # This would probably be nicer with those nose test generators
        [self._assert_padded_right(msg) for msg in self.TEST_MSGS]

    def _assert_padded_right(self, test_msg):
        padded_msg = crypt._pad(test_msg)
        self.assertEqual(0, len(padded_msg) % crypt.BLOCK_SIZE)

    def test_unpad(self):
        padded_msgs = {msg: crypt._pad(msg) for msg in self.TEST_MSGS}
        [self.assertEqual(msg, crypt._unpad(padded_msg)) for msg, padded_msg
         in padded_msgs.iteritems()]

    def test_crypt(self):
        [self.assertNotEqual(msg,
                             crypt.encrypt(msg)) for msg in self.TEST_MSGS]

    def test_decrypt(self):
        crypted_msgs = {msg: crypt.encrypt(msg) for msg in self.TEST_MSGS}
        [self.assertEqual(msg, crypt.decrypt(crypted_msg)) for msg, crypted_msg
         in crypted_msgs.iteritems()]

    def test_urlsafe_crypt(self):
        [self.assertNotEqual(msg,
                             crypt.urlsafe_encrypt(
                                 msg)) for msg in self.TEST_MSGS]

    def test_urlsafe_decrypt(self):
        crypted_msgs = {msg: crypt.urlsafe_encrypt(
            msg) for msg in self.TEST_MSGS}
        [self.assertEqual(msg, crypt.urlsafe_decrypt(
            crypted_msg)) for msg, crypted_msg
         in crypted_msgs.iteritems()]

    def test_hex_crypt(self):
        [self.assertNotEqual(msg, crypt.hex_encrypt(
            msg)) for msg in self.TEST_MSGS]

    def text_hex_decrypt(self):
        crypted_msgs = {msg: crypt.hex_encrypt(msg) for msg in self.TEST_MSGS}
        [self.assertEqual(msg, crypt.hex_decrypt(
            crypted_msg)) for msg, crypted_msg
         in crypted_msgs.iteritems()]
